"""Import the audited ScienTI works snapshot into Kahi."""

from copy import deepcopy
from kahi_impactu_utils.String import normalize_dashes
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from threading import Lock
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from hashlib import sha256
import re
import logging
from time import time, monotonic
from unicodedata import normalize as unicode_normalize

from kahi.KahiBase import KahiBase
from kahi.progress import ProgressLogger
from kahi_impactu_utils.Mongo import order_document
from kahi_impactu_utils.Work import (
    AmbiguousWorkDOI, canonical_work_doi, ensure_work_doi_indexes,
    insert_work_by_doi, prepare_work_doi, work_titles_match, normalize_work_title, _coordinate_doi,
)
from kahi_impactu_utils.Utils import lang_poll, normalize_publication_year
from bson import BSON, ObjectId
from pymongo import MongoClient, ReplaceOne, TEXT
from kahi_minciencias_opendata_works.work_types import book_types_compatible, book_type_policy


CONTRACT_VERSION = "scienti-kahi-ingestion-v1"
LOGGER = logging.getLogger(__name__)
SNAPSHOT_ENTITIES = {
    "works", "projects", "patents", "events", "persons", "affiliations",
}
WORK_FIELDS = {
    "titles", "updated", "doi", "abstracts", "keywords", "types",
    "external_ids", "external_urls", "date_published", "year_published",
    "bibliographic_info", "open_access", "apc", "references_count",
    "references", "citations_count", "citations", "author_count", "authors",
    "source", "ranking", "subjects", "citations_by_year", "groups", "rights",
    "primary_topic", "topics",
}
SOURCE_ONLY_FIELDS = {
    "source_metadata", "authorship_status", "authorship_candidates",
    "authorship_evidence", "authorship_authority", "identity_collision_of",
    "type_audit",
}
IDENTIFIED_POINTER_ID = "current"
LIST_FIELDS = {
    "titles", "updated", "abstracts", "keywords", "types", "external_ids",
    "external_urls", "references", "citations_count", "citations", "ranking",
    "subjects", "citations_by_year", "rights", "topics",
}
DICT_FIELDS = {"bibliographic_info", "open_access", "apc", "source", "primary_topic"}
SCALAR_FIELDS = {
    "doi", "date_published", "year_published", "references_count",
}
COD_RH_RE = re.compile(r"\d{10}")
GROUP_RE = re.compile(r"COL\d{7}")
SHA256_RE = re.compile(r"[0-9a-fA-F]{64}")


def append_unique(values, item):
    if item not in values:
        values.append(deepcopy(item))


def normalized_name(value):
    value = unicode_normalize("NFKD", str(value or ""))
    value = "".join(char for char in value if not 0x300 <= ord(char) <= 0x36F)
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value.lower()).split())


def kahi_work_id(source_id):
    """Normalize an audited graph id to the ObjectId required by Kahi."""
    if isinstance(source_id, ObjectId):
        return source_id
    value = str(source_id or "")
    if SHA256_RE.fullmatch(value):
        return ObjectId(bytes.fromhex(value)[:12])
    if value:
        return ObjectId(sha256(value.encode("utf-8")).digest()[:12])
    raise RuntimeError("works snapshot contains an empty _id")


def strict_name_match(left, right):
    left = normalized_name(left)
    right = normalized_name(right)
    if not left or not right:
        return False, 0.0
    ratio = SequenceMatcher(None, left, right).ratio()
    sorted_ratio = SequenceMatcher(
        None, " ".join(sorted(left.split())), " ".join(sorted(right.split()))
    ).ratio()
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    token_ratio = SequenceMatcher(
        None, " ".join(sorted(left_tokens)), " ".join(sorted(right_tokens))
    ).ratio()
    stopwords = {"de", "del", "la", "las", "los", "el", "y", "en", "para"}
    distinctive_left = left_tokens - stopwords
    distinctive_right = right_tokens - stopwords
    overlap = len(distinctive_left & distinctive_right) / max(
        1, min(len(distinctive_left), len(distinctive_right))
    )
    accepted = (
        left == right
        or ratio >= 0.94
        or sorted_ratio >= 0.96
        or token_ratio >= 0.95
        and overlap >= 0.80
        and min(len(distinctive_left), len(distinctive_right)) >= 3
    )
    return accepted, max(ratio, sorted_ratio, token_ratio)


def merge_missing(target, source):
    """Recursively add only non-empty values absent from the destination."""
    for key, value in (source or {}).items():
        if value in (None, "", [], {}):
            continue
        if key not in target or target[key] in (None, "", [], {}):
            target[key] = deepcopy(value)
        elif isinstance(target[key], dict) and isinstance(value, dict):
            merge_missing(target[key], value)
    return target


def snapshot_doi(document):
    """Use the shared validator, including DOI stored only in external_ids."""
    primary = canonical_work_doi(document.get("doi"))
    if primary:
        return primary
    identifiers = {
        doi for item in document.get("external_ids", [])
        if item.get("source") == "doi"
        and (doi := canonical_work_doi(item.get("id")))
    }
    if len(identifiers) > 1:
        raise RuntimeError("snapshot record has multiple DOI identities and no valid primary DOI")
    return next(iter(identifiers), None)


class Kahi_minciencias_opendata_works(KahiBase):
    """Consume only the works member of an audited six-entity release."""

    def __init__(self, config):
        source_config = config["minciencias_opendata_works"]
        self.source_mode = str(source_config.get("source_mode", "snapshot"))
        if self.source_mode == "legacy":
            from kahi_minciencias_opendata_works.legacy import (
                Kahi_minciencias_opendata_works as LegacyReader,
            )
            legacy_config = deepcopy(config)
            legacy_source = legacy_config["minciencias_opendata_works"]
            legacy_source.setdefault("collection_name", "gruplac_production_data")
            legacy_source.setdefault("person_related_works", False)
            self._legacy = LegacyReader(legacy_config)
            return
        if self.source_mode != "snapshot":
            raise ValueError("source_mode must be 'snapshot' or 'legacy'")
        self.config = config
        self.selection = source_config.get("task") or "all"
        if self.selection not in {"all", "doi", "nodoi"}:
            raise ValueError("legacy works options: task must be doi, nodoi or all")
        self.client = MongoClient(config["database_url"])
        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        forbidden = {
            "insert_all", "thresholds", "num_jobs",
            "person_related_works", "person_collection", "es_max_concurrency",
        }
        legacy = sorted(
            key for key in forbidden
            if key in source_config and source_config[key] is not None
        )
        if legacy:
            raise ValueError(
                "legacy works options are no longer supported: {}".format(
                    ", ".join(legacy)
                )
            )
        self.source_client = MongoClient(source_config["database_url"])
        database_name = source_config["database_name"]
        if database_name not in self.source_client.list_database_names():
            raise RuntimeError("source database {} was not found".format(database_name))
        self.source_db = self.source_client[database_name]
        self.batch_size = max(1, int(source_config.get("batch_size", 500)))
        # Rows of a batch are processed concurrently by `workers` threads; the
        # identity leases in _kahi_work_identity keep concurrent rows safe.
        self.workers = max(1, int(source_config.get("workers", 1)))
        self._counters_lock = Lock()
        self.verbose = int(source_config.get("verbose", 0))
        runs_name = source_config.get(
            "runs_collection", "minciencias_opendata_works_runs"
        )
        self.runs = self.db[runs_name]
        self._configure_snapshot(source_config)
        self.similarity = None
        self.similarity_identity = {}
        if any(source_config.get(key) for key in ("es_index", "es_url", "es_user", "es_password")):
            if self.selection != "nodoi":
                raise ValueError("snapshot Elasticsearch is supported only for task=nodoi")
            if not source_config.get("es_index") or not source_config.get("es_url"):
                raise ValueError("snapshot similarity requires es_index and es_url")
            if bool(source_config.get("es_user")) != bool(source_config.get("es_password")):
                raise ValueError("es_user and es_password must be provided together")
            from kahi_minciencias_opendata_works.snapshot_similarity import SnapshotSimilarity
            self.similarity = SnapshotSimilarity(source_config)
            if (self.collection.estimated_document_count() and
                    not self.similarity.handler.es.count(index=source_config["es_index"])["count"]):
                raise RuntimeError("snapshot similarity requires the populated works Elasticsearch index")
            self.similarity_identity = {
                "similarity_policy": "live-title-author-year-v1",
                "similarity_index": source_config["es_index"],
                "similarity_url": source_config["es_url"],
            }
        self.run_id = "works:{}".format(self.release_name)
        if self.selection != "all" or self.identified_snapshot:
            self.run_id += ":{}:{}:v2".format(self.source_collection_name, self.selection)
        if self.similarity:
            self.run_id += ":es-title-v1"
        if self.selection != "doi":
            self.run_id += ":book-types-v1"
        ensure_work_doi_indexes(self.collection)
        self.collection.create_index("authors.affiliations.id")
        self.collection.create_index("authors.id")
        self.collection.create_index([("titles.title", TEXT)])
        self.collection.create_index("external_ids.id")
        if self.selection in {"nodoi", "all"}:
            self.collection.create_index("titles.title", name="snapshot_title_identity",
                                         collation={"locale": "en", "strength": 2})

    @staticmethod
    def manages_checkpoints(source_config):
        return source_config.get("source_mode", "snapshot") == "snapshot"

    def _configure_snapshot(self, source_config):
        release_name = str(source_config.get("release_name") or "")
        if not release_name or release_name == "current":
            raise ValueError("an explicit immutable release_name is required")
        release = self.source_db["scienti_final_release_publications"].find_one(
            {"_id": release_name}
        ) or {}
        audit = self.source_db["scienti_final_release_audits"].find_one(
            {"_id": release.get("audit")}
        ) or {}
        collections = release.get("collections") or {}
        runs = release.get("materialization_runs") or {}
        collection_name = str(collections.get("works") or "")
        evidence = (audit.get("evidence") or {}).get("works") or {}
        configured_collection = str(source_config.get("collection_name") or "")
        valid = (
            release.get("status") == "published"
            and release.get("entity_count") == 6
            and set(collections) == SNAPSHOT_ENTITIES
            and set(runs) == SNAPSHOT_ENTITIES
            and audit.get("status") == "passed"
            and audit.get("release_name") == release_name
            and audit.get("collections") == collections
            and audit.get("materialization_runs") == runs
            and int(audit.get("critical_anomalies") or 0) == 0
            and evidence.get("collection") == collection_name
            and evidence.get("materialization_run") == runs.get("works")
            and collection_name in self.source_db.list_collection_names()
        )
        if not valid:
            raise RuntimeError("six-entity works snapshot is not proven and published")
        documents = self.source_db[collection_name].count_documents({}, hint="_id_")
        if documents != int(evidence.get("documents") or -1):
            raise RuntimeError("works snapshot count changed after release audit")
        self.release_name = release_name
        self.audit_name = audit["_id"]
        self.materialization_run = runs["works"]
        self.source_documents = documents
        self.source_collection_name = collection_name
        self.source_collection = self.source_db[collection_name]
        self.identified_snapshot = False
        self.identified_evidence = {}
        if not configured_collection and source_config.get("identified", True):
            configured_collection = self._current_identified_collection(collection_name)
        if configured_collection and configured_collection != collection_name:
            self._configure_identified_snapshot(configured_collection, release, audit)

    def _current_identified_collection(self, full_collection):
        """Resolve the identified (filtered) works collection from the published pointer."""
        pointer = self.source_db["scienti_identified_work_publications"].find_one(
            {"_id": IDENTIFIED_POINTER_ID}
        ) or {}
        if not pointer:
            LOGGER.warning("No identified works pointer %r; using the full works snapshot %s",
                           IDENTIFIED_POINTER_ID, full_collection)
            return ""
        collection = str(pointer.get("collection") or "")
        if (pointer.get("release_name") != self.release_name
                or str(pointer.get("full_graph") or full_collection) != full_collection
                or not collection):
            raise RuntimeError(
                "identified works pointer does not belong to release {}".format(self.release_name)
            )
        return collection

    def _configure_identified_snapshot(self, collection_name, release, release_audit):
        """Validate the selector's own publication without changing the release."""
        publication = self.source_db["scienti_identified_work_publications"].find_one(
            {"_id": collection_name}
        ) or {}
        audit = self.source_db["scienti_identified_work_audits"].find_one(
            {"_id": publication.get("audit")}
        ) or {}
        run = self.source_db["scienti_identified_work_runs"].find_one(
            {"_id": publication.get("run_name")}
        ) or {}
        config = audit.get("config") or {}
        source = config.get("source") or {}
        digest = lambda value: sha256(BSON.encode(value)).hexdigest()
        valid = all((
            publication.get("release_name") == self.release_name,
            publication.get("audit") == publication.get("run_name") == audit.get("_id") == run.get("_id"),
            audit.get("status") == "passed",
            audit.get("critical_anomalies") == 0,
            run.get("status") == "complete",
            run.get("config") == config,
            config.get("target") == collection_name,
            config.get("count_only") is False,
            config.get("policy_version") == "scienti-identified-works-v1",
            source.get("release_name") == self.release_name,
            source.get("collections") == release.get("collections"),
            source.get("release_digest") == digest(release),
            source.get("audit_digest") == digest(release_audit),
            source.get("counts", {}).get("works") == self.source_documents,
            audit.get("documents") == publication.get("documents") == run.get("stats", {}).get("selected"),
            bool(audit.get("documents_digest")),
            audit.get("documents_digest") == run.get("documents_digest"),
            collection_name in self.source_db.list_collection_names(),
        ))
        if not valid:
            raise RuntimeError("identified works snapshot is not proven and published")
        count = self.source_db[collection_name].count_documents({}, hint="_id_")
        if count != audit["documents"]:
            raise RuntimeError("identified works snapshot count changed after audit")
        self.identified_snapshot = True
        self.identified_evidence = {
            "identified_run": run["_id"], "identified_digest": audit["documents_digest"],
            "identified_policy": config["policy"],
        }
        self.source_documents = count
        self.source_collection_name = collection_name
        self.source_collection = self.source_db[collection_name]

    @staticmethod
    def _source_codes(document):
        author_codes = {
            str(author.get("id") or "")
            for author in document.get("authors", [])
            if str(author.get("id") or "")
        }
        group_codes = {
            str(group.get("id") or "") for group in document.get("groups", [])
            if str(group.get("id") or "")
        }
        group_codes.update(
            str(affiliation.get("group_code") or "")
            for author in document.get("authors", [])
            for affiliation in author.get("affiliations", [])
            if str(affiliation.get("group_code") or "")
        )
        if any(not COD_RH_RE.fullmatch(code) for code in author_codes):
            raise RuntimeError("works snapshot contains an invalid COD_RH")
        if any(not GROUP_RE.fullmatch(code) for code in group_codes):
            raise RuntimeError("works snapshot contains an invalid group code")
        return author_codes, group_codes

    @staticmethod
    def _validate_document(document, allow_sparse=False):
        fields = set(document)
        if (
            (WORK_FIELDS - fields if not allow_sparse else not document.get("titles"))
            or fields - ({"_id"} | WORK_FIELDS | SOURCE_ONLY_FIELDS)
        ):
            raise RuntimeError("works snapshot does not follow the Kahi schema")

    @staticmethod
    def _person_codes(person):
        return {
            str(item.get("id", {}).get("COD_RH") or "")
            for item in person.get("external_ids", [])
            if isinstance(item.get("id"), dict)
            and COD_RH_RE.fullmatch(str(item.get("id", {}).get("COD_RH") or ""))
        }

    @staticmethod
    def _resolve_duplicate_person(code, candidates, source_names):
        matches = []
        for person in candidates:
            variants = [person.get("full_name", "")]
            variants.extend(person.get("aliases", []))
            scores = [
                score
                for source_name in source_names
                for variant in variants
                for accepted, score in [strict_name_match(source_name, variant)]
                if accepted
            ]
            if scores:
                matches.append((max(scores), str(person["_id"]), person))
        matches.sort(key=lambda item: (-item[0], item[1]))
        if not matches or (
            len(matches) > 1 and matches[0][0] == matches[1][0]
        ):
            raise RuntimeError(
                "COD_RH {} does not resolve to one name-compatible Kahi "
                "person".format(code)
            )
        return matches[0][2]

    def _reference_maps(self, batch):
        author_codes = set()
        author_names = {}
        group_codes = set()
        for document in batch:
            self._validate_document(document, self.identified_snapshot)
            authors, groups = self._source_codes(document)
            author_codes.update(authors)
            for author in document.get("authors", []):
                code = str(author.get("id") or "")
                name = str(author.get("full_name") or "").strip()
                if code and name:
                    author_names.setdefault(code, set()).add(name)
            group_codes.update(groups)

        candidates = {}
        projection = {
            "full_name": 1, "aliases": 1, "affiliations": 1,
            "external_ids": 1,
        }
        for person in self.db["person"].find(
            {"external_ids.id.COD_RH": {"$in": list(author_codes)}}, projection
        ):
            for code in self._person_codes(person).intersection(author_codes):
                candidates.setdefault(code, {})[person["_id"]] = person
        people = {}
        for code, documents in candidates.items():
            options = list(documents.values())
            people[code] = (
                options[0] if len(options) == 1
                else self._resolve_duplicate_person(
                    code, options, author_names.get(code, set())
                )
            )
        missing_people = sorted(author_codes - set(people))
        if missing_people:
            raise RuntimeError(
                "unresolved work authors: {}".format(", ".join(missing_people[:10]))
            )

        groups = {}
        query = {
            "$or": [
                {"external_ids.id": {"$in": list(group_codes)}},
                {"_id": {"$in": list(group_codes)}},
            ]
        }
        for group in self.db["affiliations"].find(
            query, {"names": 1, "types": 1, "relations": 1, "external_ids": 1}
        ):
            codes = {
                str(item.get("id") or "")
                for item in group.get("external_ids", [])
                if GROUP_RE.fullmatch(str(item.get("id") or ""))
            }
            if GROUP_RE.fullmatch(str(group["_id"])):
                codes.add(str(group["_id"]))
            for code in codes.intersection(group_codes):
                if code in groups and groups[code]["_id"] != group["_id"]:
                    raise RuntimeError("a group code resolves to multiple affiliations")
                groups[code] = group
        missing_groups = sorted(group_codes - set(groups))
        if missing_groups:
            raise RuntimeError(
                "unresolved work groups: {}".format(", ".join(missing_groups[:10]))
            )

        relation_ids = {
            relation.get("id")
            for group in groups.values()
            for relation in group.get("relations", [])
            if relation.get("id") not in (None, "")
        }
        institutions = {
            item["_id"]: item
            for item in self.db["affiliations"].find(
                {"_id": {"$in": list(relation_ids)}}, {"names": 1, "types": 1}
            )
        }
        return people, groups, institutions

    @staticmethod
    def _preferred_name(document, fallback=""):
        names = document.get("names", [])
        for language in ("es", "en"):
            for item in names:
                if item.get("lang") == language and item.get("name"):
                    return item["name"]
        return names[0].get("name", fallback) if names else fallback

    def _kahi_affiliation(self, document, person=None):
        if person:
            for item in person.get("affiliations", []):
                if item.get("id") == document["_id"]:
                    return deepcopy(item)
        return {
            "id": document["_id"],
            "name": self._preferred_name(document),
            "types": deepcopy(document.get("types", [])),
        }

    def _institution_for_evidence(self, evidence, group, institutions):
        source_name = str(evidence.get("institution") or "").strip()
        if not source_name:
            return None, "empty"
        candidates = []
        for relation in group.get("relations", []):
            institution = institutions.get(relation.get("id"))
            if not institution:
                continue
            names = [relation.get("name", "")]
            names.extend(item.get("name", "") for item in institution.get("names", []))
            scores = [strict_name_match(source_name, name) for name in names if name]
            accepted_scores = [score for accepted, score in scores if accepted]
            if accepted_scores:
                candidates.append((max(accepted_scores), institution))
        if len(candidates) != 1:
            return None, "ambiguous" if candidates else "unresolved"
        return candidates[0][1], "resolved"

    def _resolve_author(self, source, people, groups, institutions, counters):
        code = str(source.get("id") or "")
        person = people.get(code) if code else None
        output = {
            "id": person["_id"] if person else "",
            "full_name": (
                person.get("full_name") if person
                else (normalize_dashes(source.get("full_name", "")) or "")
            ),
            "affiliations": [],
        }
        if source.get("type"):
            output["type"] = source["type"]
        for evidence in source.get("affiliations", []):
            group_code = str(evidence.get("group_code") or "")
            group = groups.get(group_code) if group_code else None
            if group:
                append_unique(output["affiliations"], self._kahi_affiliation(group, person))
                institution, status = self._institution_for_evidence(
                    evidence, group, institutions
                )
                if institution:
                    append_unique(
                        output["affiliations"],
                        self._kahi_affiliation(institution, person),
                    )
                elif status in {"ambiguous", "unresolved"}:
                    counters["unresolved_institution_evidence"] += 1
        return output

    def _project(self, source, people, groups, institutions, counters):
        output = {field: deepcopy(source.get(field, default))
                  for field, default in self.empty_work().items()}
        output["doi"] = snapshot_doi(source) or ""
        output["_id"] = kahi_work_id(source["_id"])
        # Yuku does not detect the title language; Kahi does, like the other importers.
        for title in output["titles"]:
            if not title.get("lang") and title.get("title"):
                title["lang"] = lang_poll(title["title"])
        # `updated.time` is when Kahi imported the record, not when Yuku parsed it.
        now = int(time())
        for item in output["updated"]:
            item["time"] = now
        output["authors"] = [
            self._resolve_author(author, people, groups, institutions, counters)
            for author in source.get("authors", [])
        ]
        output["author_count"] = len(output["authors"])
        output["groups"] = [
            {
                "id": groups[str(group["id"])]["_id"],
                "name": self._preferred_name(
                    groups[str(group["id"])], group.get("name", "")
                ),
            }
            for group in source.get("groups", [])
        ]
        return order_document(output, self.empty_work())

    @staticmethod
    def _author_key(author):
        identifier = author.get("id")
        if identifier not in (None, ""):
            return "id", str(identifier)
        return "name", normalized_name(author.get("full_name"))

    def _merge(self, existing, incoming):
        incoming = deepcopy(incoming)
        incoming["year_published"] = normalize_publication_year(incoming.get("year_published"))
        if not existing:
            return incoming
        output = deepcopy(existing)
        output["year_published"] = normalize_publication_year(output.get("year_published"))
        for field in LIST_FIELDS - {"updated"}:
            output.setdefault(field, [])
            for item in incoming.get(field, []):
                append_unique(output[field], item)
        updated = output.setdefault("updated", [])
        by_source = {item.get("source"): item for item in updated}
        for item in incoming.get("updated", []):
            if item.get("source") in by_source:
                by_source[item["source"]]["time"] = item.get("time")
            else:
                updated.append(deepcopy(item))
                by_source[item.get("source")] = updated[-1]
        for field in DICT_FIELDS:
            merge_missing(output.setdefault(field, {}), incoming.get(field, {}))
        for field in SCALAR_FIELDS:
            if output.get(field) in (None, "") and incoming.get(field) not in (None, ""):
                output[field] = deepcopy(incoming[field])
        authors = output.setdefault("authors", [])
        author_keys = {self._author_key(author): author for author in authors}
        for author in incoming.get("authors", []):
            key = self._author_key(author)
            if key not in author_keys:
                authors.append(deepcopy(author))
                author_keys[key] = authors[-1]
                continue
            target = author_keys[key]
            target.setdefault("affiliations", [])
            for affiliation in author.get("affiliations", []):
                append_unique(target["affiliations"], affiliation)
            if author.get("type") and not target.get("type"):
                target["type"] = author["type"]
        groups = output.setdefault("groups", [])
        group_ids = {group.get("id") for group in groups}
        for group in incoming.get("groups", []):
            if group.get("id") not in group_ids:
                groups.append(deepcopy(group))
                group_ids.add(group.get("id"))
        output["author_count"] = len(authors)
        return order_document(output, self.empty_work())

    @staticmethod
    def _bulk_replace(collection, documents):
        operations = [
            ReplaceOne({"_id": document["_id"]}, document, upsert=True)
            for document in documents
        ]
        try:
            collection.bulk_write(operations, ordered=False)
        except TypeError:
            for document in documents:
                collection.replace_one({"_id": document["_id"]}, document, upsert=True)

    @staticmethod
    def _identifier_queries(document):
        """Use product-level identities; ISSN never identifies a work."""
        queries, keys = [], []
        for item in document.get("external_ids", []):
            family, value = item.get("source"), item.get("id")
            if family == "scienti" and isinstance(value, dict):
                rh, product = value.get("COD_RH"), value.get("COD_PRODUCTO")
                if rh and product:
                    pair = {"COD_RH": str(rh), "COD_PRODUCTO": str(product)}
                    queries.append({"external_ids": {"$elemMatch": {
                        "source": family, "id": {"$in": [pair, dict(reversed(list(pair.items())))]},
                    }}})
                    keys.append("scienti:{}:{}".format(rh, product))
            elif family == "minciencias" and isinstance(value, str) and value:
                queries.append({"external_ids": {"$elemMatch": {
                    "source": family, "id": value,
                }}})
                keys.append("minciencias:" + value)
            elif family == "isbn" and isinstance(value, str):
                compact = re.sub(r"[\s-]", "", value).upper()
                if not re.fullmatch(r"(?:[0-9]{13}|[0-9]{9}[0-9X])", compact):
                    continue
                queries.append({"external_ids": {"$elemMatch": {
                    "source": family, "id": {"$in": list({value, compact})},
                }}})
                titles = [item["title"] for item in document.get("titles", []) if item.get("title")]
                if titles:
                    # A title equality index bounds the formatting fallback;
                    # never scan every ISBN for each source record.
                    pattern = "^" + r"[\s-]*".join(compact) + "$"
                    queries.append({"titles.title": {"$in": titles}, "external_ids": {"$elemMatch": {
                        "source": family, "id": {"$regex": pattern, "$options": "i"},
                    }}})
                keys.append("isbn:" + compact)
        return queries, sorted(set(keys))

    @staticmethod
    def _similarity_compatible(incoming, owner):
        """Validate current MongoDB metadata, never the ES score alone."""
        if not book_types_compatible(incoming, owner):
            return False
        if not work_titles_match(incoming.get("titles"), owner.get("titles"), strict=True):
            return False
        left_year = normalize_publication_year(incoming.get("year_published"))
        right_year = normalize_publication_year(owner.get("year_published"))
        if left_year and right_year and left_year != right_year:
            return False
        left_info, right_info = incoming.get("bibliographic_info") or {}, owner.get("bibliographic_info") or {}
        bibliography_matches = 0
        for field in ("volume", "issue", "start_page", "end_page"):
            left, right = left_info.get(field), right_info.get(field)
            if left not in (None, "") and right not in (None, ""):
                if normalize_work_title(str(left)) != normalize_work_title(str(right)):
                    return False
                bibliography_matches += 1
        left_ids = {str(author["id"]) for author in incoming.get("authors", []) if author.get("id")}
        right_ids = {str(author["id"]) for author in owner.get("authors", []) if author.get("id")}
        if left_ids and right_ids:
            return bool(left_ids.intersection(right_ids))
        left_names = [author.get("full_name", "") for author in incoming.get("authors", [])
                      if len(normalized_name(author.get("full_name")).split()) >= 2]
        right_names = [author.get("full_name", "") for author in owner.get("authors", [])
                       if len(normalized_name(author.get("full_name")).split()) >= 2]
        if left_names and right_names:
            return any(strict_name_match(left, right)[0] for left in left_names for right in right_names)
        left_titles = {normalize_work_title(item.get("title")) for item in incoming.get("titles", [])}
        right_titles = {normalize_work_title(item.get("title")) for item in owner.get("titles", [])}
        return bool(left_year and left_year == right_year and bibliography_matches >= 2
                    and any(len(title) >= 40 for title in left_titles.intersection(right_titles)))

    def _similarity_candidates(self, incoming, counters):
        titles = [item["title"] for item in incoming.get("titles", []) if item.get("title")]
        exact = [owner for owner in self.collection.find(
            {"titles.title": {"$in": titles}}, collation={"locale": "en", "strength": 2}
        ) if self._similarity_compatible(incoming, owner)]
        if exact:
            return exact
        identifiers, queries = self.similarity.candidates(incoming)
        counters["similarity_queries"] = counters.get("similarity_queries", 0) + queries
        mongo_ids = list(identifiers)
        mongo_ids.extend(ObjectId(value) for value in identifiers if ObjectId.is_valid(value))
        current = list(self.collection.find({"_id": {"$in": mongo_ids}})) if mongo_ids else []
        missing = set(identifiers) - {str(owner["_id"]) for owner in current}
        if missing:
            LOGGER.warning("Stale Elasticsearch works absent from MongoDB: %s", sorted(missing))
            counters["stale_es_hits"] = counters.get("stale_es_hits", 0) + len(missing)
        return [owner for owner in current if self._similarity_compatible(incoming, owner)]

    def _upsert_without_doi(self, incoming, counters):
        queries, keys = self._identifier_queries(incoming)
        similarity = getattr(self, "similarity", None)
        if similarity:
            keys.extend("title:" + sha256(normalize_work_title(item.get("title")).encode()).hexdigest()
                        for item in incoming.get("titles", []) if item.get("title"))
        # Locks are acquired in a consistent order, outside works. Recheck
        # candidates while holding all identities shared by this source row.
        source_key = "snapshot-id:" + str(incoming["_id"])
        with ExitStack() as locks:
            acquired_at = monotonic()
            leases = []
            for key in sorted(set([source_key, *keys])):
                leases.append(locks.enter_context(_coordinate_doi(self.collection, "identifier:" + key)))
            query = {"$or": [{"_id": incoming["_id"]}, *queries]}
            candidates = []
            for owner in self.collection.find(query, collation={"locale": "en", "strength": 2}):
                if not book_types_compatible(incoming, owner):
                    continue
                if not work_titles_match(incoming.get("titles"), owner.get("titles"), strict=True):
                    continue
                before = normalize_publication_year(owner.get("year_published"))
                after = normalize_publication_year(incoming.get("year_published"))
                if before and after and before != after:
                    continue
                _, owner_keys = self._identifier_queries(owner)
                product_keys = {key for key in keys if not key.startswith("isbn:")}
                if not product_keys.intersection(owner_keys) and owner["_id"] != incoming["_id"]:
                    left = {str(author["id"]) for author in incoming.get("authors", []) if author.get("id")}
                    right = {str(author["id"]) for author in owner.get("authors", []) if author.get("id")}
                    if left and right and not left.intersection(right):
                        # The ISBN can identify the containing book. Two
                        # similarly titled chapters may have different authors.
                        continue
                candidates.append(owner)
            by_similarity = False
            if not candidates and similarity:
                candidates = self._similarity_candidates(incoming, counters)
                by_similarity = bool(candidates)
            if len(candidates) > 1:
                LOGGER.warning("Ambiguous non-DOI identities for snapshot %s: %s; record skipped",
                               incoming["_id"], [item["_id"] for item in candidates])
                counters["identity_conflicts"] = counters.get("identity_conflicts", 0) + 1
                return
            if monotonic() - acquired_at >= 60:
                # Slow ES requests must not commit after an expired lease
                # another process could already have acquired.
                now = datetime.now(timezone.utc)
                for control, key, owner_token, _ in leases:
                    renewed = control.update_one(
                        {"_id": key, "owner": owner_token, "expires_at": {"$gt": now}},
                        {"$set": {"expires_at": now + timedelta(seconds=120)}},
                    )
                    if not renewed.matched_count:
                        raise RuntimeError("snapshot identity lease expired during lookup; retry the batch")
            owner = candidates[0] if candidates else None
            if owner:
                merged = self._merge(owner, incoming)
                self.collection.replace_one({"_id": owner["_id"]}, merged)
                counters["updated"] += 1
                if by_similarity:
                    counters["similarity_matches"] = counters.get("similarity_matches", 0) + 1
                if similarity:
                    similarity.sync(merged)
            else:
                if self.collection.find_one({"_id": incoming["_id"]}, {"_id": 1}):
                    LOGGER.warning("Incompatible source-id collision for snapshot %s; record skipped",
                                   incoming["_id"])
                    counters["identity_conflicts"] = counters.get("identity_conflicts", 0) + 1
                    return
                merged = self._merge(None, incoming)
                self.collection.insert_one(merged)
                counters["inserted"] += 1
                if similarity:
                    similarity.sync(merged)

    def _process_row(self, source, existing, people, groups, institutions, counters):
        """Import one source row. `counters` is private to the row when the batch
        runs with several workers; the caller merges it afterwards."""
        projected = self._project(source, people, groups, institutions, counters)
        current = existing.get(projected["_id"])
        if prepare_work_doi(projected, "minciencias"):
            if current and not work_titles_match(projected.get("titles"), current.get("titles")):
                # A source-id collision must not bypass the DOI/title gate.
                projected.pop("_id")
            try:
                inserted_id, owner = insert_work_by_doi(self.collection, projected)
            except AmbiguousWorkDOI as error:
                LOGGER.warning("DOI conflict; snapshot record %s skipped: %s", source["_id"], error)
                counters["doi_conflicts"] = counters.get("doi_conflicts", 0) + 1
                return
            if owner:
                merged = self._merge(owner, projected)
                self.collection.replace_one({"_id": owner["_id"]}, merged)
            counters["updated" if owner else "inserted"] += 1
            return
        self._upsert_without_doi(projected, counters)

    def _merge_counters(self, counters, partial):
        with self._counters_lock:
            for key, value in partial.items():
                counters[key] = counters.get(key, 0) + value

    def _process_batch(self, batch, counters):
        people, groups, institutions = self._reference_maps(batch)
        identifiers = [kahi_work_id(document["_id"]) for document in batch]
        existing = {
            document["_id"]: document
            for document in self.collection.find({"_id": {"$in": identifiers}})
        }
        if self.workers == 1 or len(batch) == 1:
            for source in batch:
                self._process_row(source, existing, people, groups, institutions, counters)
            return

        def process(source):
            partial = Counter()  # accepts += on any key, like the shared counters after setdefault
            self._process_row(source, existing, people, groups, institutions, partial)
            return partial

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            # Exceptions propagate: a failed row aborts the run, as it does sequentially.
            for partial in pool.map(process, batch):
                self._merge_counters(counters, partial)

    def process_snapshot(self, previous):
        counters = deepcopy(previous.get("counters") or {
            "processed": 0, "inserted": 0, "updated": 0,
            "unresolved_institution_evidence": 0, "selected": 0,
            "skipped_other_phase": 0,
        })
        counters.setdefault("selected", counters["processed"])
        counters.setdefault("skipped_other_phase", 0)
        last_id = previous.get("last_id")
        query = {"_id": {"$gt": last_id}} if last_id is not None else {}
        # For partitioned imports scan only identity fields. Fetch full
        # metadata in batches exclusively for rows belonging to this phase.
        projection = ({"doi": 1, "external_ids": 1} if self.selection != "all"
                      else {field: 0 for field in SOURCE_ONLY_FIELDS})
        cursor = self.source_collection.find(query, projection).sort("_id", 1)
        cursor = cursor.batch_size(self.batch_size)
        progress = ProgressLogger(
            "minciencias_opendata_works:{}".format(self.selection),
            total=self.source_documents,
            initial=counters["processed"],
        ).start("batch_size={}".format(self.batch_size))
        batch = []
        for document in cursor:
            batch.append(document)
            if len(batch) < self.batch_size:
                continue
            self._process_selected_batch(batch, counters, progress)
            counters["processed"] += len(batch)
            last_id = batch[-1]["_id"]
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {"last_id": last_id, "counters": deepcopy(counters)}},
            )
            batch.clear()
        if batch:
            self._process_selected_batch(batch, counters, progress)
            counters["processed"] += len(batch)
            last_id = batch[-1]["_id"]
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {"last_id": last_id, "counters": deepcopy(counters)}},
            )
        progress.done()
        if counters["processed"] != self.source_documents:
            raise RuntimeError("works snapshot changed while it was being imported")
        return counters

    def _process_selected_batch(self, rows, counters, progress=None):
        before = (counters["inserted"], counters["updated"])
        if self.selection == "all":
            selected = rows
        else:
            identifiers = [row["_id"] for row in rows
                           if bool(snapshot_doi(row)) == (self.selection == "doi")]
            documents = {row["_id"]: row for row in self.source_collection.find(
                {"_id": {"$in": identifiers}}, {field: 0 for field in SOURCE_ONLY_FIELDS}
            )} if identifiers else {}
            if set(documents) != set(identifiers):
                raise RuntimeError("selected snapshot records disappeared during import")
            selected = [documents[identifier] for identifier in identifiers]
        if selected:
            self._process_batch(selected, counters)
        counters["selected"] += len(selected)
        counters["skipped_other_phase"] += len(rows) - len(selected)
        if progress is not None:
            inserted = counters["inserted"] - before[0]
            updated = counters["updated"] - before[1]
            progress.step("nuevos", n=inserted)
            progress.step("actualizados", n=updated)
            progress.step("omitidos", n=len(rows) - inserted - updated)

    def run(self):
        if self.source_mode == "legacy":
            return self._legacy.run()
        previous = self.runs.find_one({"_id": self.run_id}) or {}
        identity = {
            "contract_version": CONTRACT_VERSION,
            "release": self.release_name,
            "audit": self.audit_name,
            "materialization_run": self.materialization_run,
            "source_collection": self.source_collection_name,
            "source_documents": self.source_documents,
        }
        if self.selection != "all" or self.identified_snapshot:
            identity.update(selection=self.selection, import_policy="partitioned-identities-v2",
                            **self.identified_evidence)
        identity.update(self.similarity_identity)
        if self.selection != "doi":
            identity.update(book_type_policy())
        if previous and any(previous.get(key) != value for key, value in identity.items()):
            raise RuntimeError("works import run exists with different source evidence")
        if previous.get("status") == "complete":
            return deepcopy(previous.get("summary") or {})
        if not previous:
            self.runs.insert_one({
                "_id": self.run_id, "status": "pending", "created_at": int(time()),
                **identity,
            })
            previous = self.runs.find_one({"_id": self.run_id})
        self.runs.update_one(
            {"_id": self.run_id},
            {"$set": {"status": "running", "started_at": int(time())},
             "$inc": {"attempts": 1}, "$unset": {"error": ""}},
        )
        try:
            counters = self.process_snapshot(previous)
            summary = {**identity, **counters}
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {
                    "status": "complete", "finished_at": int(time()),
                    "summary": deepcopy(summary), "counters": deepcopy(counters),
                }},
            )
            return summary
        except Exception as error:
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {
                    "status": "failed", "finished_at": int(time()),
                    "error": str(error),
                }},
            )
            raise
