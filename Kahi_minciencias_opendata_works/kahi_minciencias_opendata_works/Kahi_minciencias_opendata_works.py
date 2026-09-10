"""Import the audited ScienTI works snapshot into Kahi."""

from copy import deepcopy
from difflib import SequenceMatcher
import re
from time import time
from unicodedata import normalize as unicode_normalize

from kahi.KahiBase import KahiBase
from pymongo import MongoClient, ReplaceOne, TEXT


CONTRACT_VERSION = "scienti-kahi-ingestion-v1"
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
SOURCE_ONLY_FIELDS = {"source_metadata", "authorship_status"}
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


def append_unique(values, item):
    if item not in values:
        values.append(deepcopy(item))


def normalized_name(value):
    value = unicode_normalize("NFKD", str(value or ""))
    value = "".join(char for char in value if not 0x300 <= ord(char) <= 0x36F)
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value.lower()).split())


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


class Kahi_minciencias_opendata_works(KahiBase):
    """Consume only the works member of an audited six-entity release."""

    def __init__(self, config):
        self.config = config
        self.client = MongoClient(config["database_url"])
        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]
        self.collection.create_index("authors.affiliations.id")
        self.collection.create_index("authors.id")
        self.collection.create_index([("titles.title", TEXT)])
        self.collection.create_index("external_ids.id")

        source_config = config["minciencias_opendata_works"]
        forbidden = {
            "source_mode", "task", "insert_all", "thresholds", "num_jobs",
            "person_related_works", "person_collection", "es_index", "es_url",
            "es_user", "es_password", "es_max_concurrency",
        }
        legacy = sorted(forbidden.intersection(source_config))
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
        self.verbose = int(source_config.get("verbose", 0))
        runs_name = source_config.get(
            "runs_collection", "minciencias_opendata_works_runs"
        )
        self.runs = self.db[runs_name]
        self._configure_snapshot(source_config)
        self.run_id = "works:{}".format(self.release_name)

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
            and (not configured_collection or configured_collection == collection_name)
            and collection_name in self.source_db.list_collection_names()
        )
        if not valid:
            raise RuntimeError("six-entity works snapshot is not proven and published")
        documents = self.source_db[collection_name].count_documents({})
        if documents != int(evidence.get("documents") or -1):
            raise RuntimeError("works snapshot count changed after release audit")
        self.release_name = release_name
        self.audit_name = audit["_id"]
        self.materialization_run = runs["works"]
        self.source_documents = documents
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
    def _validate_document(document):
        fields = set(document)
        if (
            WORK_FIELDS - fields
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

    def _reference_maps(self, batch):
        author_codes = set()
        group_codes = set()
        for document in batch:
            self._validate_document(document)
            authors, groups = self._source_codes(document)
            author_codes.update(authors)
            group_codes.update(groups)

        people = {}
        projection = {"full_name": 1, "affiliations": 1, "external_ids": 1}
        for person in self.db["person"].find(
            {"external_ids.id.COD_RH": {"$in": list(author_codes)}}, projection
        ):
            for code in self._person_codes(person).intersection(author_codes):
                if code in people and people[code]["_id"] != person["_id"]:
                    raise RuntimeError("a COD_RH resolves to multiple Kahi people")
                people[code] = person
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
                person.get("full_name") if person else source.get("full_name", "")
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
        output = {field: deepcopy(source[field]) for field in WORK_FIELDS}
        output["_id"] = source["_id"]
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
        return output

    @staticmethod
    def _author_key(author):
        identifier = author.get("id")
        if identifier not in (None, ""):
            return "id", str(identifier)
        return "name", normalized_name(author.get("full_name"))

    def _merge(self, existing, incoming):
        if not existing:
            return incoming
        output = deepcopy(existing)
        for field in LIST_FIELDS:
            output.setdefault(field, [])
            for item in incoming.get(field, []):
                append_unique(output[field], item)
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
        return output

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

    def _process_batch(self, batch, counters):
        people, groups, institutions = self._reference_maps(batch)
        identifiers = [document["_id"] for document in batch]
        existing = {
            document["_id"]: document
            for document in self.collection.find({"_id": {"$in": identifiers}})
        }
        output = []
        for source in batch:
            projected = self._project(source, people, groups, institutions, counters)
            current = existing.get(source["_id"])
            output.append(self._merge(current, projected))
            counters["updated" if current else "inserted"] += 1
        self._bulk_replace(self.collection, output)

    def process_snapshot(self, previous):
        counters = deepcopy(previous.get("counters") or {
            "processed": 0, "inserted": 0, "updated": 0,
            "unresolved_institution_evidence": 0,
        })
        last_id = previous.get("last_id")
        query = {"_id": {"$gt": last_id}} if last_id is not None else {}
        projection = {field: 0 for field in SOURCE_ONLY_FIELDS}
        cursor = self.source_collection.find(query, projection).sort("_id", 1)
        cursor = cursor.batch_size(self.batch_size)
        batch = []
        for document in cursor:
            batch.append(document)
            if len(batch) < self.batch_size:
                continue
            self._process_batch(batch, counters)
            counters["processed"] += len(batch)
            last_id = batch[-1]["_id"]
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {"last_id": last_id, "counters": deepcopy(counters)}},
            )
            if self.verbose > 0:
                print("INFO: imported {} audited works".format(counters["processed"]))
            batch.clear()
        if batch:
            self._process_batch(batch, counters)
            counters["processed"] += len(batch)
            last_id = batch[-1]["_id"]
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {"last_id": last_id, "counters": deepcopy(counters)}},
            )
        if counters["processed"] != self.source_documents:
            raise RuntimeError("works snapshot changed while it was being imported")
        return counters

    def run(self):
        previous = self.runs.find_one({"_id": self.run_id}) or {}
        identity = {
            "contract_version": CONTRACT_VERSION,
            "release": self.release_name,
            "audit": self.audit_name,
            "materialization_run": self.materialization_run,
            "source_collection": self.source_collection_name,
            "source_documents": self.source_documents,
        }
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
