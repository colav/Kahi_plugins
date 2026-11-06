"""Import the audited ScienTI person snapshot into Kahi."""

from copy import deepcopy
import logging
import re
from time import time

from bson import ObjectId
from kahi.KahiBase import KahiBase
from kahi.progress import ProgressLogger
from kahi_impactu_utils.Mongo import order_document
from kahi_impactu_utils.Utils import parse_orcid_id_from_url
from pymongo import MongoClient, ReplaceOne, TEXT
from pymongo.collation import Collation
from kahi_minciencias_opendata_person.identity import (
    compatible_names, normalized_name, orcid_variants, person_orcids,
)


CONTRACT_VERSION = "scienti-kahi-ingestion-v1"
IMPORT_POLICY = "objectid-orcid-group-institutions-v1"
LOGGER = logging.getLogger(__name__)
UNIT_TYPES = {"group", "faculty", "department", "unit", "subunit", "campus"}
SNAPSHOT_ENTITIES = {
    "works", "projects", "patents", "events", "persons", "affiliations",
}
PERSON_FIELDS = {
    "updated", "full_name", "first_names", "last_names", "initials",
    "aliases", "affiliations", "keywords", "external_ids", "sex",
    "marital_status", "ranking", "birthplace", "birthdate", "degrees",
    "subjects", "citations_count", "products_count", "related_works",
}
SCALAR_FIELDS = {
    "full_name", "first_names", "last_names", "initials", "sex",
    "marital_status", "birthplace", "birthdate",
}
COD_RH_RE = re.compile(r"\d{10}")
GROUP_RE = re.compile(r"COL\d{7}")
DOI_RE = re.compile(
    r"^https://doi\.org/10\.\d{4,9}/[^\s<>\"{}|\\^`\[\]]+$", re.I,
)
PROGRESS_OUTCOMES = (
    ("inserted", "nuevos"), ("updated", "actualizados"),
    ("skipped_empty_name", "omitidos"),
)


def append_unique(values, item):
    if item not in values:
        values.append(deepcopy(item))


def is_minciencias(item):
    return isinstance(item, dict) and (
        item.get("provenance") in {"minciencias", "scienti"}
        or item.get("source") in {"minciencias", "scienti", "OECD"}
    )


class Kahi_minciencias_opendata_person(KahiBase):
    """Consume only an explicit, immutable six-entity ScienTI release."""

    def __init__(self, config):
        source_config = config["minciencias_opendata_person"]
        self.source_mode = str(source_config.get("source_mode", "snapshot"))
        if self.source_mode == "legacy":
            from kahi_minciencias_opendata_person.legacy import (
                Kahi_minciencias_opendata_person as LegacyReader,
            )
            legacy_config = deepcopy(config)
            legacy_source = legacy_config["minciencias_opendata_person"]
            legacy_source.setdefault("researchers", "cvlac_data")
            legacy_source.setdefault("groups_production", "gruplac_production_data")
            legacy_source.setdefault(
                "recognized_researchers", "recognized_researchers"
            )
            legacy_source.setdefault(
                "cvlac_html_profiles", "cvlac_stage_raw_full_corrected_20260823"
            )
            self._legacy = LegacyReader(legacy_config)
            return
        if self.source_mode != "snapshot":
            raise ValueError("source_mode must be 'snapshot' or 'legacy'")
        self.config = config
        self.mongodb_url = config["database_url"]
        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]
        self.collection.create_index("external_ids.id")
        self.collection.create_index("external_ids.id.COD_RH")
        self.collection.create_index([("external_ids.source", 1), ("external_ids.id", 1)])
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        forbidden = {
            "researchers", "cvlac", "groups_production",
            "private_profiles", "cvlac_html_profiles",
        }
        configured_legacy = sorted(forbidden.intersection(source_config))
        if configured_legacy:
            raise ValueError(
                "legacy person source options are no longer supported: {}".format(
                    ", ".join(configured_legacy)
                )
            )
        self.source_client = MongoClient(source_config["database_url"])
        source_database = source_config["database_name"]
        if source_database not in self.source_client.list_database_names():
            raise RuntimeError("source database {} was not found".format(source_database))
        self.source_db = self.source_client[source_database]
        self.verbose = int(source_config.get("verbose", 0))
        self.batch_size = max(1, int(source_config.get("batch_size", 1000)))
        self.runs = self.db[source_config.get(
            "runs_collection", "minciencias_opendata_person_runs"
        )]
        self._configure_snapshot(source_config)
        self.run_id = "persons:{}:{}".format(self.release_name, IMPORT_POLICY)
        self.affiliation_cache = {}
        self.affiliation_name_cache = {}
        self.affiliations_by_id = {}
        self.group_institution_cache = {}
        self.affiliation_name_collation = Collation(locale="es", strength=2)
        self.db["affiliations"].create_index(
            [("names.name", 1)],
            name="names.name_es_ci",
            collation=self.affiliation_name_collation,
        )

    def _report_progress(self, progress, before, counts, batch_size):
        """Count one finished batch by outcome; silent when verbose < 1."""
        if progress is None:
            return
        for key, outcome in PROGRESS_OUTCOMES:
            done = counts.get(key, 0) - before.get(key, 0)
            progress.step(outcome, n=done)
            batch_size -= done
        progress.step(n=batch_size)

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
        collection_name = str(collections.get("persons") or "")
        evidence = (audit.get("evidence") or {}).get("persons") or {}
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
            and evidence.get("materialization_run") == runs.get("persons")
            and (not configured_collection or configured_collection == collection_name)
            and collection_name in self.source_db.list_collection_names()
        )
        if not valid:
            raise RuntimeError("six-entity person snapshot is not proven and published")
        documents = self.source_db[collection_name].count_documents({})
        if documents != int(evidence.get("documents") or -1):
            raise RuntimeError("person snapshot count changed after release audit")
        self.release_name = release_name
        self.audit_name = audit["_id"]
        self.materialization_run = runs["persons"]
        self.source_documents = documents
        self.source_collection_name = collection_name
        self.source_collection = self.source_db[collection_name]

    @staticmethod
    def _cod_rh(document):
        code = str(document.get("_id") or "")
        external_codes = {
            str(item.get("id", {}).get("COD_RH") or "")
            for item in document.get("external_ids", [])
            if isinstance(item.get("id"), dict)
        }
        if not COD_RH_RE.fullmatch(code) or code not in external_codes:
            raise RuntimeError("person snapshot contains an invalid COD_RH")
        return code

    @staticmethod
    def _validate_document(document):
        unexpected = set(document) - ({"_id"} | PERSON_FIELDS)
        missing = PERSON_FIELDS - set(document)
        if unexpected or missing:
            raise RuntimeError("person snapshot does not follow the Kahi schema")
        for item in document.get("related_works", []):
            if (
                item.get("source") != "doi"
                or not DOI_RE.fullmatch(str(item.get("id") or ""))
                or set(item) - {"provenance", "source", "id", "author_count"}
            ):
                raise RuntimeError("person snapshot contains a non-DOI related_work")

    def _target_affiliation(self, source_id):
        source_id = str(source_id or "")
        if source_id in self.affiliation_cache:
            return self.affiliation_cache[source_id]
        result = self.db["affiliations"].find_one(
            {"external_ids.id": source_id}, {"names": 1, "types": 1, "relations": 1}
        )
        if not result:
            result = self.db["affiliations"].find_one(
                {"_id": source_id}, {"names": 1, "types": 1, "relations": 1}
            )
        self.affiliation_cache[source_id] = result
        if result:
            self.affiliations_by_id[result["_id"]] = result
        return result

    def _target_affiliation_by_name(self, name):
        name = str(name or "").strip()
        key = name.casefold()
        if not key:
            return None
        if key in self.affiliation_name_cache:
            return self.affiliation_name_cache[key]
        query = {"names.name": name}
        matches = list(
            self.db["affiliations"].find(
                query,
                {"names": 1, "types": 1, "relations": 1},
                collation=self.affiliation_name_collation,
            ).limit(2)
        )
        result = matches[0] if len(matches) == 1 else None
        self.affiliation_name_cache[key] = result
        if result:
            self.affiliations_by_id[result["_id"]] = result
        return result

    @staticmethod
    def _preferred_name(document, fallback):
        names = document.get("names", [])
        for language in ("es", "en"):
            for item in names:
                if item.get("lang") == language and item.get("name"):
                    return item["name"]
        return names[0].get("name", fallback) if names else fallback

    def _resolve_affiliations(self, affiliations):
        output = []
        for source in affiliations or []:
            item = deepcopy(source)
            source_id = str(item.get("id") or "")
            target = self._target_affiliation(source_id) if source_id else None
            if source_id and GROUP_RE.fullmatch(source_id) and not target:
                raise RuntimeError(
                    "group {} must be imported before its people".format(source_id)
                )
            if not target and not source_id:
                target = self._target_affiliation_by_name(item.get("name"))
            if target:
                item["id"] = target["_id"]
                item["name"] = self._preferred_name(target, item.get("name", ""))
                item["types"] = deepcopy(target.get("types", item.get("types", [])))
            append_unique(output, item)
        return output

    @staticmethod
    def _replace_managed(existing, incoming, field):
        retained = [item for item in existing.get(field, []) if not is_minciencias(item)]
        for item in incoming.get(field, []):
            append_unique(retained, item)
        return retained

    def _group_institutions(self, group_id):
        if group_id in self.group_institution_cache:
            return self.group_institution_cache[group_id]
        group = self.affiliations_by_id.get(group_id) or {}
        output = []
        for relation in group.get("relations", []):
            identifier = relation.get("id")
            if identifier in (None, ""):
                continue
            if identifier not in self.affiliations_by_id:
                self.affiliations_by_id[identifier] = self.db["affiliations"].find_one(
                    {"_id": identifier}, {"names": 1, "types": 1},
                )
            institution = self.affiliations_by_id[identifier]
            if not institution:
                LOGGER.warning("Missing group institution: group=%s institution=%s", group_id, identifier)
                continue
            types = {item.get("type") for item in institution.get("types", [])}
            if not types or types & UNIT_TYPES:
                continue
            append_unique(output, {
                "id": institution["_id"],
                "name": self._preferred_name(institution, relation.get("name", "")),
                "types": deepcopy(institution.get("types", [])),
                "start_date": None, "end_date": -1, "position": "", "years": [],
            })
        self.group_institution_cache[group_id] = output
        return output

    def _add_group_institutions(self, entry, incoming, counts):
        present = {item.get("id") for item in entry["affiliations"] if item.get("id") is not None}
        for group in incoming:
            if not any(item.get("type") == "group" for item in group.get("types", [])):
                continue
            for institution in self._group_institutions(group["id"]):
                if institution["id"] in present:
                    continue
                entry["affiliations"].append(deepcopy(institution))
                present.add(institution["id"])
                if counts is not None:
                    counts["indirect_institutions_added"] += 1

    def _merge(self, existing, source, counts=None):
        source = deepcopy(source)
        for external in source.get("external_ids", []):
            if external.get("source") == "orcid" and isinstance(external.get("id"), str):
                parsed = parse_orcid_id_from_url(external["id"])
                if parsed:
                    external["id"] = parsed
        if not existing:
            entry = self.empty_person()
            entry["_id"] = ObjectId()
            entry.update({field: deepcopy(source[field]) for field in PERSON_FIELDS})
            entry["affiliations"] = self._resolve_affiliations(source["affiliations"])
            self._add_group_institutions(entry, list(entry["affiliations"]), counts)
            return entry

        entry = deepcopy(existing)
        incoming_codes = {
            str(item["id"].get("COD_RH")) for item in source.get("external_ids", [])
            if isinstance(item.get("id"), dict) and item["id"].get("COD_RH")
        }
        other_codes = {
            str(item["id"].get("COD_RH")) for item in existing.get("external_ids", [])
            if isinstance(item.get("id"), dict) and item["id"].get("COD_RH")
        } - incoming_codes
        for field in SCALAR_FIELDS:
            value = source.get(field)
            if value not in (None, "", [], {}, -1):
                entry[field] = deepcopy(value)
        for field in {"updated", "external_ids", "ranking", "degrees", "subjects"}:
            if other_codes:
                for item in source.get(field, []):
                    append_unique(entry.setdefault(field, []), item)
            else:
                entry[field] = self._replace_managed(entry, source, field)
        # An ORCID match may link another COD_RH to the same person. Retain
        # prior codes so retries and later code-only records find this ObjectId.
        for item in existing.get("external_ids", []):
            if isinstance(item.get("id"), dict) and item["id"].get("COD_RH"):
                append_unique(entry["external_ids"], item)
        for field in {"aliases", "keywords"}:
            entry.setdefault(field, [])
            for item in source.get(field, []):
                append_unique(entry[field], item)
        entry.setdefault("affiliations", [])
        incoming = self._resolve_affiliations(source.get("affiliations", []))
        for item in incoming:
            append_unique(entry["affiliations"], item)
        self._add_group_institutions(entry, incoming, counts)
        if other_codes:
            entry["related_works"] = [
                item for item in entry.get("related_works", [])
                if not is_minciencias(item) or item.get("source") == "doi"
            ]
            for item in source.get("related_works", []):
                append_unique(entry["related_works"], item)
        else:
            entry["related_works"] = self._replace_managed(entry, source, "related_works")
        entry.setdefault("citations_count", [])
        entry.setdefault("products_count", 0)
        return entry

    @staticmethod
    def _candidate_score(document):
        external_ids = document.get("external_ids", [])
        strong_ids = sum(
            not (item.get("source") == "scienti" and isinstance(item.get("id"), dict))
            for item in external_ids
        )
        scienti_updates = sum(
            item.get("source") == "scienti" for item in document.get("updated", [])
        )
        return (
            strong_ids,
            scienti_updates,
            len(external_ids),
            len(document.get("affiliations", [])),
        )

    def _resolve_candidates(self, code, source, candidates):
        if len(candidates) == 1:
            return candidates[0]
        source_name = normalized_name(source.get("full_name"))
        name_matches = [
            item for item in candidates
            if source_name and normalized_name(item.get("full_name")) == source_name
        ]
        candidate_names = {
            normalized_name(item.get("full_name")) for item in candidates
        }
        if (
            not name_matches
            and source_name
            and len(candidate_names) == 1
            and set(source_name.split()).issubset(
                set(next(iter(candidate_names)).split())
            )
        ):
            name_matches = candidates
        if len(name_matches) == 1:
            return name_matches[0]
        if len(name_matches) > 1:
            ranked = sorted(
                ((self._candidate_score(item), item) for item in name_matches),
                key=lambda value: value[0], reverse=True,
            )
            if len(ranked) == 1 or ranked[0][0] > ranked[1][0]:
                return ranked[0][1]
        raise RuntimeError(
            "COD_RH {} resolves to multiple Kahi people without unique name/evidence".format(
                code
            )
        )

    def _preflight(self):
        group_ids = {
            str(value) for value in self.source_collection.distinct("affiliations.id")
            if GROUP_RE.fullmatch(str(value))
        }
        projection = {"names": 1, "types": 1, "relations": 1, "external_ids": 1}
        ordered_ids = sorted(group_ids)
        for start in range(0, len(ordered_ids), self.batch_size):
            batch = ordered_ids[start:start + self.batch_size]
            for item in self.db["affiliations"].find({"$or": [
                    {"_id": {"$in": batch}}, {"external_ids.id": {"$in": batch}},
            ]}, projection):
                self.affiliations_by_id[item["_id"]] = item
                matches = {str(item["_id"])} | {
                    value["id"] for value in item.get("external_ids", [])
                    if isinstance(value.get("id"), str)
                }
                for code in matches & group_ids:
                    previous = self.affiliation_cache.get(code)
                    if previous and previous["_id"] != item["_id"]:
                        raise RuntimeError("group {} has ambiguous affiliation identifiers".format(code))
                    self.affiliation_cache[code] = item
        missing = [
            code for code in sorted(group_ids)
            if not self._target_affiliation(code)
        ]
        if missing:
            raise RuntimeError(
                "groups must be imported before their people: {}".format(
                    ", ".join(missing[:10])
                )
            )
        institution_ids = {
            relation.get("id") for code in group_ids
            for relation in self.affiliation_cache[code].get("relations", [])
            if relation.get("id") not in (None, "")
        }
        institution_ids = list(institution_ids - self.affiliations_by_id.keys())
        for start in range(0, len(institution_ids), self.batch_size):
            for item in self.db["affiliations"].find(
                    {"_id": {"$in": institution_ids[start:start + self.batch_size]}}, projection):
                self.affiliations_by_id[item["_id"]] = item

    def _process_batch(self, batch, counts):
        for field in ("resolved_duplicate_cod_rh", "skipped_empty_name", "matched_orcid",
                      "orcid_conflicts", "indirect_institutions_added"):
            counts.setdefault(field, 0)
        codes = {}
        for source in batch:
            code = self._cod_rh(source)
            self._validate_document(source)
            if not normalized_name(source.get("full_name")):
                counts["skipped_empty_name"] += 1
                LOGGER.warning("Skipping person without a name: COD_RH=%s", code)
                continue
            codes[code] = source
        if not codes:
            return
        candidates_by_code = {}
        query = {"external_ids.id.COD_RH": {"$in": list(codes)}}
        for existing in self.collection.find(query):
            matched = {
                str(item.get("id", {}).get("COD_RH") or "")
                for item in existing.get("external_ids", [])
                if isinstance(item.get("id"), dict)
            }.intersection(codes)
            for code in matched:
                candidates_by_code.setdefault(code, []).append(existing)

        existing_by_code = {}
        counts.setdefault("resolved_duplicate_cod_rh", 0)
        for code, candidates in candidates_by_code.items():
            existing_by_code[code] = self._resolve_candidates(
                code, codes[code], candidates
            )
            if len(candidates) > 1:
                counts["resolved_duplicate_cod_rh"] += 1

        orcids_by_code = {code: person_orcids(source) for code, source in codes.items()}
        lookup_orcids = set().union(*(values for code, values in orcids_by_code.items()
                                     if code not in existing_by_code))
        orcid_candidates = {}
        if lookup_orcids:
            variants = sorted(set().union(*(orcid_variants(value) for value in lookup_orcids)))
            for item in self.collection.find({"external_ids": {"$elemMatch": {
                    "source": {"$in": ["orcid", "ORCID"]}, "id": {"$in": variants},
            }}}):
                for value in person_orcids(item) & lookup_orcids:
                    orcid_candidates.setdefault(value, {})[item["_id"]] = item

        entries = {}
        for code, source in codes.items():
            existing = existing_by_code.get(code)
            if existing:
                existing = entries.get(existing["_id"], existing)
            elif orcids_by_code[code]:
                orcids = orcids_by_code[code]
                candidates = {
                    identifier: entries.get(identifier, item)
                    for value in orcids
                    for identifier, item in orcid_candidates.get(value, {}).items()
                }
                candidates = [item for item in candidates.values() if person_orcids(item) & orcids]
                compatible = [item for item in candidates if compatible_names(source, item)]
                if len(orcids) == 1 and len(compatible) == 1:
                    existing = compatible[0]
                    counts["matched_orcid"] += 1
                elif candidates or len(orcids) > 1:
                    counts["orcid_conflicts"] += 1
                    LOGGER.warning(
                        "ORCID match rejected: COD_RH=%s orcids=%s candidates=%s compatible=%s",
                        code, sorted(orcids), [str(item["_id"]) for item in candidates], len(compatible),
                    )
            if existing and not isinstance(existing["_id"], ObjectId):
                raise RuntimeError("existing person must have a MongoDB ObjectId before this import")
            if existing:
                counts["removed_legacy_related_works"] += sum(
                    is_minciencias(item) and item.get("source") != "doi"
                    for item in existing.get("related_works", [])
                )
            entry = order_document(
                self._merge(existing, source, counts), self.empty_person()
            )
            entries[entry["_id"]] = entry
            for value in entry.get("external_ids", []):
                identifier = value.get("id")
                if isinstance(identifier, dict) and str(identifier.get("COD_RH") or "") in codes:
                    existing_by_code[str(identifier["COD_RH"])] = entry
            for value in person_orcids(entry):
                orcid_candidates.setdefault(value, {})[entry["_id"]] = entry
            action = "updated" if existing else "inserted"
            counts[action] += 1
            if self.verbose > 4:
                print("{} person {} from audited snapshot".format(action, code))
        operations = [
            ReplaceOne({"_id": entry["_id"]}, entry, upsert=True)
            for entry in entries.values()
        ]
        try:
            self.collection.bulk_write(operations, ordered=False)
        except TypeError:
            for entry in entries.values():
                self.collection.replace_one({"_id": entry["_id"]}, entry, upsert=True)

    def process_snapshot(self, previous):
        self._preflight()
        counts = deepcopy(previous.get("counters") or {
            "processed": 0, "inserted": 0, "updated": 0,
            "removed_legacy_related_works": 0,
            "resolved_duplicate_cod_rh": 0,
        })
        last_id = previous.get("last_id")
        query = {"_id": {"$gt": last_id}} if last_id is not None else {}
        cursor = self.source_collection.find(query).sort("_id", 1).batch_size(
            self.batch_size
        )
        progress = None
        if self.verbose > 0:
            progress = ProgressLogger(
                "minciencias_opendata_person",
                total=self.source_documents,
                initial=counts["processed"],
            ).start("release={} batch_size={}".format(
                self.release_name, self.batch_size))
        batch = []
        for source in cursor:
            batch.append(source)
            if len(batch) < self.batch_size:
                continue
            before = dict(counts)
            self._process_batch(batch, counts)
            counts["processed"] += len(batch)
            last_id = batch[-1]["_id"]
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {"last_id": last_id, "counters": deepcopy(counts)}},
            )
            self._report_progress(progress, before, counts, len(batch))
            batch.clear()
        if batch:
            before = dict(counts)
            self._process_batch(batch, counts)
            counts["processed"] += len(batch)
            last_id = batch[-1]["_id"]
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {"last_id": last_id, "counters": deepcopy(counts)}},
            )
            self._report_progress(progress, before, counts, len(batch))
        if progress is not None:
            progress.done()
        if counts["processed"] != self.source_documents:
            raise RuntimeError("person snapshot changed while it was being imported")
        return counts

    def run(self):
        if self.source_mode == "legacy":
            return self._legacy.run()
        previous = self.runs.find_one({"_id": self.run_id}) or {}
        identity = {
            "contract_version": CONTRACT_VERSION,
            "import_policy": IMPORT_POLICY,
            "release": self.release_name,
            "audit": self.audit_name,
            "materialization_run": self.materialization_run,
            "source_collection": self.source_collection_name,
            "source_documents": self.source_documents,
        }
        if previous and any(
            previous.get(key) != value for key, value in identity.items()
        ):
            raise RuntimeError("person import run exists with different source evidence")
        if previous.get("status") == "complete":
            return deepcopy(previous["summary"])
        now = int(time())
        if not previous:
            previous = {
                "_id": self.run_id, "status": "pending",
                "created_at": now, **identity,
            }
            self.runs.insert_one(previous)
        self.runs.update_one(
            {"_id": self.run_id},
            {"$set": {"status": "running", "started_at": now},
             "$inc": {"attempts": 1}, "$unset": {"error": ""}},
        )
        try:
            counters = self.process_snapshot(previous)
            summary = {**identity, **counters}
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {
                    "status": "complete", "finished_at": int(time()),
                    "summary": deepcopy(summary),
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
