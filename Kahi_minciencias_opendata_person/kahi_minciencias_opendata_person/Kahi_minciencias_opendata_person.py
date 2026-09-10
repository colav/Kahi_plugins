"""Import the audited ScienTI person snapshot into Kahi."""

from copy import deepcopy
import re
from time import time

from kahi.KahiBase import KahiBase
from pymongo import MongoClient, ReplaceOne, TEXT


CONTRACT_VERSION = "scienti-kahi-ingestion-v1"
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
        self.config = config
        self.mongodb_url = config["database_url"]
        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]
        self.collection.create_index("external_ids.id")
        self.collection.create_index("external_ids.id.COD_RH")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        source_config = config["minciencias_opendata_person"]
        forbidden = {
            "source_mode", "researchers", "cvlac", "groups_production",
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
        self.run_id = "persons:{}".format(self.release_name)
        self.affiliation_cache = {}
        self.affiliation_name_cache = {}

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
            {"external_ids.id": source_id}, {"names": 1, "types": 1}
        )
        if not result:
            result = self.db["affiliations"].find_one(
                {"_id": source_id}, {"names": 1, "types": 1}
            )
        self.affiliation_cache[source_id] = result
        return result

    def _target_affiliation_by_name(self, name):
        name = str(name or "").strip()
        key = name.casefold()
        if not key:
            return None
        if key in self.affiliation_name_cache:
            return self.affiliation_name_cache[key]
        query = {
            "names.name": {
                "$regex": "^{}$".format(re.escape(name)), "$options": "i",
            }
        }
        matches = list(
            self.db["affiliations"].find(
                query, {"names": 1, "types": 1}
            ).limit(2)
        )
        result = matches[0] if len(matches) == 1 else None
        self.affiliation_name_cache[key] = result
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

    def _merge(self, existing, source):
        if not existing:
            entry = self.empty_person()
            entry["_id"] = source["_id"]
            entry.update({field: deepcopy(source[field]) for field in PERSON_FIELDS})
            entry["affiliations"] = self._resolve_affiliations(source["affiliations"])
            return entry

        entry = deepcopy(existing)
        for field in SCALAR_FIELDS:
            value = source.get(field)
            if value not in (None, "", [], {}, -1):
                entry[field] = deepcopy(value)
        for field in {"updated", "external_ids", "ranking", "degrees", "subjects"}:
            entry[field] = self._replace_managed(entry, source, field)
        for field in {"aliases", "keywords"}:
            entry.setdefault(field, [])
            for item in source.get(field, []):
                append_unique(entry[field], item)
        entry.setdefault("affiliations", [])
        for item in self._resolve_affiliations(source.get("affiliations", [])):
            append_unique(entry["affiliations"], item)
        entry["related_works"] = self._replace_managed(
            entry, source, "related_works"
        )
        entry.setdefault("citations_count", [])
        entry.setdefault("products_count", 0)
        return entry

    def _preflight(self):
        group_ids = {
            str(value) for value in self.source_collection.distinct("affiliations.id")
            if GROUP_RE.fullmatch(str(value))
        }
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

    def _process_batch(self, batch, counts):
        codes = {}
        for source in batch:
            code = self._cod_rh(source)
            self._validate_document(source)
            codes[code] = source
        existing_by_code = {}
        query = {
            "$or": [
                {"external_ids.id.COD_RH": {"$in": list(codes)}},
                {"_id": {"$in": list(codes)}},
            ]
        }
        for existing in self.collection.find(query):
            matched = {
                str(item.get("id", {}).get("COD_RH") or "")
                for item in existing.get("external_ids", [])
                if isinstance(item.get("id"), dict)
            }.intersection(codes)
            if str(existing["_id"]) in codes:
                matched.add(str(existing["_id"]))
            for code in matched:
                if code in existing_by_code and existing_by_code[code]["_id"] != existing["_id"]:
                    raise RuntimeError("a COD_RH resolves to multiple Kahi people")
                existing_by_code[code] = existing

        entries = []
        for code, source in codes.items():
            existing = existing_by_code.get(code)
            if existing:
                counts["removed_legacy_related_works"] += sum(
                    is_minciencias(item) and item.get("source") != "doi"
                    for item in existing.get("related_works", [])
                )
            entry = self._merge(existing, source)
            entries.append(entry)
            action = "updated" if existing else "inserted"
            counts[action] += 1
            if self.verbose > 4:
                print("{} person {} from audited snapshot".format(action, code))
        operations = [
            ReplaceOne({"_id": entry["_id"]}, entry, upsert=True)
            for entry in entries
        ]
        try:
            self.collection.bulk_write(operations, ordered=False)
        except TypeError:
            for entry in entries:
                self.collection.replace_one({"_id": entry["_id"]}, entry, upsert=True)

    def process_snapshot(self, previous):
        self._preflight()
        counts = deepcopy(previous.get("counters") or {
            "processed": 0, "inserted": 0, "updated": 0,
            "removed_legacy_related_works": 0,
        })
        last_id = previous.get("last_id")
        query = {"_id": {"$gt": last_id}} if last_id is not None else {}
        cursor = self.source_collection.find(query).sort("_id", 1).batch_size(
            self.batch_size
        )
        batch = []
        for source in cursor:
            batch.append(source)
            if len(batch) < self.batch_size:
                continue
            self._process_batch(batch, counts)
            counts["processed"] += len(batch)
            last_id = batch[-1]["_id"]
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {"last_id": last_id, "counters": deepcopy(counts)}},
            )
            batch.clear()
        if batch:
            self._process_batch(batch, counts)
            counts["processed"] += len(batch)
            last_id = batch[-1]["_id"]
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {"last_id": last_id, "counters": deepcopy(counts)}},
            )
        if counts["processed"] != self.source_documents:
            raise RuntimeError("person snapshot changed while it was being imported")
        return counts

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
