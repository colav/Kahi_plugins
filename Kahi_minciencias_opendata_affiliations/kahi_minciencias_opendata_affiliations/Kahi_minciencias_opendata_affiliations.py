from kahi_impactu_utils.Utils import check_date_format
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from pymongo.errors import DuplicateKeyError
from joblib import Parallel, delayed
from datetime import datetime as dt
from kahi.KahiBase import KahiBase
from kahi.progress import ProgressLogger
from kahi_impactu_utils.Mongo import order_document
from unidecode import unidecode
from thefuzz import fuzz
from copy import deepcopy
from bson import json_util
import hashlib
from pathlib import Path
from threading import RLock
from time import time
import re


CONTRACT_VERSION = "scienti-kahi-ingestion-v1"
INSTITUTION_RESOLUTION_VERSION = "v3"
DEFAULT_IMPORT_REVISION = "institutions-v3-split-lists"
SNAPSHOT_ENTITIES = {
    "works", "projects", "patents", "events", "persons", "affiliations"
}
SNAPSHOT_LIST_FIELDS = (
    "updated", "names", "aliases", "abbreviations", "types", "status",
    "addresses", "external_urls", "external_ids", "subjects", "ranking",
    "description",
)
SNAPSHOT_FIELDS = set(SNAPSHOT_LIST_FIELDS) | {"year_established", "relations"}
INVALID_NITS = {
    "000000000", "000000010", "111111111", "123456789", "999999999",
}
PRESERVE_FULL_INSTITUTION_NAMES = {
    "universidad de sucre",
    "universidad militar nueva granada",
    "universidad del magdalena",
    "universidad de pamplona",
    "universidad de la salle",
}


# GrupLAC appends the endorsement status to each declared institution name,
# e.g. "UNIVERSIDAD SURCOLOMBIANA - (No Avalado)". It is not part of the name.
# A group relation means endorsement (aval): downstream importers attribute the
# group's production to related institutions, so non-endorsing declarations are
# excluded from relations (the source keeps them) instead of being resolved.
ENDORSEMENT_MARKER = re.compile(
    r"\s*-?\s*\(\s*(no\s+)?avalad[oa]\s*\)\s*$", re.IGNORECASE)


def strip_endorsement_marker(name):
    """Remove GrupLAC's trailing "(Avalado)"/"(No Avalado)" status marker."""
    return ENDORSEMENT_MARKER.sub("", str(name or "")).strip()


def is_non_endorsing_declaration(name):
    match = ENDORSEMENT_MARKER.search(str(name or ""))
    return bool(match and match.group(1))


def split_endorsement_declarations(reg):
    """Return the record with only endorsing declarations, plus the excluded ones.

    Endorsing markers are stripped; non-endorsing declarations (whole relations
    or parts of compound declarations) are removed and returned for auditing.
    """
    relations, excluded = [], []
    for relation in reg.get("relations", []) or []:
        name = relation.get("name")
        if name is None:
            relations.append(relation)
            continue
        kept = []
        for part in split_institution_names(name) or [str(name)]:
            if is_non_endorsing_declaration(part):
                excluded.append({**relation, "name": part})
            else:
                kept.append(strip_endorsement_marker(part))
        if kept:
            relations.append({**relation, "name": "; ".join(kept)})
    return {**reg, "relations": relations}, excluded


def split_institution_names(value):
    """Expand list separators without splitting campus parenthetical labels."""
    parts, buffer = [], []
    depth = 0
    for char in str(value or ""):
        if char in "([":
            depth += 1
        elif char in ")]":
            depth = max(0, depth - 1)
        if char in ";|" and depth == 0:
            name = " ".join("".join(buffer).split())
            if name:
                parts.append(name)
            buffer = []
        else:
            buffer.append(char)
    name = " ".join("".join(buffer).split())
    if name:
        parts.append(name)
    return parts


def normalize_scienti_nit(value):
    """Return a plausible Colombian NIT, excluding placeholders."""
    raw = str(value or "").strip().split("-", maxsplit=1)[0]
    digits = "".join(char for char in raw if char.isdigit())
    if not 7 <= len(digits) <= 10:
        return ""
    if digits in INVALID_NITS or len(set(digits)) == 1:
        return ""
    return digits


def is_strict_institution_match(match):
    """Accept exact names or high-confidence similarities only."""
    if not match:
        return False
    scores = match["scores"]
    return any([
        match["normalized_name"] == match["normalized_source_name"],
        scores["ratio"] >= 94,
        scores["token_sort_ratio"] >= 96,
        scores["token_set_ratio"] >= 95
        and match["distinctive_token_overlap"] >= 0.80
        and match["distinctive_name_token_count"] >= 3,
        scores["token_set_ratio"] >= 99
        and match["distinctive_token_overlap"] >= 0.80,
        scores["wratio"] >= 96
        and match["distinctive_token_overlap"] >= 0.80,
    ])


def legacy_group_ranking(reg):
    """Build the ranking supplied by the selected legacy group record."""
    return {
        "source": "minciencias",
        "rank": reg.get("nme_clasificacion_gr", ""),
        "order": reg.get("orden_clas_gr", ""),
        "date": check_date_format(reg.get("ano_convo", "")),
    }


def replace_legacy_group_ranking(entry, reg):
    """Refresh Minciencias ranking without touching evidence from other sources."""
    entry["ranking"] = [
        item for item in entry.get("ranking", [])
        if item.get("source") != "minciencias"
    ]
    entry["ranking"].append(legacy_group_ranking(reg))


def replace_legacy_group_name(entry, name):
    """Refresh the legacy name while preserving names supplied by other sources."""
    name = str(name or "").strip()
    if not name:
        return
    entry["names"] = [
        item for item in entry.get("names", [])
        if item.get("source") != "minciencias"
    ]
    entry["names"].append({
        "source": "minciencias", "lang": "es", "name": name,
    })


class Kahi_minciencias_opendata_affiliations(KahiBase):

    config = {}

    @staticmethod
    def manages_checkpoints(task_config):
        """Resolve current/code/catalog changes even when Kahi has a success log."""
        return task_config.get("source_mode") == "snapshot"

    @staticmethod
    def implementation_fingerprint():
        """Invalidate checkpoints automatically when this importer's code changes."""
        return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    missing_group_names = {
        "COL0014761": "Política, Género y Democracia",
        "COL0058004": "Estudios Hobbesianos",
        "COL0095339": "Grupo de Investigación Tlamatinime sobre Ontología Latinoamericana",
        "COL0011438": "Resiliencia y Saneamiento RESA",
    }

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("types.type")
        self.collection.create_index("names.name")
        self.collection.create_index([("names.name", TEXT)])

        source_config = config["minciencias_opendata_affiliations"]
        self.source_mode = source_config.get("source_mode")
        if self.source_mode == "legacy_open_data":
            self.source_mode = "legacy"
        if self.source_mode not in {"snapshot", "legacy"}:
            raise ValueError(
                "minciencias_opendata_affiliations.source_mode must explicitly be "
                "'snapshot' or 'legacy'"
            )
        self.openadata_client = MongoClient(source_config["database_url"])
        if source_config["database_name"] not in self.openadata_client.list_database_names():
            raise Exception("Database {} not found in {}".format(
                source_config['database_name'], source_config["database_url"]))

        self.openadata_db = self.openadata_client[source_config["database_name"]]

        if self.source_mode == "snapshot":
            self._configure_snapshot_source(source_config)
        else:
            self._configure_legacy_source(source_config)

        self.n_jobs = source_config.get("num_jobs", 1)
        self.verbose = source_config.get("verbose", 0)
        self.batch_size = max(1, int(source_config.get("batch_size", 500)))
        if self.source_mode == "snapshot":
            self.runs = self.db[source_config.get(
                "runs_collection", "minciencias_opendata_affiliations_runs"
            )]

        self.inserted_cod_grupo = []
        self.institution_match_cache = {}
        self.scienti_catalog = self.load_scienti_catalog(config)
        self.scienti_name_index = self.index_scienti_catalog(self.scienti_catalog)
        self.unique_scienti_nits = self.catalog_unique_nits(self.scienti_catalog)
        self.materialized_scienti_codes = {}
        self.materialization_lock = RLock()
        if self.source_mode == "snapshot":
            # Revision belongs to the implementation, never to the workflow.
            self.import_revision = DEFAULT_IMPORT_REVISION
            self.implementation_sha256 = self.implementation_fingerprint()
            self.catalog_fingerprint = self.fingerprint_catalog()
            signature = hashlib.sha256(json_util.dumps({
                "resolver": INSTITUTION_RESOLUTION_VERSION,
                "import_revision": self.import_revision,
                "implementation_sha256": self.implementation_sha256,
                "catalog": self.catalog_fingerprint,
                "publication_kind": self.snapshot_publication_kind,
                "collection": self.snapshot_collection_name,
                "audit": self.snapshot_audit_name,
                "materialization_run": self.snapshot_materialization_run,
            }, sort_keys=True).encode()).hexdigest()
            self.run_id = "affiliations:{}:{}".format(
                self.snapshot_release_name, signature)
            self.resolution_audits = self.db[source_config.get(
                "institution_resolutions_collection",
                "minciencias_opendata_affiliations_institution_resolutions",
            )]
            self.resolution_audits.create_index([("run_id", 1), ("kind", 1)])

        for reg in self.collection.find({"types.type": "group"}):
            for ext in reg["external_ids"]:
                if ext["source"] == "minciencias":
                    self.inserted_cod_grupo.append(ext["id"])

    def _configure_legacy_source(self, source_config):
        collection_name = source_config.get(
            "collection_name", "gruplac_groups_data")
        if not collection_name or collection_name not in self.openadata_db.list_collection_names():
            raise Exception("Collection {} not found in {}".format(
                collection_name, source_config["database_url"]))
        self.openadata_collection = self.openadata_db[collection_name]
        self.openadata_collection.create_index(
            [("cod_grupo_gr", ASCENDING), ("ano_convo", DESCENDING)],
            name="cod_grupo_gr_1_ano_convo_-1",
        )

    def _configure_snapshot_source(self, source_config):
        release_name = str(source_config.get("release_name") or "")
        if not release_name or release_name == "current":
            return self._configure_current_affiliation_source(source_config)
        release = self.openadata_db["scienti_final_release_publications"].find_one(
            {"_id": release_name}
        ) or {}
        audit = self.openadata_db["scienti_final_release_audits"].find_one(
            {"_id": release.get("audit")}
        ) or {}
        collections = release.get("collections") or {}
        materialization_runs = release.get("materialization_runs") or {}
        evidence = (audit.get("evidence") or {}).get("affiliations") or {}
        collection_name = str(collections.get("affiliations") or "")
        configured_collection = str(source_config.get("collection_name") or "")
        if (
            release.get("status") != "published"
            or release.get("entity_count") != 6
            or set(collections) != SNAPSHOT_ENTITIES
            or set(materialization_runs) != SNAPSHOT_ENTITIES
            or audit.get("status") != "passed"
            or audit.get("release_name") != release_name
            or audit.get("collections") != collections
            or audit.get("materialization_runs") != materialization_runs
            or int(audit.get("critical_anomalies") or 0)
            or evidence.get("collection") != collection_name
            or evidence.get("materialization_run")
            != materialization_runs.get("affiliations")
            or (configured_collection and configured_collection != collection_name)
            or collection_name not in self.openadata_db.list_collection_names()
        ):
            raise RuntimeError("six-entity affiliation snapshot is not proven and published")
        actual = self.openadata_db[collection_name].count_documents({})
        if actual != int(evidence.get("documents") or -1):
            raise RuntimeError("affiliation snapshot count changed after release audit")
        self.snapshot_release_name = release_name
        self.snapshot_publication_kind = "six_entity_release"
        self.snapshot_audit_name = audit["_id"]
        self.snapshot_materialization_run = materialization_runs["affiliations"]
        self.snapshot_documents = actual
        self.snapshot_collection_name = collection_name
        self.openadata_collection = self.openadata_db[collection_name]

    def _configure_current_affiliation_source(self, source_config):
        """Pin the entity's own audited publication once, independently of releases."""
        publications = self.openadata_db["scienti_affiliation_publications"]
        pointer = publications.find_one({"_id": "current"}) or {}
        run_name = str(pointer.get("current_run") or "")
        if not run_name or run_name == "current":
            raise RuntimeError("current affiliation snapshot has no published target")
        publication = publications.find_one({"_id": run_name}) or {}
        audit_name = str(publication.get("audit") or "")
        audit = self.openadata_db["scienti_affiliation_materialization_audits"].find_one(
            {"_id": audit_name}
        ) or {}
        run = self.openadata_db["scienti_affiliation_materialization_runs"].find_one(
            {"_id": run_name}
        ) or {}
        collection_name = str(publication.get("collection") or "")
        documents = publication.get("documents")
        summary = run.get("summary") or {}
        configured_collection = str(source_config.get("collection_name") or "")
        if (
            publication.get("status") != "published"
            or pointer.get("collection") != collection_name
            or pointer.get("audit") != audit_name
            or audit.get("status") != "passed"
            or audit.get("run_name") != run_name
            or audit.get("collection") != collection_name
            or int(audit.get("critical_anomalies") or 0)
            or audit.get("documents") != documents
            or not audit.get("source_fingerprints")
            or audit.get("source_fingerprints") != publication.get("source_fingerprints")
            or run.get("status") != "complete"
            or (run.get("config") or {}).get("target_collection") != collection_name
            or summary.get("collection") != collection_name
            or summary.get("audit") != audit_name
            or summary.get("documents") != documents
            or not isinstance(documents, int) or documents < 1
            or (configured_collection and configured_collection != collection_name)
            or collection_name not in self.openadata_db.list_collection_names()
        ):
            raise RuntimeError("current affiliation snapshot is not proven and published")
        if self.openadata_db[collection_name].count_documents({}) != documents:
            raise RuntimeError("affiliation snapshot count changed after materialization audit")
        self.snapshot_release_name = run_name
        self.snapshot_publication_kind = "affiliation_publication"
        self.snapshot_audit_name = audit_name
        self.snapshot_materialization_run = run_name
        self.snapshot_documents = documents
        self.snapshot_collection_name = collection_name
        self.openadata_collection = self.openadata_db[collection_name]

    def rename_institution(self, name):
        if unidecode(name) in PRESERVE_FULL_INSTITUTION_NAMES:
            return name
        if name in {
            "colegio mayor nuestra señora del rosario",
            "colegio mayor de nuestra señora del rosario",
            "colegio mayor nuestra senora del rosario",
        }:
            return "universidad del rosario"
        elif name == "universidad de la guajira":
            return "guajira"
        elif "minuto" in name and "dios" in name:
            return "minuto dios"
        elif "salle" in name:
            return "universidad salle"
        elif "icesi" in name:
            return "icesi"
        elif "sede" in name:
            # Preserve the campus: removing it can link a group to the parent
            # institution or to a different campus with the same base name.
            return name
        elif name == "universidad militar nueva granada":
            return "nueva granada"
        elif "pamplona" in name:
            return "pamplona"
        elif "sucre" in name:
            return "sucre"
        elif name in {"universidad santo tomás", "universidad santo tomas"}:
            return "universidad santo tomas"
        elif name == "universidad simón bolívar":
            return "simon bolivar"
        elif "unidades" in name and "santander" in name:
            return "unidades tecnológicas santander"
        elif "popayán" in name:
            return "popayán"
        elif any(
            value in name
            for value in [
                "tecnológico metropolitano",
                "tecnologico metropolitano",
                "institucion universitaria itm",
                "institución universitaria itm",
                "institucion universitaria - itm",
                "institución universitaria - itm",
            ]
        ):
            return "instituto tecnologico metropolitano"
        elif "cesmag" in name:
            return "estudios superiores maría goretti"
        elif "distrital francisco" in name:
            return "distrital francisco josé"
        elif name in ["universidad industrial de santander", "uis"]:
            return "universidad industrial de santander"
        elif name in ["universidad de santander", "udes"]:
            return "universidad de santander"
        elif "universidad" in name and "francisco" in name and "paula" in name and "santander" in name:
            return "universidad francisco paula santander"
        elif "magdalena" in name:
            return "magdalena"
        elif "corporacion universitaria iberoamericana" == name:
            return "iberoamericana"
        elif name in ["corporacion universitaria adventista", "corporación universitaria adventista"]:
            return "colombia adventist university"
        elif name in ["fundacion hospitalaria san vicente de paul", "fundación hospitalaria san vicente de paúl"]:
            return "hospital universitario de san vicente fundacion"
        elif name in ["alcaldia de medellin", "alcaldía de medellín"]:
            return "municipality of medellin"
        else:
            return name

    def normalize_institution_name(self, name):
        if name is None:
            return ""
        name = unidecode(str(name).lower())
        name = name.replace("(colombia)", "").replace("bogotá", "")
        name = re.sub(r"[^\w\s]", " ", name)
        name = re.sub(r"\s+", " ", name).strip()
        return name

    def is_non_institution_fragment(self, name):
        """Observed country-only fragment in historical institution declarations."""
        return self.normalize_institution_name(name) == "colombia"

    def normalize_inst_aval(self, name):
        name = name.lower().strip()
        name = self.rename_institution(name)
        return self.normalize_institution_name(name)

    def declared_campus_parent_name(self, name):
        """Return an explicit parent from a campus-style avaladora label.

        DAM commonly represents a campus as ``campus label (parent name)``.
        Only explicit campus markers or a child name that starts with the full
        parent name are collapsed.  This deliberately excludes clinics and
        other units that merely mention a university in parentheses.
        """
        raw = str(name or "").strip()
        match = re.search(r"\(([^()]*)\)\s*$", raw)
        if not match:
            return ""
        parent = match.group(1).strip()
        child = raw[:match.start()].strip()
        normalized_parent = self.normalize_institution_name(parent)
        normalized_child = self.normalize_institution_name(child)
        if not normalized_parent or not normalized_child:
            return ""
        explicit_campus = bool(re.search(
            r"\b(?:sede|campus|seccional)\b", normalized_child))
        parent_with_location = normalized_child.startswith(
            normalized_parent + " ")
        if not explicit_campus and not parent_with_location:
            return ""
        return parent

    def get_institution_name(self, institution):
        name = ""
        for n in institution.get("names", []):
            if n.get("lang") == "es" and n.get("name"):
                return n["name"]
            elif n.get("lang") == "en" and n.get("name") and not name:
                name = n["name"]
        if not name and institution.get("names"):
            name = institution["names"][0].get("name", "")
        return name

    def aval_institution_id(self, inst_aval):
        normalized = self.normalize_inst_aval(inst_aval)
        digest = hashlib.sha1(
            "v1|CO|{}".format(normalized).encode("utf-8")
        ).hexdigest()[:12]
        return "IUA_{}".format(digest)

    def affiliation_address_from_group(self, reg):
        return {
            "lat": "",
            "lng": "",
            "postcode": "",
            "state": reg.get("nme_departamento_gr", ""),
            "city": reg.get("nme_municipio_gr", ""),
            "country": "Colombia",
            "country_code": "CO"
        }

    def affiliation_address_from_institution(self, institution, reg):
        addresses = institution.get("addresses", [])
        if not addresses:
            return self.affiliation_address_from_group(reg)
        return {
            "lat": addresses[0].get("lat", None),
            "lng": addresses[0].get("lng", None),
            "postcode": addresses[0].get("postcode", None),
            "state": addresses[0].get("state", None),
            "city": addresses[0].get("city", None),
            "country": addresses[0].get("country", None),
            "country_code": addresses[0].get("country_code", None)
        }

    def append_unique(self, values, item):
        if item not in values:
            values.append(item)

    def load_scienti_catalog(self, config):
        """Load a compact lookup catalog; no affiliation is inserted here."""
        source_config = config.get("minciencias_opendata_affiliations", {})
        databases = source_config.get("scienti_databases")
        if databases is None:
            databases = config.get("scienti_affiliations", {}).get("databases", [])
        if isinstance(databases, dict):
            databases = [databases]
        catalog = {}
        self.catalog_sources = []
        projection = {
            "_id": 0, "COD_INST": 1, "COD_INST_MACRO": 1,
            "ID_INSTITUCION": 1, "NME_INST": 1, "NME_INST_FILTRO": 1,
            "SGL_INST": 1, "TXT_NIT": 1, "COD_IES": 1,
            "TXT_CIUDAD_INST": 1, "SGL_PAIS": 1, "country": 1,
            "department": 1, "city": 1, "URL_HOME_PAGE": 1,
        }
        for item in databases or []:
            client = MongoClient(item["database_url"])
            try:
                db = client[item["database_name"]]
                name = item.get("institution_collection_name", "institution")
                if name not in db.list_collection_names():
                    raise RuntimeError("Scienti institution catalog {}.{} not found".format(
                        item["database_name"], name))
                record_hashes = []
                for reg in db[name].find({}, projection):
                    encoded = json_util.dumps(
                        reg, sort_keys=True, separators=(",", ":")
                    ).encode()
                    record_hashes.append(hashlib.sha256(encoded).hexdigest())
                    code = str(reg.get("COD_INST") or "").strip()
                    if not code or code == "000000000000" or not reg.get("NME_INST"):
                        continue
                    previous = catalog.get(code)
                    score = sum(bool(value) for value in reg.values())
                    previous_score = sum(bool(value) for value in previous.values()) if previous else -1
                    # Resolve completeness ties independently of cursor order.
                    if score > previous_score or (
                        score == previous_score and encoded > json_util.dumps(
                            previous, sort_keys=True, separators=(",", ":")
                        ).encode()
                    ):
                        catalog[code] = reg
                self.catalog_sources.append({
                    "database_url": item["database_url"],
                    "database_name": item["database_name"],
                    "collection": name,
                    "documents": len(record_hashes),
                    "sha256": hashlib.sha256(
                        "".join(sorted(record_hashes)).encode()
                    ).hexdigest(),
                })
            finally:
                client.close()
        return catalog

    def fingerprint_catalog(self):
        digest = hashlib.sha256()
        for code, record in sorted(self.scienti_catalog.items()):
            digest.update(json_util.dumps(
                record, sort_keys=True, separators=(",", ":")
            ).encode())
            digest.update(b"\n")
        return {
            "sha256": digest.hexdigest(), "codes": len(self.scienti_catalog),
            "sources": sorted(self.catalog_sources, key=lambda item: (
                item["database_url"], item["database_name"], item["collection"]
            )),
        }

    def record_institution_resolution(
            self, institution, *, created, origin, code=None, committed=True):
        """Store durable counters outside affiliation documents."""
        audits = getattr(self, "resolution_audits", None)
        if audits is None or not institution:
            return
        identifier = str(institution["_id"])
        identity = hashlib.sha256(identifier.encode()).hexdigest()
        update = {
            "$setOnInsert": {
                "run_id": self.run_id, "kind": "institution",
                "institution_id": institution["_id"], "first_seen": int(time()),
            },
            "$max": {"created": bool(created), "committed": bool(committed)},
            "$addToSet": {"origins": origin},
        }
        if code:
            update["$addToSet"]["catalog_codes"] = code
        if created:
            update["$set"] = {"created_from": origin}
        query = {"_id": self.run_id + ":institution:" + identity}
        try:
            audits.update_one(query, update, upsert=True)
        except DuplicateKeyError:
            # Concurrent workers may declare the same provisional institution.
            audits.update_one(query, update)

    def record_group_resolution(self, reg, source_relation, institution, resolved, evidence):
        audits = getattr(self, "resolution_audits", None)
        if audits is None:
            return
        group_id = str(reg.get("_id") or reg.get("cod_grupo_gr") or "")
        key = json_util.dumps([
            group_id, source_relation.get("id"), source_relation.get("name")
        ], sort_keys=True)
        identity = hashlib.sha256(key.encode()).hexdigest()
        kind = "compound" if evidence.get("compound") else "relation"
        document = {
            "run_id": self.run_id, "kind": kind, "group_id": group_id,
            "declared_name": source_relation.get("name") or "",
            "declared_id": source_relation.get("id") or "",
            "institution_id": institution["_id"] if institution else None,
            "resolved_ids": [item["_id"] for item in resolved],
            "outcome": evidence.get("outcome", "unresolved"),
            "evidence": deepcopy(evidence),
        }
        query = {"_id": self.run_id + ":" + kind + ":" + identity}
        if kind == "compound":
            removed = document["evidence"].pop("removed_relation_ids", [])
            audits.update_one(query, {
                "$set": document,
                "$addToSet": {"removed_relation_ids": {"$each": removed}},
            }, upsert=True)
        else:
            audits.replace_one(query, document, upsert=True)

    def institution_resolution_summary(self):
        institutions = {
            "run_id": self.run_id, "kind": "institution", "committed": True,
        }
        relations = {"run_id": self.run_id, "kind": "relation"}
        count = self.resolution_audits.count_documents
        created = count({**institutions, "created": True})
        total = count(institutions)
        resolved_ids = self.resolution_audits.distinct("resolved_ids", relations)
        present = sum(self.collection.count_documents({
            "_id": {"$in": resolved_ids[start:start + 500]},
            "types.type": {"$ne": "group"},
        }) for start in range(0, len(resolved_ids), 500))
        return {
            "institutions": total, "institutions_created": created,
            "institutions_reused": total - created,
            "pending_institutions": count({
                "run_id": self.run_id, "kind": "institution", "committed": {"$ne": True}}),
            "institutions_created_scienti": count({
                **institutions, "created": True, "created_from": "scienti"}),
            "institutions_created_provisional": count({
                **institutions, "created": True, "created_from": "provisional"}),
            "relations_processed": count(relations),
            "relations_excluded_non_institution": count({
                **relations, "outcome": "excluded", "evidence.reason": "country_only_fragment"}),
            "relations_excluded_non_endorsing": count({
                **relations, "outcome": "excluded", "evidence.reason": "non_endorsing_institution"}),
            "non_endorsing_prior_links_removed": count({
                **relations, "outcome": "excluded", "evidence.reason": "non_endorsing_institution",
                "evidence.removed_relation_ids.0": {"$exists": True}}),
            "compound_declarations": count({"run_id": self.run_id, "kind": "compound"}),
            "compound_declarations_unresolved": count({
                "run_id": self.run_id, "kind": "compound", "outcome": "unresolved"}),
            "enriched_compound_institutions_repaired": count({
                "run_id": self.run_id, "kind": "enriched_compound_repair", "committed": True}),
            "enriched_compound_links_kept": count({
                "run_id": self.run_id, "kind": "enriched_compound_group", "outcome": "kept"}),
            "enriched_compound_links_removed": count({
                "run_id": self.run_id, "kind": "enriched_compound_group", "outcome": "removed"}),
            **{"relations_" + outcome: count({**relations, "outcome": outcome})
               for outcome in ("catalog", "existing", "provisional", "unresolved")},
            "catalog_ambiguous": count({
                **relations, "evidence.catalog_status": "ambiguous"}),
            "missing_relation_targets": len(resolved_ids) - present,
        }

    def index_scienti_catalog(self, catalog):
        index = {}
        for code, reg in catalog.items():
            for value in (reg.get("NME_INST"), reg.get("NME_INST_FILTRO")):
                if len(split_institution_names(value)) > 1:
                    continue
                for normalized in {
                    self.normalize_institution_name(value),
                    self.normalize_inst_aval(str(value or "")),
                }:
                    if normalized:
                        index.setdefault(normalized, set()).add(code)
        return index

    def catalog_unique_nits(self, catalog):
        """Return valid NIT values owned by exactly one Scienti code."""
        owners = {}
        for code, reg in catalog.items():
            nit = normalize_scienti_nit(reg.get("TXT_NIT"))
            if nit:
                owners.setdefault(nit, set()).add(code)
        return {nit for nit, codes in owners.items() if len(codes) == 1}

    @staticmethod
    def scienti_value(reg, field):
        value = reg.get(field)
        return str(value or "").strip()

    def scienti_address(self, reg):
        city_doc = (reg.get("city") or [{}])[0] if isinstance(reg.get("city"), list) else {}
        department = reg.get("department") or []
        department = department[0] if isinstance(department, list) and department else {}
        nested_department = city_doc.get("department") or []
        nested_department = nested_department[0] if isinstance(nested_department, list) and nested_department else {}
        state = self.scienti_value(department, "TXT_NME_DEPARTAMENTO") or self.scienti_value(
            nested_department, "TXT_NME_DEPARTAMENTO")
        city = self.scienti_value(reg, "TXT_CIUDAD_INST") or self.scienti_value(
            city_doc, "TXT_NME_MUNICIPIO")
        country_doc = reg.get("country") or []
        country_doc = country_doc[0] if isinstance(country_doc, list) and country_doc else {}
        country = self.scienti_value(country_doc, "TXT_NME_PAIS") or "Colombia"
        country_code = self.scienti_value(country_doc, "SGL_PAIS_ISO_2") or "CO"
        return {
            "lat": "", "lng": "", "postcode": "", "state": state,
            "city": city, "country": country, "country_code": country_code,
        }

    def scienti_geo_score(self, reg, context):
        address = self.scienti_address(reg)
        source_state = self.normalize_institution_name(
            (context or {}).get("nme_departamento_gr", ""))
        source_city = self.normalize_institution_name(
            (context or {}).get("nme_municipio_gr", ""))
        state = self.normalize_institution_name(address.get("state"))
        city = self.normalize_institution_name(address.get("city"))
        return int(bool(source_state and state == source_state)) * 2 + int(
            bool(source_city and city == source_city)) * 3

    def resolve_scienti_catalog_record(self, inst_aval, reg=None, cod_inst=None):
        catalog = getattr(self, "scienti_catalog", {})
        code = str(cod_inst or "").strip()
        if code in catalog:
            return catalog[code]
        normalized = self.normalize_inst_aval(inst_aval)
        codes = set(getattr(self, "scienti_name_index", {}).get(normalized, set()))
        if not codes:
            return None
        candidates = [catalog[item] for item in codes]
        ranked = sorted(
            ((self.scienti_geo_score(item, reg), item) for item in candidates),
            key=lambda value: value[0], reverse=True,
        )
        if ranked[0][0] > 0 and (
                len(ranked) == 1 or ranked[0][0] > ranked[1][0]):
            return ranked[0][1]
        roots = {
            self.scienti_value(item, "COD_INST_MACRO")
            or self.scienti_value(item, "COD_INST")
            for item in candidates
        }
        if len(roots) == 1:
            root = next(iter(roots))
            return catalog.get(root) or ranked[0][1]
        return ranked[0][1] if len(ranked) == 1 else None

    def find_by_external_id(self, source, value):
        value = str(value or "").strip()
        if not value:
            return None
        return self.collection.find_one({
            "external_ids": {"$elemMatch": {"source": source, "id": value}},
            "types.type": {"$ne": "group"},
        })

    def find_snies_institution(self, code):
        """Resolve a SNIES principal or embedded campus to its principal."""
        code = str(code or "").strip()
        if not code:
            return None
        institution = self.collection.find_one({
            "external_ids": {"$elemMatch": {
                "source": "snies", "id": code, "provenance": "snies",
            }},
            "types.type": {"$ne": "group"},
        })
        if institution:
            return institution
        return self.collection.find_one({
            "campuses.external_ids": {"$elemMatch": {
                "source": "snies", "id": code, "provenance": "snies",
            }},
            "types.type": {"$ne": "group"},
        })

    @staticmethod
    def is_embedded_snies_campus(institution, code):
        return any(
            item.get("source") == "snies"
            and item.get("provenance") == "snies"
            and str(item.get("id")) == str(code)
            for campus in (institution or {}).get("campuses", [])
            for item in campus.get("external_ids", [])
        )

    def add_institution_parent(self, entry, parent):
        if not parent or entry.get("_id") == parent.get("_id"):
            return
        relation = {
            "types": parent.get("types", []), "id": parent["_id"],
            "name": self.get_institution_name(parent),
        }
        current = next((item for item in entry.get("relations", [])
                        if item.get("id") == parent["_id"]), None)
        if current:
            current.update(relation)
        else:
            entry.setdefault("relations", []).append(relation)

    def affiliation_cod_inst_codes(self, institution):
        return {
            str(item.get("id"))
            for item in (institution or {}).get("external_ids", [])
            if item.get("source") == "COD_INST" and item.get("id")
        }

    def is_authoritative_institution(self, institution):
        return any(
            item.get("source") == "ror" or
            (item.get("source") == "snies"
             and item.get("provenance") == "snies")
            for item in (institution or {}).get("external_ids", [])
        )

    def canonical_parent_for_distinct_code(
            self, institution, source, unique_nit=False):
        """Keep reliable identity as a parent without merging two COD_INST."""
        if not self.is_authoritative_institution(institution):
            return None
        source_name = self.normalize_institution_name(
            self.scienti_value(source, "NME_INST"))
        exact_name = bool(source_name) and any(
            self.normalize_institution_name(name) == source_name
            for name in self.institution_candidate_names(institution)
        )
        source_snies = self.scienti_value(source, "COD_IES")
        same_snies = bool(source_snies) and any(
            item.get("source") == "snies"
            and item.get("provenance") == "snies"
            and str(item.get("id")) == source_snies
            for item in institution.get("external_ids", [])
        )
        source_nit = normalize_scienti_nit(
            self.scienti_value(source, "TXT_NIT"))
        same_unique_nit = unique_nit and bool(source_nit) and any(
            item.get("source") == "nit"
            and str(item.get("id")) == source_nit
            for item in institution.get("external_ids", [])
        )
        return institution if exact_name or same_snies or same_unique_nit else None

    def materialize_scienti_institution(self, source, stack=None):
        """Serialize catalog materialization shared by parallel group workers."""
        lock = getattr(self, "materialization_lock", None)
        if lock is None:
            lock = RLock()
            self.materialization_lock = lock
        with lock:
            return self._materialize_scienti_institution(source, stack=stack)

    def _materialize_scienti_institution(self, source, stack=None):
        code = self.scienti_value(source, "COD_INST")
        if not code:
            return None
        cached = getattr(self, "materialized_scienti_codes", {}).get(code)
        if cached:
            return self.collection.find_one({"_id": cached})
        stack = set(stack or ())
        if code in stack:
            raise RuntimeError("cycle in Scienti institution hierarchy: {}".format(code))
        stack.add(code)
        macro = self.scienti_value(source, "COD_INST_MACRO")
        parent = None
        if macro and macro != code and macro in getattr(self, "scienti_catalog", {}):
            parent = self.materialize_scienti_institution(
                self.scienti_catalog[macro], stack)

        institution = self.find_by_external_id("COD_INST", code)
        nit = normalize_scienti_nit(self.scienti_value(source, "TXT_NIT"))
        snies = self.scienti_value(source, "COD_IES")
        snies_institution = self.find_snies_institution(snies)
        if (not institution and not parent and snies_institution
                and self.is_embedded_snies_campus(snies_institution, snies)):
            self.materialized_scienti_codes[code] = snies_institution["_id"]
            self.record_institution_resolution(
                snies_institution, created=False, origin="scienti", code=code)
            return snies_institution
        unique_nits = getattr(self, "unique_scienti_nits", None)
        if unique_nits is None:
            unique_nits = self.catalog_unique_nits(
                getattr(self, "scienti_catalog", {}))
        nit_is_unique = nit in unique_nits
        if not institution and not parent and nit and nit_is_unique:
            institution = self.find_by_external_id("nit", nit)
        if not institution and not parent and snies:
            institution = snies_institution
        if not institution and not parent:
            institution = self.find_matching_institution(
                self.scienti_value(source, "NME_INST"), reg={
                    "nme_departamento_gr": self.scienti_address(source)["state"],
                    "nme_municipio_gr": self.scienti_address(source)["city"],
                })

        candidate_parent = None
        existing_codes = self.affiliation_cod_inst_codes(institution)
        if institution and existing_codes and code not in existing_codes:
            candidate_parent = self.canonical_parent_for_distinct_code(
                institution, source, unique_nit=nit_is_unique)
            institution = None

        if institution:
            entry = deepcopy(institution)
        else:
            collision = self.collection.find_one({"_id": code})
            collision_codes = {
                item.get("id") for item in (collision or {}).get("external_ids", [])
                if item.get("source") == "COD_INST"
            }
            if collision and (
                    any(item.get("type") == "group" for item in collision.get("types", []))
                    or (collision_codes and code not in collision_codes)):
                raise RuntimeError("COD_INST collides with affiliation _id {}".format(code))
            entry = collision or self.empty_affiliation()
            entry["_id"] = code
        # A text-search candidate is a projection, not a document suitable for
        # replacement.  Preserve every standard affiliation field defensively
        # even if a partial document reaches this point.
        persisted = self.collection.find_one({"_id": entry["_id"]}) or {}
        for field, default in self.empty_affiliation().items():
            if field not in entry:
                entry[field] = deepcopy(persisted.get(field, default))
        entry.pop("score", None)
        name = self.scienti_value(source, "NME_INST")
        self.append_unique(entry["names"], {
            "source": "scienti", "lang": "es", "name": name})
        abbreviation = self.scienti_value(source, "SGL_INST")
        if abbreviation:
            self.append_unique(entry["abbreviations"], abbreviation)
        for ext in [
            {"provenance": "scienti", "source": "COD_INST", "id": code},
            {"provenance": "scienti", "source": "ID_INSTITUCION",
             "id": self.scienti_value(source, "ID_INSTITUCION")},
            {"provenance": "scienti", "source": "nit", "id": nit},
        ]:
            if ext["id"] and not any(item.get("source") == ext["source"] and
                                     item.get("id") == ext["id"]
                                     for item in entry["external_ids"]):
                entry["external_ids"].append(ext)
        self.append_unique(entry["types"], {
            "provenance": "scienti", "source": "scienti",
            "type": "education" if snies else "other",
        })
        address = self.scienti_address(source)
        if not entry["addresses"] and any(address.get(field) for field in ("state", "city")):
            entry["addresses"].append(address)
        entry["updated"] = [item for item in entry["updated"]
                            if item.get("source") != "scienti"]
        entry["updated"].append({"source": "scienti", "time": int(time())})
        self.add_institution_parent(entry, parent or candidate_parent)
        # Persist creation intent before the entity write. A retry can then
        # confirm the existing entity without losing the original creation count.
        self.record_institution_resolution(
            entry, created=not bool(persisted), origin="scienti", code=code,
            committed=False)
        self.collection.replace_one(
            {"_id": entry["_id"]},
            order_document(entry, self.empty_affiliation()), upsert=True)
        self.record_institution_resolution(
            entry, created=not bool(persisted), origin="scienti", code=code)
        self.materialized_scienti_codes[code] = entry["_id"]
        return self.collection.find_one({"_id": entry["_id"]})

    def institution_with_canonical_parents(self, institution, collection):
        resolved = [institution]
        seen = {institution["_id"]}
        queue = list(institution.get("relations", []))
        while queue:
            relation = queue.pop(0)
            parent = collection.find_one({
                "_id": relation.get("id"), "types.type": {"$ne": "group"}})
            if not parent or parent["_id"] in seen:
                continue
            seen.add(parent["_id"])
            queue.extend(parent.get("relations", []))
            authoritative = any(
                item.get("source") == "ror" or
                (item.get("source") == "snies" and item.get("provenance") == "snies")
                for item in parent.get("external_ids", []))
            if authoritative:
                resolved.append(parent)
        authoritative = [
            item for item in resolved
            if any(
                ext.get("source") == "ror" or
                (ext.get("source") == "snies"
                 and ext.get("provenance") == "snies")
                for ext in item.get("external_ids", []))
        ]
        return authoritative or [institution]

    def get_or_create_aval_institution(
            self, inst_aval, collection, reg, cod_inst=None, resolution=None):
        resolution = resolution if resolution is not None else {}
        names = split_institution_names(inst_aval)
        if len(names) > 1:
            raise ValueError("institution lists must be split before identity resolution")
        if not names:
            return None
        inst_aval = names[0]

        if self.is_non_institution_fragment(inst_aval):
            resolution.update(outcome="excluded", reason="country_only_fragment")
            return None

        declared_parent = self.declared_campus_parent_name(inst_aval)
        if declared_parent:
            # The group relation intentionally points only to the principal.
            # Do not pass a campus COD_INST into the parent resolver.
            return self.get_or_create_aval_institution(
                declared_parent, collection, reg, cod_inst=None,
                resolution=resolution)

        source = self.resolve_scienti_catalog_record(
            inst_aval, reg=reg, cod_inst=cod_inst)
        if source:
            resolution.update({
                "catalog_status": "matched",
                "catalog_code": self.scienti_value(source, "COD_INST"),
            })
            institution = self.materialize_scienti_institution(source)
            if institution:
                resolution["outcome"] = "catalog"
                return institution
        else:
            codes = getattr(self, "scienti_name_index", {}).get(
                self.normalize_inst_aval(inst_aval), set())
            resolution["catalog_status"] = "ambiguous" if len(codes) > 1 else "not_found"

        institution = self.find_matching_institution(
            inst_aval, reg=reg, cod_inst=cod_inst)
        if self.compound_iua_name(institution):
            institution = None
        if institution:
            resolution["outcome"] = (
                "provisional" if str(institution["_id"]).startswith("IUA")
                else "existing"
            )
            self.record_institution_resolution(
                institution, created=False, origin="existing")
            return institution

        institution_id = self.aval_institution_id(inst_aval)
        existing = collection.find_one({"_id": institution_id})
        if existing and any(len(split_institution_names(item.get("name"))) > 1
                            for item in existing.get("names", [])):
            # Old alias normalization may give a list and one component the
            # same fallback hash. Keep the original document as repair evidence.
            digest = hashlib.sha256(("atomic:" + self.normalize_institution_name(
                inst_aval)).encode()).hexdigest()[:12]
            institution_id = "IUA_" + digest
            existing = collection.find_one({"_id": institution_id})
        resolution["outcome"] = "provisional"
        if existing:
            self.record_institution_resolution(
                existing, created=False, origin="provisional")
            return existing

        entry = self.empty_affiliation()
        entry["_id"] = institution_id
        entry["updated"].append({"source": "minciencias", "time": int(time())})
        entry["names"].append(
            {"source": "minciencias", "lang": "es", "name": inst_aval})
        entry["types"].append({"source": "minciencias", "type": "other"})
        entry["addresses"].append(self.affiliation_address_from_group(reg))
        entry["external_ids"].append(
            {"source": "minciencias", "id": institution_id})

        created = False
        self.record_institution_resolution(
            entry, created=True, origin="provisional", committed=False)
        try:
            collection.insert_one(order_document(entry, self.empty_affiliation()))
            created = True
        except DuplicateKeyError:
            pass
        institution = collection.find_one({"_id": institution_id})
        self.record_institution_resolution(
            institution, created=created, origin="provisional")
        return institution

    def link_aval_institution(self, entry, institution, collection, name, context):
        """Apply one canonical relation policy in both ingestion modes."""
        resolved = self.institution_with_canonical_parents(institution, collection)
        resolved_ids = {item["_id"] for item in resolved}
        superseded = {self.aval_institution_id(name)} if name else set()
        for identifier in list(superseded):
            if any(relation.get("id") == identifier for relation in entry.get("relations", [])):
                if self.compound_iua_name(collection.find_one({"_id": identifier})):
                    # Compound replacement must wait for all its components.
                    superseded.discard(identifier)
        # The selected institution's ancestry proves this replacement; unrelated
        # relations, including other authoritative campuses, remain untouched.
        superseded.add(institution["_id"])
        superseded.difference_update(resolved_ids)
        entry["relations"] = [
            relation for relation in entry.get("relations", [])
            if relation.get("id") not in superseded
        ]
        for item in resolved:
            self.record_institution_resolution(item, created=False, origin="existing")
            relation = {
                "types": item.get("types", []), "id": item["_id"],
                "name": self.get_institution_name(item) or name,
            }
            if not any(value.get("id") == relation["id"]
                       for value in entry["relations"]):
                entry["relations"].append(relation)
        self.append_unique(
            entry["addresses"],
            self.affiliation_address_from_institution(institution, context),
        )
        return resolved

    def add_aval_institution_relations(self, reg, entry, collection):
        if "inst_aval" not in reg:
            return

        names = split_institution_names(reg["inst_aval"])
        raw_codes = next((reg.get(field) for field in (
            "cod_inst_aval", "COD_INST_AVAL", "cod_inst", "COD_INST"
        ) if reg.get(field) not in (None, "")), "")
        codes = split_institution_names(raw_codes)
        for index, inst_aval in enumerate(names):
            cod_inst = codes[index] if len(codes) == len(names) else None
            institution = self.get_or_create_aval_institution(
                inst_aval, collection, reg, cod_inst=cod_inst)
            if institution:
                self.link_aval_institution(
                    entry, institution, collection, inst_aval.strip(), reg)
            else:
                self.append_unique(
                    entry["addresses"],
                    self.affiliation_address_from_group(reg),
                )
        self.repair_compound_iua_relations(reg, entry, collection, reg)
        self.remove_non_institution_relations(reg, entry, collection)

    def institution_candidate_names(self, institution):
        names = []
        for item in institution.get("names", []):
            name = item.get("name")
            if name and name not in names:
                names.append(name)
        for field in ["aliases", "abbreviations"]:
            for item in institution.get(field, []):
                name = item.get("name") if isinstance(item, dict) else item
                if name and name not in names:
                    names.append(name)
        return names

    def get_institution_candidates(self, inst_aval):
        projection = {
            "names": 1,
            "aliases": 1,
            "abbreviations": 1,
            "types": 1,
            "addresses": 1,
            "external_ids": 1,
            "relations": 1,
        }
        base_query = {
            "types.type": {"$ne": "group"},
            "$or": [
                {"addresses.country": "Colombia"},
                {"addresses.country_code": "CO"},
                {"addresses.0": {"$exists": False}},
            ],
        }
        candidates = []
        seen = set()
        exact_query = deepcopy(base_query)
        exact_query["names.name"] = {
            "$regex": "^{}$".format(re.escape(inst_aval)), "$options": "i"
        }
        for candidate in self.collection.find(exact_query, projection).limit(100):
            candidates.append(candidate)
            seen.add(candidate["_id"])
        if candidates:
            return candidates
        text_projection = deepcopy(projection)
        text_projection["score"] = {"$meta": "textScore"}
        for search in ['"{}"'.format(inst_aval), inst_aval]:
            query = base_query.copy()
            query["$text"] = {"$search": search}
            cursor = self.collection.find(query, text_projection).sort(
                [("score", {"$meta": "textScore"})]).limit(100)
            for candidate in cursor:
                if candidate["_id"] not in seen:
                    candidates.append(candidate)
                    seen.add(candidate["_id"])
        return candidates

    def institution_match_score(self, institution, inst_aval):
        if self.is_non_institution_fragment(inst_aval):
            return None
        repair = getattr(self, "enriched_compound_repairs", {}).get(str(institution.get("_id")))
        if repair and self.normalize_institution_name(inst_aval) not in repair["owner_keys"]:
            return None
        best = None
        stopwords = {"de", "del", "la", "las", "los", "el", "y", "e", "en", "para", "por", "a"}
        for name in self.institution_candidate_names(institution):
            if len(split_institution_names(name)) > 1:
                continue
            name_mod = self.normalize_institution_name(name)
            compare_inst_aval = inst_aval
            inst_tokens = set(compare_inst_aval.split())

            name_tokens = set(name_mod.split())
            token_overlap = len(inst_tokens & name_tokens) / max(
                1, min(len(inst_tokens), len(name_tokens)))
            distinctive_inst_tokens = inst_tokens - stopwords
            distinctive_name_tokens = name_tokens - stopwords
            distinctive_token_overlap = len(distinctive_inst_tokens & distinctive_name_tokens) / max(
                1, min(len(distinctive_inst_tokens), len(distinctive_name_tokens)))
            scores = {
                "ratio": fuzz.ratio(name_mod, compare_inst_aval),
                "token_sort_ratio": fuzz.token_sort_ratio(name_mod, compare_inst_aval),
                "token_set_ratio": fuzz.token_set_ratio(name_mod, compare_inst_aval),
                "wratio": fuzz.WRatio(name_mod, compare_inst_aval),
            }
            score = max(scores.values())
            current = {
                "name": name,
                "normalized_name": name_mod,
                "normalized_source_name": compare_inst_aval,
                "score": score,
                "scores": scores,
                "token_overlap": token_overlap,
                "distinctive_token_overlap": distinctive_token_overlap,
                "distinctive_name_token_count": len(distinctive_name_tokens)
            }
            current_rank = (
                current["normalized_name"] == current["normalized_source_name"],
                current["scores"]["ratio"],
                current["scores"]["token_sort_ratio"],
                current["distinctive_token_overlap"],
                current["score"],
                current["distinctive_name_token_count"],
            )
            best_rank = (
                best["normalized_name"] == best["normalized_source_name"],
                best["scores"]["ratio"],
                best["scores"]["token_sort_ratio"],
                best["distinctive_token_overlap"],
                best["score"],
                best["distinctive_name_token_count"],
            ) if best else None
            if best is None or current_rank > best_rank:
                best = current
        return best

    def institution_address_score(self, institution, reg):
        if not reg:
            return 0
        source_state = self.normalize_institution_name(
            reg.get("nme_departamento_gr", ""))
        source_city = self.normalize_institution_name(
            reg.get("nme_municipio_gr", ""))
        score = 0
        for address in institution.get("addresses", []):
            state = self.normalize_institution_name(address.get("state", ""))
            city = self.normalize_institution_name(address.get("city", ""))
            current = int(bool(source_state and state == source_state)) * 2
            current += int(bool(source_city and city == source_city)) * 3
            score = max(score, current)
        return score

    def institution_candidate_rank(self, candidate, match, reg):
        has_catalog_id = any(
            ext.get("source") == "COD_INST"
            and re.fullmatch(r"\d{12}", str(ext.get("id", "")))
            for ext in candidate.get("external_ids", []))
        is_synthetic = str(candidate.get("_id", "")).startswith("IUA")
        is_root = not candidate.get("relations")
        return (
            match["normalized_name"] == match["normalized_source_name"],
            has_catalog_id,
            not is_synthetic,
            self.institution_address_score(candidate, reg),
            is_root,
            match["score"],
            match["distinctive_token_overlap"],
        )

    def find_matching_institution(self, inst_aval, reg=None, cod_inst=None):
        if (len(split_institution_names(inst_aval)) > 1
                or self.is_non_institution_fragment(inst_aval)):
            return None
        inst_aval = self.normalize_inst_aval(inst_aval)
        cod_inst = str(cod_inst or "").strip()
        cache_key = (
            cod_inst,
            inst_aval,
            self.normalize_institution_name(reg.get("nme_departamento_gr", "")) if reg else "",
            self.normalize_institution_name(reg.get("nme_municipio_gr", "")) if reg else "",
        )
        if cache_key in self.institution_match_cache:
            return self.institution_match_cache[cache_key]
        if cod_inst:
            candidates = list(self.collection.find({
                "external_ids": {"$elemMatch": {
                    "source": "COD_INST", "id": cod_inst,
                }},
                "types.type": {"$ne": "group"},
            }).limit(2))
            if len(candidates) == 1:
                self.institution_match_cache[cache_key] = candidates[0]
                return candidates[0]
        candidates = self.get_institution_candidates(inst_aval)
        matches = []
        for candidate in candidates:
            match = self.institution_match_score(candidate, inst_aval)
            if is_strict_institution_match(match):
                matches.append((
                    candidate,
                    match,
                    self.institution_candidate_rank(candidate, match, reg),
                ))
        if not matches:
            self.institution_match_cache[cache_key] = None
            return None
        matches.sort(key=lambda item: item[2], reverse=True)
        if len(matches) > 1:
            top_rank = matches[0][2]
            second_rank = matches[1][2]
            tied = top_rank == second_rank
            close_fuzzy = (
                not top_rank[0]
                and top_rank[:5] == second_rank[:5]
                and top_rank[5] - second_rank[5] < 3
            )
            if tied or close_fuzzy:
                self.institution_match_cache[cache_key] = None
                return None
        selected = self.collection.find_one({"_id": matches[0][0]["_id"]})
        if selected is None:
            selected = deepcopy(matches[0][0])
        selected.pop("score", None)
        self.institution_match_cache[cache_key] = selected
        return self.institution_match_cache[cache_key]

    def group_name(self, reg):
        if "nme_grupo_gr" in reg.keys():
            return reg["nme_grupo_gr"]
        return self.missing_group_names.get(reg["cod_grupo_gr"], "")

    def process_one(self, reg, collection, empty_affiliation, verbose):
        if "cod_grupo_gr" not in reg.keys() or not reg["cod_grupo_gr"]:
            return
        idgr = reg["cod_grupo_gr"]
        if idgr:
            db_reg = collection.find_one({"external_ids.id": idgr})
            if db_reg:
                if idgr not in self.inserted_cod_grupo:
                    self.inserted_cod_grupo.append(idgr)
                if "minciencias" not in [idx["source"] for idx in db_reg["updated"]]:
                    db_reg["updated"].append(
                        {"source": "minciencias", "time": int(time())})
                if not db_reg["year_established"]:
                    date_established = check_date_format(
                        reg["fcreacion_gr"]) if "fcreacion_gr" in reg.keys() else ""
                    if date_established:
                        db_reg["year_established"] = dt.fromtimestamp(
                            date_established).year
                if not db_reg["addresses"]:
                    if not db_reg["relations"]:
                        pass
                    else:
                        if not db_reg["relations"][0]["id"]:
                            pass
                        else:
                            aff_db = collection.find_one({"_id": db_reg["relations"][0]["id"]})
                            if aff_db:
                                self.append_unique(
                                    db_reg["addresses"],
                                    self.affiliation_address_from_institution(aff_db, reg),
                                )
                self.add_aval_institution_relations(reg, db_reg, collection)
                replace_legacy_group_name(db_reg, self.group_name(reg))
                replace_legacy_group_ranking(db_reg, reg)
                collection.update_one(
                    {"_id": db_reg["_id"]},
                    {"$set": {
                        "updated": db_reg["updated"],
                        "names": db_reg.get("names"),
                        "year_established": db_reg.get("year_established"),
                        "addresses": db_reg.get("addresses"),
                        "relations": db_reg.get("relations"),
                        "ranking": db_reg.get("ranking"),
                    }}, upsert=True)
                if verbose > 4:
                    print("Updated group {}".format(idgr))
                return

            self.inserted_cod_grupo.append(idgr)
            entry = deepcopy(empty_affiliation)
            entry["updated"].append(
                {"source": "minciencias", "time": int(time())})
            replace_legacy_group_name(entry, self.group_name(reg))
            entry["types"].append({"source": "minciencias", "type": "group"})
            year_established = ""
            date_established = check_date_format(reg["fcreacion_gr"]) if "fcreacion_gr" in reg.keys() else ""
            if date_established:
                year_established = dt.fromtimestamp(date_established).year
            entry["year_established"] = year_established
            entry["external_ids"].append(
                {"source": "minciencias", "id": reg["cod_grupo_gr"]})
            entry["subjects"].append({
                "provenance": "minciencias",
                "source": "OECD",
                "subjects": [
                    {
                        "level": 0,
                        "name": reg["nme_gran_area_gr"] if "nme_gran_area_gr" in reg.keys() else "",
                        "id": "",
                        "external_ids": [{
                            "source": "OECD",
                            "id": reg["id_area_con_gr"][0]
                            if "id_area_con_gr" in reg else "",
                        }]
                    },
                    {
                        "level": 1,
                        "name": reg["nme_area_gr"] if "nme_area_gr" in reg.keys() else "",
                        "id": "",
                        "external_ids": [{
                            "source": "OECD",
                            "id": reg["id_area_con_gr"][1]
                            if "id_area_con_gr" in reg else "",
                        }]
                    },
                ]
            })

            self.add_aval_institution_relations(reg, entry, collection)
            replace_legacy_group_ranking(entry, reg)
            # END CLASSIFICATION SECTION
            entry["_id"] = idgr
            self.collection.insert_one(order_document(entry, self.empty_affiliation()))
            if verbose > 4:
                print("Inserted group {}".format(idgr))

    def snapshot_address_context(self, reg):
        address = (reg.get("addresses") or [{}])[0]
        return {
            "nme_departamento_gr": address.get("state", ""),
            "nme_municipio_gr": address.get("city", ""),
            "nme_pais_gr": address.get("country", ""),
            "addresses": reg.get("addresses", []),
        }

    def resolve_single_institution_relation(self, reg, source_relation, entry, collection, context):
        name = str(source_relation.get("name") or "").strip()
        relation_id = str(source_relation.get("id") or "")
        evidence = {}
        institution = None
        if self.is_non_institution_fragment(name):
            evidence.update(outcome="excluded", reason="country_only_fragment")
            self.record_group_resolution(reg, source_relation, None, [], evidence)
            return [], evidence
        if not self.declared_campus_parent_name(name):
            institution = collection.find_one({
                "_id": relation_id, "types.type": {"$ne": "group"},
            }) if relation_id else None
            if not institution and relation_id:
                institution = collection.find_one({
                    "external_ids": {"$elemMatch": {
                        "source": "COD_INST", "id": relation_id,
                    }}, "types.type": {"$ne": "group"},
                })
        if self.compound_iua_name(institution):
            institution = None
            relation_id = ""
        repair = getattr(self, "enriched_compound_repairs", {}).get(
            str((institution or {}).get("_id")))
        if repair and not self.enriched_owner_declared(repair, [source_relation]):
            institution = None
            relation_id = ""
        if institution:
            evidence["outcome"] = (
                "provisional" if str(institution["_id"]).startswith("IUA") else "existing"
            )
            self.record_institution_resolution(institution, created=False, origin="existing")
        if not institution and name:
            institution = self.get_or_create_aval_institution(
                name, collection, context, cod_inst=relation_id, resolution=evidence)
        resolved = self.link_aval_institution(
            entry, institution, collection, name, context) if institution else []
        self.record_group_resolution(reg, source_relation, institution, resolved, evidence)
        return resolved, evidence

    def resolve_institution_declaration(self, reg, source_relation, entry, collection, context):
        name = str(source_relation.get("name") or "").strip()
        parts = split_institution_names(name)
        if len(parts) < 2:
            atomic = {**source_relation, "name": parts[0] if parts else name}
            return self.resolve_single_institution_relation(reg, atomic, entry, collection, context)[0]
        resolved, details = [], []
        complete = True
        for part in parts:
            # A compound declaration's id cannot identify each component.
            targets, evidence = self.resolve_single_institution_relation(
                reg, {"name": part, "id": ""}, entry, collection, context)
            complete = complete and (bool(targets) or evidence.get("outcome") == "excluded")
            details.append({"name": part, "resolved_ids": [item["_id"] for item in targets],
                            **evidence})
            for target in targets:
                if not any(item["_id"] == target["_id"] for item in resolved):
                    resolved.append(target)
        removed = []
        if complete:
            obsolete_id = self.aval_institution_id(name)
            obsolete = collection.find_one({"_id": obsolete_id})
            if obsolete is None or self.compound_iua_name(obsolete):
                for relation in entry.get("relations", []):
                    if relation.get("id") == obsolete_id:
                        removed.append(obsolete_id)
                entry["relations"] = [relation for relation in entry["relations"]
                                      if relation.get("id") != obsolete_id]
        self.record_group_resolution(reg, source_relation, None, resolved, {
            "compound": True, "outcome": "split" if complete else "unresolved",
            "parts": details, "removed_relation_ids": removed,
        })
        return resolved

    def compound_iua_name(self, institution):
        if not institution:
            return None
        identifier = str(institution.get("_id") or "")
        if not identifier.startswith("IUA_") or any(
                item.get("source") != "minciencias" for item in institution.get("external_ids", [])):
            return None
        if not any(item.get("source") == "minciencias" and item.get("type") == "other"
                   for item in institution.get("types", [])):
            return None
        if self.is_authoritative_institution(institution):
            return None
        return next((item.get("name") for item in institution.get("names", [])
                     if len(split_institution_names(item.get("name"))) > 1
                     and self.aval_institution_id(item["name"]) == identifier), None)

    def verified_enriched_compound(self, institution, original=None):
        """Prove one catalog owner without assigning its code to a compound alias."""
        identifier = str(institution.get("_id") or "")
        codes = self.affiliation_cod_inst_codes(institution)
        if (not identifier.startswith("IUA_") or len(codes) != 1
                or self.is_authoritative_institution(institution)):
            return None
        code = next(iter(codes))
        catalog = self.scienti_catalog.get(code)
        if not catalog or len(split_institution_names(catalog.get("NME_INST"))) != 1:
            return None
        owner_names = [str(catalog.get(key) or "").strip()
                       for key in ("NME_INST", "NME_INST_FILTRO", "SGL_INST")]
        owner_keys = sorted({self.normalize_institution_name(name) for name in owner_names if name})
        atomic_names = [self.normalize_institution_name(item.get("name"))
                        for item in institution.get("names", [])
                        if len(split_institution_names(item.get("name"))) == 1]
        if not atomic_names or any(name not in owner_keys for name in atomic_names):
            return None
        if original and original.get("registry_external_ids") != institution.get("external_ids", []):
            return None
        expected = {
            "COD_INST": code,
            "ID_INSTITUCION": str(catalog.get("ID_INSTITUCION") or ""),
            "nit": normalize_scienti_nit(catalog.get("TXT_NIT")),
        }
        for external in institution.get("external_ids", []):
            source, value = external.get("source"), str(external.get("id") or "")
            if source == "minciencias" and value == identifier:
                continue
            if source not in expected or not expected[source]:
                return None
            if source == "nit":
                value = normalize_scienti_nit(value)
            if value != expected[source]:
                return None
        if self.collection.count_documents({
            "_id": {"$ne": institution["_id"]},
            "external_ids": {"$elemMatch": {"source": "COD_INST", "id": code}},
        }):
            return None
        fields = (original or {}).get("original_fields") or {
            field: deepcopy(institution[field])
            for field in ("names", "aliases", "abbreviations") if field in institution
        }
        compounds = [item.get("name") for item in fields.get("names", [])
                     if len(split_institution_names(item.get("name"))) > 1]
        if not compounds or any(self.aval_institution_id(name) != identifier for name in compounds):
            return None
        cleaned = {}
        for field in fields:
            # Preserve every unrelated label and every base-schema key.
            cleaned[field] = [deepcopy(item) for item in institution.get(field, [])
                              if (item.get("name") if isinstance(item, dict) else item) not in compounds]
        if not cleaned.get("names"):
            return None
        return {
            "institution_id": institution["_id"], "cod_inst": code,
            "owner_name": catalog["NME_INST"], "owner_keys": owner_keys,
            "compound_names": compounds, "original_fields": fields,
            "registry_external_ids": deepcopy(institution.get("external_ids", [])),
            "cleaned_fields": cleaned,
        }

    def prepare_enriched_compound_repairs(self):
        """Persist evidence before changing labels; recover it across runs/retries."""
        controls = self.resolution_audits
        saved = {item["institution_id"]: item for item in controls.find({
            "kind": "enriched_compound_repair",
        })}
        candidates = {item["_id"]: item for item in self.collection.find({
            "_id": {"$regex": "^IUA_"}, "names.name": {"$regex": "[;|]"},
            "external_ids": {"$elemMatch": {"source": {"$ne": "minciencias"}}},
        })}
        for identifier in saved:
            institution = self.collection.find_one({"_id": identifier})
            if institution:
                candidates[identifier] = institution
        self.enriched_compound_repairs = {}
        plans = []
        for identifier, institution in candidates.items():
            previous = saved.get(identifier)
            plan = self.verified_enriched_compound(institution, previous)
            if not plan:
                referenced_codes = set()
                for group in self.collection.find({"relations.id": identifier}, {"external_ids": 1}):
                    referenced_codes.update(str(item.get("id")) for item in group.get("external_ids", [])
                                            if re.fullmatch(r"COL\d{7}", str(item.get("id") or "")))
                if referenced_codes and self.openadata_collection.count_documents({
                        "_id": {"$in": sorted(referenced_codes)}}):
                    raise RuntimeError("unverified compound institution with registry identifiers requires review: " + identifier)
                continue
            references = (previous or {}).get("original_group_ids")
            if references is None:
                references = [str(item["_id"]) for item in self.collection.find(
                    {"relations.id": identifier}, {"_id": 1})]
            plan["original_group_ids"] = references
            plans.append((identifier, institution, plan))
        for identifier, institution, plan in plans:
            query = {"_id": "enriched_compound:" + identifier}
            controls.update_one(query, {"$setOnInsert": {
                "kind": "enriched_compound_repair", **deepcopy(plan),
                "created_at": int(time()), "committed": False,
            }, "$set": {"run_id": self.run_id}}, upsert=True)
            if plan["cleaned_fields"] != {key: institution.get(key) for key in plan["cleaned_fields"]}:
                guard = {"_id": identifier, "external_ids": institution.get("external_ids", [])}
                guard.update({field: institution.get(field) for field in plan["cleaned_fields"]})
                updated = self.collection.update_one(guard, {"$set": plan["cleaned_fields"]})
                if updated.matched_count != 1:
                    raise RuntimeError("enriched compound institution changed during repair")
            controls.update_one(query, {"$set": {"committed": True}})
            self.enriched_compound_repairs[identifier] = plan
        self.institution_match_cache.clear()

    def enriched_owner_declared(self, repair, declarations):
        for declaration in declarations:
            if str(declaration.get("id") or "") == repair["cod_inst"]:
                return True
            if any(self.normalize_institution_name(part) in repair["owner_keys"]
                   for part in split_institution_names(declaration.get("name"))):
                return True
        return False

    def repair_enriched_group_relations(self, reg, entry, collection):
        """Keep verified identities only for groups that declare their owner."""
        for identifier, repair in getattr(self, "enriched_compound_repairs", {}).items():
            group_id = str(entry["_id"])
            linked = any(item.get("id") == identifier for item in entry["relations"])
            if not linked and group_id not in repair["original_group_ids"]:
                continue
            declared = self.enriched_owner_declared(repair, reg.get("relations", []))
            institution = collection.find_one({"_id": identifier})
            if declared:
                for relation in entry["relations"]:
                    if relation.get("id") == identifier:
                        relation["name"] = repair["owner_name"]
                        relation["types"] = deepcopy(institution.get("types", []))
            else:
                entry["relations"] = [item for item in entry["relations"] if item.get("id") != identifier]
            self.resolution_audits.replace_one({
                "_id": f"{self.run_id}:enriched_group:{group_id}:{identifier}",
            }, {
                "run_id": self.run_id, "kind": "enriched_compound_group",
                "group_id": group_id, "institution_id": identifier,
                "cod_inst": repair["cod_inst"], "owner_name": repair["owner_name"],
                "owner_declared": declared, "outcome": "kept" if declared else "removed",
                "source_declarations": deepcopy(reg.get("relations", [])),
            }, upsert=True)

    def repair_compound_iua_relations(self, reg, entry, collection, context):
        """Replace proven provisional compound links; leave entity documents intact."""
        for relation in list(entry.get("relations", [])):
            identifier = str(relation.get("id") or "")
            if not identifier.startswith("IUA_"):
                continue
            compound = self.compound_iua_name(collection.find_one({"_id": identifier}))
            if compound:
                self.resolve_institution_declaration(
                    reg, {"name": compound, "id": identifier}, entry, collection, context)

    def remove_non_institution_relations(self, reg, entry, collection):
        """Remove only proven country fallbacks; keep entity documents as evidence."""
        for relation in list(entry.get("relations", [])):
            if not self.is_non_institution_fragment(relation.get("name")):
                continue
            identifier = relation.get("id")
            institution = collection.find_one({"_id": identifier}) or {}
            if (identifier != self.aval_institution_id(relation["name"])
                    or not institution
                    or self.is_authoritative_institution(institution)
                    or any(item.get("source") != "minciencias"
                           for item in institution.get("external_ids", []))):
                raise RuntimeError("country-only relation has unverified institutional identity")
            entry["relations"] = [item for item in entry["relations"] if item.get("id") != identifier]
            self.record_group_resolution(reg, relation, None, [], {
                "outcome": "excluded", "reason": "country_only_fragment",
                "removed_relation_id": identifier,
            })

    def resolve_snapshot_relations(self, reg, entry, collection, non_endorsing=()):
        context = self.snapshot_address_context(reg)
        endorsed_ids = set()
        for source_relation in reg.get("relations", []) or []:
            resolved = self.resolve_institution_declaration(
                reg, source_relation, entry, collection, context)
            endorsed_ids.update(item["_id"] for item in resolved or [])
        self.repair_compound_iua_relations(reg, entry, collection, context)
        self.repair_enriched_group_relations(reg, entry, collection)
        self.remove_non_institution_relations(reg, entry, collection)
        self.remove_non_endorsing_relations(reg, entry, non_endorsing, endorsed_ids)
        if any(len(split_institution_names(item.get("name"))) > 1
               for item in entry["relations"]):
            raise RuntimeError("unresolved institution relations: unverified compound institution relation requires review")

    def existing_institution_ids(self, name, reg=None):
        """Read-only identities an institution declaration refers to; never creates."""
        name = strip_endorsement_marker(name)
        name = self.declared_campus_parent_name(name) or name
        if not name or self.is_non_institution_fragment(name):
            return set()
        identifiers = set()
        source = self.resolve_scienti_catalog_record(name, reg=reg)
        code = str(self.scienti_value(source, "COD_INST") or "") if source else ""
        if code:
            for item in self.collection.find({
                "external_ids": {"$elemMatch": {"source": "COD_INST", "id": code}},
                "types.type": {"$ne": "group"},
            }, {"_id": 1}):
                identifiers.add(item["_id"])
        match = self.find_matching_institution(name, reg)
        if match:
            identifiers.add(match["_id"])
        provisional = self.collection.find_one({"_id": self.aval_institution_id(name)}, {"_id": 1})
        if provisional:
            identifiers.add(provisional["_id"])
        return identifiers

    def remove_non_endorsing_relations(self, reg, entry, declarations, endorsed_ids):
        """Drop prior links (e.g. from ScienTI) to institutions declared non-endorsing.

        Institutions also resolved from an endorsing declaration of the same
        group, including their canonical parents, are kept.
        """
        for declaration in declarations or []:
            matched = self.existing_institution_ids(declaration.get("name"), reg)
            removable = matched - set(endorsed_ids)
            removed = [relation.get("id") for relation in entry.get("relations", [])
                       if relation.get("id") in removable]
            if removed:
                entry["relations"] = [relation for relation in entry["relations"]
                                      if relation.get("id") not in removable]
            self.record_group_resolution(reg, declaration, None, [], {
                "outcome": "excluded", "reason": "non_endorsing_institution",
                "matched_ids": sorted(str(item) for item in matched),
                "removed_relation_ids": removed,
            })

    def process_snapshot_one(self, reg, collection, verbose):
        group_code = str(reg.get("_id") or "")
        if not re.fullmatch(r"COL\d{7}", group_code):
            raise RuntimeError("affiliation snapshot contains an invalid group code")
        if set(reg) != {"_id"} | SNAPSHOT_FIELDS:
            raise RuntimeError("affiliation snapshot does not follow the Kahi schema")
        reg, non_endorsing = split_endorsement_declarations(reg)
        entry = collection.find_one({"external_ids.id": group_code})
        if not entry:
            entry = collection.find_one({"_id": group_code})
        action = "updated" if entry else "inserted"
        if not entry:
            entry = self.empty_affiliation()
            entry["_id"] = group_code
        entry.setdefault("relations", [])
        for field in SNAPSHOT_LIST_FIELDS:
            entry.setdefault(field, [])
            for value in reg.get(field, []) or []:
                self.append_unique(entry[field], deepcopy(value))
        if entry.get("year_established") in (None, ""):
            entry["year_established"] = reg.get("year_established")
        self.resolve_snapshot_relations(reg, entry, collection, non_endorsing)
        collection.replace_one(
            {"_id": entry["_id"]},
            order_document(entry, self.empty_affiliation()),
            upsert=True,
        )
        if verbose > 4:
            print("{} group {} from audited snapshot".format(action, group_code))
        return action

    def _process_snapshot_batch(self, batch, counters, progress=None):
        results = Parallel(
            n_jobs=self.n_jobs,
            verbose=0,
            backend="threading",
        )(
            delayed(self.process_snapshot_one)(reg, self.collection, self.verbose)
            for reg in batch
        )
        for action in ("inserted", "updated"):
            counters[action] += results.count(action)
        if progress is not None:
            progress.step("nuevos", n=results.count("inserted"))
            progress.step("actualizados", n=results.count("updated"))
            progress.step("otros", n=len(results) - results.count("inserted") - results.count("updated"))

    def process_snapshot(self, previous):
        counters = deepcopy(previous.get("counters") or {
            "processed": 0, "inserted": 0, "updated": 0,
        })
        last_id = previous.get("last_id")
        query = {"_id": {"$gt": last_id}} if last_id is not None else {}
        cursor = self.openadata_collection.find(query).sort("_id", 1).batch_size(
            self.batch_size
        )
        progress = ProgressLogger(
            "minciencias_opendata_affiliations",
            total=self.snapshot_documents,
            initial=counters["processed"],
        ).start("batch_size={} num_jobs={}".format(self.batch_size, self.n_jobs))
        batch = []
        for reg in cursor:
            batch.append(reg)
            if len(batch) < self.batch_size:
                continue
            self._process_snapshot_batch(batch, counters, progress)
            counters["processed"] += len(batch)
            last_id = batch[-1]["_id"]
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {"last_id": last_id, "counters": deepcopy(counters)}},
            )
            batch.clear()
        if batch:
            self._process_snapshot_batch(batch, counters, progress)
            counters["processed"] += len(batch)
            last_id = batch[-1]["_id"]
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {"last_id": last_id, "counters": deepcopy(counters)}},
            )
        progress.done()
        if counters["processed"] != self.snapshot_documents:
            raise RuntimeError("affiliation snapshot processing count mismatch")
        return counters

    def _run_snapshot(self):
        previous = self.runs.find_one({"_id": self.run_id}) or {}
        identity = {
            "contract_version": CONTRACT_VERSION,
            "release": self.snapshot_release_name,
            "audit": self.snapshot_audit_name,
            "materialization_run": self.snapshot_materialization_run,
            "source_collection": self.snapshot_collection_name,
            "source_documents": self.snapshot_documents,
            "institution_resolution_version": INSTITUTION_RESOLUTION_VERSION,
            "import_revision": self.import_revision,
            "implementation_sha256": self.implementation_sha256,
            "publication_kind": self.snapshot_publication_kind,
            "catalog_fingerprint": deepcopy(self.catalog_fingerprint),
        }
        if previous and any(
            previous.get(key) != value for key, value in identity.items()
        ):
            raise RuntimeError(
                "affiliation import run exists with different source evidence"
            )
        if previous.get("status") == "complete":
            print("[minciencias_opendata_affiliations] SKIP snapshot ya importado (run {})".format(
                self.run_id), flush=True)
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
            self.prepare_enriched_compound_repairs()
            counters = self.process_snapshot(previous)
            resolution_summary = self.institution_resolution_summary()
            if (resolution_summary["relations_unresolved"]
                    or resolution_summary["missing_relation_targets"]
                    or resolution_summary["pending_institutions"]
                    or resolution_summary["compound_declarations_unresolved"]):
                raise RuntimeError("affiliation import has unresolved institution relations")
            summary = {**identity, **counters, "institution_resolution": resolution_summary}
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

    def process_openadata(self):
        # Pipeline to find duplicate documents and keep the one with the highest edad_anos_gr in each group
        pipeline = [
            {
                "$sort": {"ano_convo": -1}  # Sort documents by edad_anos_gr in descending order
            },
            {
                "$group": {
                    "_id": "$cod_grupo_gr",  # Group documents by the group code
                    "doc": {"$first": "$$ROOT"}  # Select the first document of each group
                }
            },
            {
                "$replaceRoot": {"newRoot": "$doc"}  # Replace the root of the document with the selected documents
            }
        ]
        affiliation_cursor = self.openadata_collection.aggregate(
            pipeline, allowDiskUse=True)
        progress = ProgressLogger("minciencias_opendata_affiliations:legacy").start(
            "num_jobs={}".format(self.n_jobs))

        def tracked(aff, collection, empty_affiliation, verbose):
            self.process_one(aff, collection, empty_affiliation, verbose)
            progress.step()

        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            collection = db["affiliations"]

            Parallel(
                n_jobs=self.n_jobs,
                verbose=0,
                backend="threading")(
                delayed(tracked)(
                    aff,
                    collection,
                    self.empty_affiliation(),
                    self.verbose,
                ) for aff in affiliation_cursor
            )
        progress.done()

    def run(self):
        try:
            result = (
                self._run_snapshot()
                if self.source_mode == "snapshot"
                else self.process_openadata()
            )
            return result if self.source_mode == "snapshot" else 0
        finally:
            self.client.close()
            self.openadata_client.close()
