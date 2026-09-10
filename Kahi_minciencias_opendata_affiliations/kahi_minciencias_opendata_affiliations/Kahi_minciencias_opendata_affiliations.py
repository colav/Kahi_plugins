from kahi_impactu_utils.Utils import check_date_format
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from pymongo.errors import DuplicateKeyError
from joblib import Parallel, delayed
from datetime import datetime as dt
from kahi.KahiBase import KahiBase
from unidecode import unidecode
from thefuzz import fuzz
from copy import deepcopy
import hashlib
from time import time
import re


CONTRACT_VERSION = "scienti-kahi-ingestion-v1"
SNAPSHOT_ENTITIES = {
    "works", "projects", "patents", "events", "persons", "affiliations"
}
SNAPSHOT_LIST_FIELDS = (
    "updated", "names", "aliases", "abbreviations", "types", "status",
    "addresses", "external_urls", "external_ids", "subjects", "ranking",
    "description",
)
SNAPSHOT_FIELDS = set(SNAPSHOT_LIST_FIELDS) | {"year_established", "relations"}


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


class Kahi_minciencias_opendata_affiliations(KahiBase):

    config = {}
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
        if self.source_mode not in {"snapshot", "legacy_open_data"}:
            raise ValueError(
                "minciencias_opendata_affiliations.source_mode must explicitly be "
                "'snapshot' or 'legacy_open_data'"
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
            self.run_id = "affiliations:{}".format(self.snapshot_release_name)

        self.inserted_cod_grupo = []
        self.institution_match_cache = {}

        for reg in self.collection.find({"types.type": "group"}):
            for ext in reg["external_ids"]:
                if ext["source"] == "minciencias":
                    self.inserted_cod_grupo.append(ext["id"])

    def _configure_legacy_source(self, source_config):
        collection_name = source_config.get("collection_name")
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
            raise ValueError("snapshot mode requires an explicit immutable release_name")
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
        self.snapshot_audit_name = audit["_id"]
        self.snapshot_materialization_run = materialization_runs["affiliations"]
        self.snapshot_documents = actual
        self.snapshot_collection_name = collection_name
        self.openadata_collection = self.openadata_db[collection_name]

    def rename_institution(self, name):
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
            return name.split("sede")[0].strip()  # Keep only the first part of the name if it contains "sede"
        elif name == "universidad militar nueva granada":
            return "nueva granada"
        elif "pamplona" in name:
            return "pamplona"
        elif "sucre" in name:
            return "sucre"
        elif "santo tomás" in name or "santo tomas" in name:
            return "santo tomas"
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

    def normalize_inst_aval(self, name):
        name = name.lower().strip()
        name = self.rename_institution(name)
        return self.normalize_institution_name(name)

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
        digest = hashlib.sha1(inst_aval.encode("utf-8")).hexdigest()[:6]
        return "IUA{}".format(digest)

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

    def get_or_create_aval_institution(self, inst_aval, collection, reg):
        inst_aval = inst_aval.strip()
        if not inst_aval:
            return None

        institution = self.find_matching_institution(inst_aval, reg=reg)
        if institution:
            return institution

        institution_id = self.aval_institution_id(inst_aval)
        existing = collection.find_one({"_id": institution_id})
        if existing:
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

        try:
            collection.insert_one(entry)
        except DuplicateKeyError:
            pass
        return collection.find_one({"_id": institution_id})

    def add_aval_institution_relations(self, reg, entry, collection):
        if "inst_aval" not in reg:
            return

        for inst_aval in reg["inst_aval"].split("|"):
            institution = self.get_or_create_aval_institution(
                inst_aval, collection, reg)
            if institution:
                synthetic_id = self.aval_institution_id(inst_aval.strip())
                if institution["_id"] != synthetic_id:
                    entry["relations"] = [
                        relation for relation in entry["relations"]
                        if relation.get("id") != synthetic_id
                    ]
                relation = {
                    "types": institution.get("types", []),
                    "id": institution["_id"],
                    "name": self.get_institution_name(institution)
                }
                if not any(rel.get("id") == relation["id"] for rel in entry["relations"]):
                    entry["relations"].append(relation)
                self.append_unique(
                    entry["addresses"],
                    self.affiliation_address_from_institution(institution, reg),
                )
            else:
                self.append_unique(
                    entry["addresses"],
                    self.affiliation_address_from_group(reg),
                )

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
        best = None
        stopwords = {"de", "del", "la", "las", "los", "el", "y", "e", "en", "para", "por", "a"}
        for name in self.institution_candidate_names(institution):
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
            if best is None or current["score"] > best["score"]:
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
            ext.get("source") == "minciencias"
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

    def find_matching_institution(self, inst_aval, reg=None):
        inst_aval = self.normalize_inst_aval(inst_aval)
        cache_key = (
            inst_aval,
            self.normalize_institution_name(reg.get("nme_departamento_gr", "")) if reg else "",
            self.normalize_institution_name(reg.get("nme_municipio_gr", "")) if reg else "",
        )
        if cache_key in self.institution_match_cache:
            return self.institution_match_cache[cache_key]
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
        self.institution_match_cache[cache_key] = matches[0][0]
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
                        {"time": int(time()), "source": "minciencias"})
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
                collection.update_one(
                    {"_id": db_reg["_id"]},
                    {"$set": {
                        "updated": db_reg["updated"],
                        "year_established": db_reg.get("year_established"),
                        "addresses": db_reg.get("addresses"),
                        "relations": db_reg.get("relations")
                    }}, upsert=True)
                if verbose > 4:
                    print("Updated group {}".format(idgr))
                return

            self.inserted_cod_grupo.append(idgr)
            entry = deepcopy(empty_affiliation)
            entry["updated"].append(
                {"source": "minciencias", "time": int(time())})
            entry["names"].append(
                {"source": "minciencias", "lang": "es", "name": self.group_name(reg)})
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
            entry_rank = {
                "source": "minciencias",
                "rank": reg["nme_clasificacion_gr"] if "nme_clasificacion_gr" in reg.keys() else "",
                "order": reg["orden_clas_gr"] if "orden_clas_gr" in reg.keys() else "",
                "date": check_date_format(reg["ano_convo"] if "ano_convo" in reg.keys() else ""),
            }
            entry["ranking"].append(entry_rank)
            # END CLASSIFICATION SECTION
            entry["_id"] = idgr
            self.collection.insert_one(entry)
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

    def resolve_snapshot_relations(self, reg, entry, collection):
        context = self.snapshot_address_context(reg)
        for source_relation in reg.get("relations", []) or []:
            name = str(source_relation.get("name") or "").strip()
            relation_id = str(source_relation.get("id") or "")
            institution = collection.find_one({"_id": relation_id}) if relation_id else None
            if not institution and name:
                institution = self.get_or_create_aval_institution(
                    name, collection, context
                )
            if not institution:
                continue
            synthetic_id = self.aval_institution_id(name) if name else ""
            if synthetic_id and institution["_id"] != synthetic_id:
                entry["relations"] = [
                    relation for relation in entry["relations"]
                    if relation.get("id") != synthetic_id
                ]
            relation = {
                "types": institution.get("types", []),
                "id": institution["_id"],
                "name": self.get_institution_name(institution) or name,
            }
            if not any(
                value.get("id") == relation["id"]
                for value in entry["relations"]
            ):
                entry["relations"].append(relation)
            self.append_unique(
                entry["addresses"],
                self.affiliation_address_from_institution(institution, context),
            )

    def process_snapshot_one(self, reg, collection, verbose):
        group_code = str(reg.get("_id") or "")
        if not re.fullmatch(r"COL\d{7}", group_code):
            raise RuntimeError("affiliation snapshot contains an invalid group code")
        if set(reg) != {"_id"} | SNAPSHOT_FIELDS:
            raise RuntimeError("affiliation snapshot does not follow the Kahi schema")
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
        self.resolve_snapshot_relations(reg, entry, collection)
        payload = {
            field: entry[field]
            for field in SNAPSHOT_LIST_FIELDS
        }
        payload["relations"] = entry["relations"]
        payload["year_established"] = entry.get("year_established")
        collection.update_one(
            {"_id": entry["_id"]},
            {
                "$set": payload,
                "$setOnInsert": {"citation_count": [], "products_count": 0},
            },
            upsert=True,
        )
        if verbose > 4:
            print("{} group {} from audited snapshot".format(action, group_code))
        return action

    def _process_snapshot_batch(self, batch, counters):
        results = Parallel(
            n_jobs=self.n_jobs,
            verbose=self.verbose,
            backend="threading",
        )(
            delayed(self.process_snapshot_one)(reg, self.collection, self.verbose)
            for reg in batch
        )
        for action in ("inserted", "updated"):
            counters[action] += results.count(action)

    def process_snapshot(self, previous):
        counters = deepcopy(previous.get("counters") or {
            "processed": 0, "inserted": 0, "updated": 0,
        })
        last_id = previous.get("last_id")
        query = {"_id": {"$gt": last_id}} if last_id is not None else {}
        cursor = self.openadata_collection.find(query).sort("_id", 1).batch_size(
            self.batch_size
        )
        batch = []
        for reg in cursor:
            batch.append(reg)
            if len(batch) < self.batch_size:
                continue
            self._process_snapshot_batch(batch, counters)
            counters["processed"] += len(batch)
            last_id = batch[-1]["_id"]
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {"last_id": last_id, "counters": deepcopy(counters)}},
            )
            batch.clear()
        if batch:
            self._process_snapshot_batch(batch, counters)
            counters["processed"] += len(batch)
            last_id = batch[-1]["_id"]
            self.runs.update_one(
                {"_id": self.run_id},
                {"$set": {"last_id": last_id, "counters": deepcopy(counters)}},
            )
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
        }
        if previous and any(
            previous.get(key) != value for key, value in identity.items()
        ):
            raise RuntimeError(
                "affiliation import run exists with different source evidence"
            )
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
        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            collection = db["affiliations"]

            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(self.process_one)(
                    aff,
                    collection,
                    self.empty_affiliation(),
                    self.verbose,
                ) for aff in affiliation_cursor
            )
            client.close()

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
