from kahi.KahiBase import KahiBase
from kahi.progress import ProgressLogger, format_duration
from copy import deepcopy
from datetime import datetime
from pymongo import MongoClient, ASCENDING, TEXT
from pymongo.errors import DuplicateKeyError
import re
from time import time, monotonic
from thefuzz import fuzz
from unicodedata import normalize as unicode_normalize
from urllib.parse import urlparse
from kahi_impactu_utils.Utils import check_date_format
from kahi_impactu_utils.Mongo import order_document


SOURCE = "scienti"
MINCIENCIAS_SOURCE = "minciencias"
COD_INST_SOURCE = "COD_INST"
ID_INSTITUCION_SOURCE = "ID_INSTITUCION"
INVALID_TEXT_VALUES = {
    "",
    "-",
    "n/a",
    "na",
    "n.a.",
    "nd",
    "ne",
    "no",
    "no aplica",
    "no tiene",
    "none",
    "null",
    "pendiente",
    "prueba",
}
INVALID_NITS = {
    "000000000",
    "000000010",
    "111111111",
    "123456789",
    "999999999",
}
GENERIC_INSTITUTION_TOKENS = {
    "asociacion", "centro", "colegio", "corporacion", "educacion", "escuela",
    "fundacion", "institucion", "instituto", "superior", "universidad",
    "universitaria", "universitario",
}
NAME_STOPWORDS = {
    "a", "de", "del", "e", "el", "en", "la", "las", "los", "para", "por", "y",
}


def clean_text(value):
    """Return source text without accepting known placeholder values."""
    if value is None:
        return ""
    value = str(value).strip()
    return "" if value.lower() in INVALID_TEXT_VALUES else value


def normalize_institution_name(value):
    """Normalize an institution name for matching without changing stored text."""
    value = unicode_normalize("NFKD", clean_text(value)).encode(
        "ascii", "ignore").decode("ascii").lower()
    value = re.sub(r"[^\w\s]", " ", value)
    return " ".join(value.split())


def distinctive_institution_tokens(value):
    """Exclude legal-form words that cannot identify an institution alone."""
    return set(normalize_institution_name(value).split()) - (
        GENERIC_INSTITUTION_TOKENS | NAME_STOPWORDS)


def append_unique(items, item):
    if item and item not in items:
        items.append(item)


def append_unique_external_id(items, item):
    """Deduplicate identifiers by source and value, regardless of provenance."""
    if not item.get("id"):
        return
    if not any(
            ext.get("source") == item["source"] and ext.get("id") == item["id"]
            for ext in items):
        items.append(item)


def first_dict(value):
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return value[0]
    return {}


def source_country_code(value):
    """Normalize only country codes whose meaning is explicit in the source."""
    value = clean_text(value).upper()
    if value in {"1", "COL", "CO"}:
        return "CO"
    return value if len(value) == 2 and value.isalpha() else ""


def scienti_country(reg):
    """Return the best supported country name/code pair from an institution."""
    direct = first_dict(reg.get("country"))
    city = first_dict(reg.get("city"))
    city_department = first_dict(city.get("department"))
    nested = first_dict(city_department.get("country"))
    raw_country = clean_text(reg.get("SGL_PAIS")).upper()

    for country in [direct, nested]:
        nested_raw = clean_text(country.get("SGL_PAIS")).upper()
        if raw_country and not raw_country.isdigit() and nested_raw and raw_country != nested_raw:
            continue
        name = clean_text(country.get("TXT_NME_PAIS"))
        code = clean_text(country.get("SGL_PAIS_ISO_2")).upper()
        if name or code:
            return name, code

    if source_country_code(raw_country) == "CO":
        return "Colombia", "CO"
    return "", source_country_code(raw_country)


def scienti_address(reg):
    """Build a standard address, rejecting known cross-country join conflicts."""
    country, country_code = scienti_country(reg)
    raw_country = clean_text(reg.get("SGL_PAIS")).upper()
    raw_department = clean_text(reg.get("SGL_DEPARTAMENTO")).upper()
    department = first_dict(reg.get("department"))
    city_reg = first_dict(reg.get("city"))

    state = clean_text(department.get("TXT_NME_DEPARTAMENTO"))
    department_country = clean_text(department.get("SGL_PAIS")).upper()
    if state and raw_country and not raw_country.isdigit() and department_country not in {"", raw_country}:
        state = ""

    city = clean_text(reg.get("TXT_CIUDAD_INST"))
    if not city and city_reg:
        city_country = clean_text(city_reg.get("SGL_PAIS")).upper()
        city_department_code = clean_text(city_reg.get("SGL_DEPARTAMENTO")).upper()
        country_matches = not raw_country or raw_country.isdigit() or not city_country or (
            source_country_code(raw_country) == source_country_code(city_country)
            if source_country_code(raw_country) and source_country_code(city_country)
            else raw_country == city_country
        )
        department_matches = (
            not raw_department
            or raw_department.isdigit()
            or not city_department_code
            or raw_department == city_department_code
        )
        if country_matches and department_matches:
            city = clean_text(city_reg.get("TXT_NME_MUNICIPIO"))
            if not state:
                city_department = first_dict(city_reg.get("department"))
                state = clean_text(city_department.get("TXT_NME_DEPARTAMENTO"))

    if not any([state, city, country, country_code]):
        return None
    return {
        "lat": "",
        "lng": "",
        "postcode": "",
        "state": state,
        "city": city,
        "country": country,
        "country_code": country_code,
    }


def normalize_nit(reg):
    """Return a plausible Colombian NIT without its verification digit."""
    _, country_code = scienti_country(reg)
    if country_code and country_code != "CO":
        return ""
    raw = clean_text(reg.get("TXT_NIT")).split("-", maxsplit=1)[0]
    digits = "".join(char for char in raw if char.isdigit())
    if not 7 <= len(digits) <= 10:
        return ""
    if digits in INVALID_NITS or len(set(digits)) == 1:
        return ""
    return digits


def normalize_url(value):
    """Return a usable public URL, adding a scheme when necessary."""
    value = clean_text(value)
    if not value or " " in value or "@" in value:
        return ""
    if not value.startswith(("http://", "https://")):
        value = "https://" + value
    parsed = urlparse(value)
    if not parsed.hostname or "." not in parsed.hostname:
        return ""
    if parsed.hostname.lower() in {"example.com", "www.example.com"}:
        return ""
    return value


def parse_scienti_datetime(value):
    if isinstance(value, datetime):
        return value
    value = clean_text(value)
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def scienti_year_established(reg):
    date = parse_scienti_datetime(reg.get("DTA_CONSTITUCION"))
    if date and 1500 <= date.year <= datetime.now().year:
        return date.year
    return None


def scienti_group_year_established(reg):
    """Read the formation year without requiring or inventing a month/day."""
    value = clean_text(reg.get("ANO_FORMACAO"))
    if not re.fullmatch(r"[0-9]{4}", value):
        return None
    year = int(value)
    return year if 1500 <= year <= datetime.now().year else None


def scienti_updated_time(reg):
    date = parse_scienti_datetime(reg.get("DTA_ACTUALIZACION"))
    return int(date.timestamp()) if date else int(time())


def scienti_external_ids(reg, include_snies_id=True):
    """Map stable identifiers into the existing affiliation schema."""
    identifiers = []
    cod_inst = clean_text(reg.get("COD_INST"))
    if cod_inst and cod_inst != "000000000000":
        identifiers.append({
            "provenance": SOURCE,
            "source": COD_INST_SOURCE,
            "id": cod_inst,
        })
    internal_id = clean_text(reg.get("ID_INSTITUCION"))
    if internal_id:
        identifiers.append({
            "provenance": SOURCE,
            "source": ID_INSTITUCION_SOURCE,
            "id": internal_id,
        })
    nit = normalize_nit(reg)
    if nit:
        identifiers.append({
            "provenance": SOURCE,
            "source": "nit",
            "id": nit,
        })
    cod_ies = clean_text(reg.get("COD_IES"))
    if include_snies_id and cod_ies and cod_ies.isdigit():
        identifiers.append({
            "provenance": SOURCE,
            "source": "snies",
            "id": cod_ies,
        })
    return identifiers


def mark_scienti_updated(entry, reg):
    updated_time = scienti_updated_time(reg)
    for item in entry["updated"]:
        if item.get("source") == SOURCE:
            item["time"] = max(item.get("time", 0), updated_time)
            return
    entry["updated"].append({"source": SOURCE, "time": updated_time})


def merge_scienti_address(entry, address):
    """Fill missing address values without replacing better upstream geography."""
    if not address:
        return
    if not entry["addresses"]:
        entry["addresses"].append(address)
        return
    current = entry["addresses"][0]
    current_country = clean_text(current.get("country_code")) or clean_text(
        current.get("country"))
    source_country = clean_text(address.get("country_code")) or clean_text(
        address.get("country"))
    if current_country and source_country and current_country.lower() != source_country.lower():
        return
    for field in ["state", "city", "country", "country_code"]:
        if not current.get(field) and address.get(field):
            current[field] = address[field]


def apply_scienti_institution_data(entry, reg, include_snies_id=True):
    """Enrich an affiliation using only fields in the current entity schema."""
    for field in [
            "updated", "names", "aliases", "abbreviations", "types", "status",
            "relations", "addresses", "external_urls", "external_ids", "subjects",
            "ranking", "description", "citation_count"]:
        entry.setdefault(field, [])

    name = clean_text(reg.get("NME_INST"))
    if name:
        append_unique(entry["names"], {
            "source": SOURCE, "name": name, "lang": "es"})

    abbreviation = clean_text(reg.get("SGL_INST"))
    if abbreviation and normalize_institution_name(abbreviation) != normalize_institution_name(name):
        append_unique(entry["abbreviations"], abbreviation)

    cod_ies = clean_text(reg.get("COD_IES"))
    if not include_snies_id and cod_ies:
        entry["external_ids"] = [
            item for item in entry["external_ids"]
            if not (
                item.get("source") == "snies"
                and str(item.get("id")) == cod_ies
                and item.get("provenance") == SOURCE
            )
        ]
    for external_id in scienti_external_ids(
            reg, include_snies_id=include_snies_id):
        append_unique_external_id(entry["external_ids"], external_id)

    inferred_type = "education" if clean_text(reg.get("COD_IES")) else "other"
    if inferred_type == "education" or not entry["types"]:
        append_unique(entry["types"], {
            "provenance": SOURCE, "source": SOURCE, "type": inferred_type})

    year = scienti_year_established(reg)
    if year and entry.get("year_established") in {None, "", -1}:
        entry["year_established"] = year

    merge_scienti_address(entry, scienti_address(reg))

    website = normalize_url(reg.get("URL_HOME_PAGE"))
    if website:
        append_unique(entry["external_urls"], {
            "provenance": SOURCE, "source": "site", "url": website})

    mark_scienti_updated(entry, reg)
    return entry


def normalize_scienti_rank(value):
    """Translate the Scienti code for recognized groups."""
    return "Reconocido" if value == "00" else value


def inherit_parent_addresses(entry, parents):
    """Copy unique parent addresses when the affiliation has none."""
    if entry.get("addresses"):
        return False
    for parent in parents:
        for address in parent.get("addresses", []):
            if address not in entry["addresses"]:
                entry["addresses"].append(deepcopy(address))
    return bool(entry["addresses"])


def institution_record_score(reg):
    """Prefer the most complete copy when catalogs overlap."""
    fields = [
        "NME_INST", "SGL_INST", "TXT_NIT", "TXT_DIGITO_VERIFICADOR",
        "COD_IES", "URL_HOME_PAGE", "DTA_CONSTITUCION", "DTA_ACTUALIZACION",
        "TXT_CIUDAD_INST", "country", "department", "city",
    ]
    return sum(bool(reg.get(field)) for field in fields)


def candidate_names(affiliation):
    return [
        item.get("name")
        for item in affiliation.get("names", [])
        if item.get("name")
    ]


def institution_name_match(affiliation, source_names):
    """Return the strongest strict name match for an affiliation."""
    best = None
    for source_name in source_names:
        source_name = normalize_institution_name(source_name)
        if not source_name:
            continue
        source_tokens = set(source_name.split()) - NAME_STOPWORDS
        source_distinctive = distinctive_institution_tokens(source_name)
        for name in candidate_names(affiliation):
            normalized_name = normalize_institution_name(name)
            if not normalized_name:
                continue
            name_tokens = set(normalized_name.split()) - NAME_STOPWORDS
            name_distinctive = distinctive_institution_tokens(normalized_name)
            same_distinctive_terms = (
                bool(source_distinctive)
                and source_distinctive == name_distinctive
            )
            overlap = len(source_tokens & name_tokens) / max(
                1, min(len(source_tokens), len(name_tokens)))
            scores = {
                "ratio": fuzz.ratio(normalized_name, source_name),
                "token_sort_ratio": fuzz.token_sort_ratio(normalized_name, source_name),
                "token_set_ratio": fuzz.token_set_ratio(normalized_name, source_name),
                "wratio": fuzz.WRatio(normalized_name, source_name),
            }
            exact = normalized_name == source_name and bool(source_distinctive)
            strict = same_distinctive_terms and any([
                scores["ratio"] >= 94,
                scores["token_sort_ratio"] >= 96,
                scores["token_set_ratio"] >= 98 and overlap >= 0.80 and min(
                    len(source_tokens), len(name_tokens)) >= 2,
                scores["wratio"] >= 96 and overlap >= 0.80 and min(
                    len(source_tokens), len(name_tokens)) >= 2,
            ])
            current = {
                "exact": exact,
                "strict": strict,
                "score": max(scores.values()),
                "overlap": overlap,
            }
            if best is None or (current["exact"], current["score"], current["overlap"]) > (
                    best["exact"], best["score"], best["overlap"]):
                best = current
    return best


def canonical_names_compatible(canonical_names, source_name):
    """Match canonical and Scienti names after removing only legal-form words."""
    source_tokens = distinctive_institution_tokens(source_name)
    return bool(source_tokens) and any(
        source_tokens == distinctive_institution_tokens(name)
        for name in canonical_names
    )


class Kahi_scienti_affiliations(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("names.name")
        self.collection.create_index("types.type")
        self.collection.create_index([("names.name", TEXT)])

        self.verbose = config["scienti_affiliations"]["verbose"] if "verbose" in config["scienti_affiliations"].keys(
        ) else 0
        # Sin progress_interval se usa el estandar de kahi.progress (5%, minimo 1000).
        progress_interval = config["scienti_affiliations"].get("progress_interval")
        self.progress_interval = int(progress_interval) if progress_interval is not None else None
        self.progress_seconds = float(
            config["scienti_affiliations"].get("progress_seconds", 60))
        if (self.progress_interval is not None and self.progress_interval < 1) or self.progress_seconds <= 0:
            raise ValueError("progress_interval and progress_seconds must be positive")

        name_index = False
        for key, val in self.collection.index_information().items():
            if key == "names.name_text":
                name_index = True
                break
        if not name_index:
            self.collection.create_index([("names.name", TEXT)])
            print("Text index created on names.name field")

        # checking if the databases and collections are available
        self._progress_log("validating source databases and collections")
        self.check_databases_and_collections()
        # creating indexes for the scienti sources
        self.create_source_indexes()

    def _progress_log(self, message, verbose=None):
        if (getattr(self, "verbose", 0) if verbose is None else verbose) > 0:
            print("[scienti_affiliations] {}".format(message), flush=True)

    def _progress(self, records, label, total=None, verbose=None):
        """Report completed records, including iterations that use continue."""
        verbosity = getattr(self, "verbose", 0) if verbose is None else verbose
        if verbosity < 1:
            yield from records
            return
        progress = ProgressLogger(
            "scienti_affiliations:{}".format(label),
            total=total,
            every=getattr(self, "progress_interval", None),
            seconds=getattr(self, "progress_seconds", 60),
        ).start()
        for record in records:
            yield record
            progress.step()
        progress.done()

    def check_databases_and_collections(self):
        for db_info in self.config["scienti_affiliations"]["databases"]:
            client = MongoClient(db_info["database_url"])
            try:
                database_name = db_info["database_name"]
                if database_name not in client.list_database_names():
                    raise Exception("Database {} not found".format(database_name))
                available = client[database_name].list_collection_names()
                required = [
                    db_info["collection_name"],
                    db_info.get("institution_collection_name", "institution"),
                ]
                for collection_name in required:
                    if collection_name not in available:
                        raise Exception(
                            "Collection {}.{} not found in {}".format(
                                database_name, collection_name, db_info["database_url"])
                        )
            finally:
                client.close()

    def create_source_indexes(self):
        for db_info in self.config["scienti_affiliations"]["databases"]:
            database_url = db_info.get('database_url', '')
            database_name = db_info.get('database_name', '')
            collection_name = db_info.get('collection_name', '')
            institution_collection_name = db_info.get(
                "institution_collection_name", "institution")

            if database_url and database_name and collection_name:
                self._progress_log("preparing indexes: {}.{} and {}".format(
                    database_name, collection_name, institution_collection_name))
                client = MongoClient(database_url)
                try:
                    db = client[database_name]
                    collection = db[collection_name]
                    collection.create_index(
                        [('group.institution.TXT_NIT', ASCENDING)])
                    collection.create_index([('group.COD_ID_GRUPO', ASCENDING)])
                    institutions = db[institution_collection_name]
                    institutions.create_index([('COD_INST', ASCENDING)])
                    institutions.create_index([('ID_INSTITUCION', ASCENDING)])
                    institutions.create_index([('COD_INST_MACRO', ASCENDING)])
                finally:
                    client.close()

    def load_institution_catalog(self, configs):
        """Load one best copy of every COD_INST from overlapping Scienti dumps."""
        catalog = {}
        for config in configs:
            client = MongoClient(config["database_url"])
            try:
                collection_name = config.get(
                    "institution_collection_name", "institution")
                institutions = client[config["database_name"]][collection_name]
                records = self._progress(
                    institutions.find({}, {"_id": 0}),
                    "catalog {}.{}".format(config["database_name"], collection_name),
                    total=institutions.count_documents({}),
                )
                for reg in records:
                    cod_inst = clean_text(reg.get("COD_INST"))
                    name = clean_text(reg.get("NME_INST"))
                    if not cod_inst or cod_inst == "000000000000" or not name:
                        continue
                    previous = catalog.get(cod_inst)
                    if previous is None or institution_record_score(reg) > institution_record_score(previous):
                        catalog[cod_inst] = reg
            finally:
                client.close()
        return catalog

    def catalog_unique_values(self, catalog, field):
        """Return values associated with only one logical Scienti institution."""
        owners = {}
        for cod_inst, reg in catalog.items():
            if field == "TXT_NIT":
                value = normalize_nit(reg)
            else:
                value = clean_text(reg.get(field))
            if not value:
                continue
            owner = clean_text(reg.get("ID_INSTITUCION")) or cod_inst
            owners.setdefault(value, set()).add(owner)
        return {value for value, values in owners.items() if len(values) == 1}

    def find_by_external_id(self, source, value):
        if not value:
            return None
        return self.collection.find_one({
            "external_ids": {"$elemMatch": {"source": source, "id": value}},
            "types.type": {"$ne": "group"},
        })

    def find_snies_canonical(self, code):
        """Return the principal affiliation for a SNIES institution or campus."""
        if not code:
            return None
        canonical = self.collection.find_one({
            "external_ids": {"$elemMatch": {
                "source": "snies", "id": code, "provenance": "snies",
            }},
            "types.type": {"$ne": "group"},
        })
        if canonical:
            return canonical
        canonical = self.collection.find_one({
            "campuses.external_ids": {"$elemMatch": {
                "source": "snies", "id": code, "provenance": "snies",
            }},
            "types.type": {"$ne": "group"},
        })
        if canonical:
            return canonical
        return self.collection.find_one({
            "_id": "snies_{}".format(code),
            "types.type": {"$ne": "group"},
        })

    @staticmethod
    def is_embedded_snies_campus(affiliation, code):
        return any(
            item.get("source") == "snies"
            and item.get("provenance") == "snies"
            and str(item.get("id")) == str(code)
            for campus in (affiliation or {}).get("campuses", [])
            for item in campus.get("external_ids", [])
        )

    @staticmethod
    def add_parent_relation(entry, parent):
        """Link a Scienti unit or campus to its canonical SNIES institution."""
        if not parent or entry.get("_id") == parent.get("_id"):
            return
        name = next((
            item.get("name") for item in parent.get("names", [])
            if item.get("name") and item.get("lang") == "es"
        ), "") or next(iter(candidate_names(parent)), "")
        relation = {
            "id": parent["_id"],
            "name": name,
            "types": parent.get("types", []),
        }
        current = next((
            item for item in entry["relations"]
            if item.get("id") == parent["_id"]
        ), None)
        if current:
            current.update(relation)
        else:
            entry["relations"].append(relation)

    def institution_candidates(self, reg):
        source_names = [reg.get("NME_INST"), reg.get("NME_INST_FILTRO")]
        candidates = []
        seen = set()
        name = clean_text(reg.get("NME_INST"))
        if name:
            direct = self.collection.find({
                "names.name": {"$regex": "^{}$".format(re.escape(name)), "$options": "i"},
                "types.type": {"$ne": "group"},
            }).limit(20)
            for candidate in direct:
                if candidate["_id"] not in seen:
                    candidates.append(candidate)
                    seen.add(candidate["_id"])

        query = normalize_institution_name(reg.get("NME_INST_FILTRO")) or normalize_institution_name(name)
        if query:
            cursor = self.collection.find({
                "$text": {"$search": query},
                "types.type": {"$ne": "group"},
            }, {"names": 1, "addresses": 1, "external_ids": 1, "relations": 1,
                "score": {"$meta": "textScore"}}).sort(
                    [("score", {"$meta": "textScore"})]).limit(100)
            for candidate in cursor:
                if candidate["_id"] not in seen:
                    candidates.append(candidate)
                    seen.add(candidate["_id"])
        return candidates, source_names

    def countries_compatible(self, affiliation, reg):
        _, source_code = scienti_country(reg)
        if not source_code:
            return True
        target_codes = {
            clean_text(address.get("country_code")).upper()
            for address in affiliation.get("addresses", [])
            if clean_text(address.get("country_code"))
        }
        return not target_codes or source_code in target_codes

    def has_different_scienti_code(self, affiliation, cod_inst):
        codes = {
            ext.get("id")
            for ext in affiliation.get("external_ids", [])
            if ext.get("source") == COD_INST_SOURCE
            and re.fullmatch(r"\d{12}", str(ext.get("id", "")))
        }
        return bool(codes and cod_inst not in codes)

    def match_institution_by_name(self, reg):
        candidates, source_names = self.institution_candidates(reg)
        matches = []
        cod_inst = clean_text(reg.get("COD_INST"))
        for candidate in candidates:
            if not self.countries_compatible(candidate, reg):
                continue
            if self.has_different_scienti_code(candidate, cod_inst):
                continue
            match = institution_name_match(candidate, source_names)
            if not match or not (match["exact"] or match["strict"]):
                continue
            has_authoritative_id = any(
                ext.get("source") in {"ror", "snies"}
                for ext in candidate.get("external_ids", []))
            matches.append((candidate, match, has_authoritative_id))
        if not matches:
            return None
        matches.sort(
            key=lambda item: (
                item[1]["exact"], item[2], item[1]["score"], item[1]["overlap"]),
            reverse=True,
        )
        if len(matches) > 1:
            first = matches[0]
            second = matches[1]
            first_rank = (first[1]["exact"], first[2], first[1]["score"], first[1]["overlap"])
            second_rank = (second[1]["exact"], second[2], second[1]["score"], second[1]["overlap"])
            if first_rank == second_rank:
                return None
        return self.collection.find_one({"_id": matches[0][0]["_id"]})

    def find_scienti_institution(self, reg, unique_nits, unique_cod_ies):
        """Match by stable identifiers first, then by an unambiguous strict name."""
        cod_inst = clean_text(reg.get("COD_INST"))
        institution = self.find_by_external_id(COD_INST_SOURCE, cod_inst)
        if institution:
            return institution

        internal_id = clean_text(reg.get("ID_INSTITUCION"))
        institution = self.find_by_external_id(ID_INSTITUCION_SOURCE, internal_id)
        if institution:
            return institution

        nit = normalize_nit(reg)
        if nit in unique_nits:
            institution = self.find_by_external_id("nit", nit)
            if institution:
                return institution

        cod_ies = clean_text(reg.get("COD_IES"))
        if cod_ies in unique_cod_ies:
            institution = self.find_snies_canonical(cod_ies)
            if institution:
                return institution
        return self.match_institution_by_name(reg)

    def save_scienti_institution(self, institution, reg):
        cod_ies = clean_text(reg.get("COD_IES"))
        canonical = self.find_snies_canonical(cod_ies)
        if canonical and self.is_embedded_snies_campus(canonical, cod_ies):
            # Campus-level Scienti evidence resolves to the principal.  Do not
            # copy campus names, geography or identifiers onto the parent.
            return canonical["_id"], False
        include_snies_id = not canonical or (
            institution and institution.get("_id") == canonical.get("_id"))
        if institution:
            entry = apply_scienti_institution_data(
                institution, reg, include_snies_id=include_snies_id)
            if canonical and entry["_id"] != canonical["_id"]:
                self.add_parent_relation(entry, canonical)
            self.collection.update_one(
                {"_id": entry["_id"]},
                {"$set": {
                    "updated": entry["updated"],
                    "names": entry["names"],
                    "abbreviations": entry["abbreviations"],
                    "types": entry["types"],
                    "year_established": entry.get("year_established"),
                    "addresses": entry["addresses"],
                    "external_urls": entry["external_urls"],
                    "external_ids": entry["external_ids"],
                    "relations": entry["relations"],
                }},
            )
            return entry["_id"], False

        cod_inst = clean_text(reg.get("COD_INST"))
        entry = apply_scienti_institution_data(
            self.empty_affiliation(), reg,
            include_snies_id=include_snies_id)
        # COD_INST is already a persistent source identifier. Keeping it as
        # the document id avoids a second plugin-specific identity namespace.
        entry["_id"] = cod_inst
        self.add_parent_relation(entry, canonical)
        try:
            self.collection.insert_one(order_document(entry, self.empty_affiliation()))
            return entry["_id"], True
        except DuplicateKeyError:
            current = self.collection.find_one({"_id": entry["_id"]})
            return self.save_scienti_institution(current, reg)[0], False

    def compatible_scienti_codes(self, affiliation, codes, catalog):
        """Keep canonical matches and their explicit Scienti hierarchy together."""
        canonical_names = []
        for source in ["ror", "wikidata", "openalex"]:
            canonical_names = [
                item.get("name")
                for item in affiliation.get("names", [])
                if item.get("source") == source and item.get("name")
            ]
            if canonical_names:
                break
        if not canonical_names:
            return set(codes)

        compatible = {
            code for code in codes
            if canonical_names_compatible(
                canonical_names, catalog[code].get("NME_INST"))
        }
        if not compatible:
            return set(codes)

        owners = {}
        for code in codes:
            owner = clean_text(catalog[code].get("ID_INSTITUCION"))
            if owner:
                owners.setdefault(owner, set()).add(code)
        changed = True
        while changed:
            changed = False
            for code in codes - compatible:
                reg = catalog[code]
                macro = clean_text(reg.get("COD_INST_MACRO"))
                same_owner = owners.get(
                    clean_text(reg.get("ID_INSTITUCION")), set())
                related = macro in compatible or any(
                    clean_text(catalog[item].get("COD_INST_MACRO")) == code
                    for item in compatible
                ) or bool(same_owner & compatible)
                if related:
                    compatible.add(code)
                    changed = True
        return compatible

    def rebuild_scienti_metadata(self, affiliation, retained_codes, all_codes, catalog):
        """Remove contaminated Scienti metadata and replay only retained records."""
        entry = deepcopy(affiliation)
        entry["names"] = [
            item for item in entry.get("names", [])
            if item.get("source") != SOURCE
        ]
        source_abbreviations = {
            clean_text(catalog[code].get("SGL_INST"))
            for code in all_codes
            if clean_text(catalog[code].get("SGL_INST"))
        }
        entry["abbreviations"] = [
            item for item in entry.get("abbreviations", [])
            if item not in source_abbreviations
        ]
        for field in ["external_ids", "external_urls", "types"]:
            entry[field] = [
                item for item in entry.get(field, [])
                if item.get("provenance") != SOURCE
            ]
        entry["updated"] = [
            item for item in entry.get("updated", [])
            if item.get("source") != SOURCE
        ]
        for code in sorted(retained_codes):
            apply_scienti_institution_data(entry, catalog[code])
        return entry

    def repair_scienti_code_fusions(self, catalog):
        """Split unrelated COD_INST values previously merged by generic names."""
        repaired_ids = set()
        separated_codes = set()
        query = {
            "external_ids": {"$elemMatch": {"source": COD_INST_SOURCE}},
            "types.type": {"$ne": "group"},
        }
        for affiliation in self.collection.find(query):
            codes = {
                str(item.get("id"))
                for item in affiliation.get("external_ids", [])
                if item.get("source") == COD_INST_SOURCE
                and str(item.get("id")) in catalog
            }
            if len(codes) < 2:
                continue
            retained = self.compatible_scienti_codes(
                affiliation, codes, catalog)
            if retained == codes:
                continue
            entry = self.rebuild_scienti_metadata(
                affiliation, retained, codes, catalog)
            self.collection.update_one(
                {"_id": affiliation["_id"]},
                {"$set": {
                    "updated": entry["updated"],
                    "names": entry["names"],
                    "abbreviations": entry["abbreviations"],
                    "types": entry["types"],
                    "external_urls": entry["external_urls"],
                    "external_ids": entry["external_ids"],
                }},
            )
            repaired_ids.add(affiliation["_id"])
            separated_codes.update(codes - retained)
            self._progress_log(
                "repaired {}: retained {} COD_INST; separated {}".format(
                    affiliation["_id"], len(retained), len(codes - retained)))
        self.last_separated_scienti_codes = separated_codes
        return repaired_ids

    def add_catalog_relations(self, catalog, cod_to_affiliation):
        changed = 0
        for cod_inst, reg in self._progress(
                catalog.items(), "hierarchy relations", total=len(catalog)):
            macro = clean_text(reg.get("COD_INST_MACRO"))
            child_id = cod_to_affiliation.get(cod_inst)
            parent_id = cod_to_affiliation.get(macro)
            if not macro or macro == cod_inst or not child_id or not parent_id or child_id == parent_id:
                continue
            child = self.collection.find_one({"_id": child_id})
            parent = self.collection.find_one({"_id": parent_id})
            if not child or not parent:
                continue
            parent_name = next(iter(candidate_names(parent)), "")
            relation = {
                "id": parent_id,
                "name": parent_name,
                "types": parent.get("types", []),
            }
            relations = child.get("relations", [])
            existing = next(
                (item for item in relations if item.get("id") == parent_id), None)
            if existing == relation:
                continue
            if existing:
                existing.update(relation)
            else:
                relations.append(relation)
            self.collection.update_one(
                {"_id": child_id}, {"$set": {"relations": relations}})
            changed += 1
        return changed

    def process_scienti_institutions(self, configs, verbose=0):
        """Prepare the Scienti catalog without materializing it wholesale."""
        if isinstance(configs, dict):
            configs = [configs]
        catalog = self.load_institution_catalog(configs)
        self.institution_catalog = catalog
        self.unique_scienti_nits = self.catalog_unique_values(catalog, "TXT_NIT")
        self.unique_scienti_cod_ies = self.catalog_unique_values(catalog, "COD_IES")
        self.materialized_scienti_codes = {}
        self.repaired_affiliation_ids = set()
        if verbose > 0:
            print(
                "Prepared {} Scienti institutions as an on-demand lookup catalog; "
                "none were inserted wholesale".format(len(catalog)),
                flush=True,
            )
        return {}

    def materialize_scienti_institution(self, cod_inst, stack=None):
        """Insert or enrich only a catalog institution referenced by a group."""
        cod_inst = clean_text(cod_inst)
        catalog = getattr(self, "institution_catalog", {})
        reg = catalog.get(cod_inst)
        if not reg:
            return None
        cached_id = getattr(self, "materialized_scienti_codes", {}).get(cod_inst)
        if cached_id:
            return self.collection.find_one({"_id": cached_id})

        stack = set(stack or ())
        if cod_inst in stack:
            raise RuntimeError("cycle in Scienti institution hierarchy: {}".format(cod_inst))
        stack.add(cod_inst)
        macro = clean_text(reg.get("COD_INST_MACRO"))
        parent = None
        if macro and macro != cod_inst and macro in catalog:
            parent = self.materialize_scienti_institution(macro, stack)

        institution = self.find_by_external_id(COD_INST_SOURCE, cod_inst)
        if not institution and not parent:
            institution = self.find_scienti_institution(
                reg,
                getattr(self, "unique_scienti_nits", set()),
                getattr(self, "unique_scienti_cod_ies", set()),
            )
        affiliation_id, _ = self.save_scienti_institution(institution, reg)
        if parent and affiliation_id != parent["_id"]:
            entry = self.collection.find_one({"_id": affiliation_id})
            self.add_parent_relation(entry, parent)
            self.collection.update_one(
                {"_id": affiliation_id}, {"$set": {"relations": entry["relations"]}})
        self.materialized_scienti_codes[cod_inst] = affiliation_id
        return self.collection.find_one({"_id": affiliation_id})

    def extract_subject(self, subjects, data):
        subjects.append({
            "id": "",
            "name": data["TXT_NME_AREA"],
            "level": data["NRO_NIVEL"],
            "external_ids": [{"source": "OCDE", "id": data["COD_AREA_CONOCIMIENTO"]}]
        })
        if "knowledge_area" in data.keys():
            self.extract_subject(subjects, data["knowledge_area"][0])
        return subjects

    def embedded_parent_institution(self, inst):
        """Resolve an embedded group institution using catalog identifiers first."""
        cod_inst = clean_text(inst.get("COD_INST"))
        institution = self.find_by_external_id(COD_INST_SOURCE, cod_inst)
        if institution:
            return institution
        if cod_inst in getattr(self, "institution_catalog", {}):
            return self.materialize_scienti_institution(cod_inst)

        internal_id = clean_text(inst.get("ID_INSTITUCION"))
        institution = self.find_by_external_id(ID_INSTITUCION_SOURCE, internal_id)
        if institution:
            return institution

        nit = normalize_nit(inst)
        if not nit:
            return None
        institution = self.find_by_external_id("nit", nit)
        if institution:
            return institution
        verification_digit = clean_text(inst.get("TXT_DIGITO_VERIFICADOR"))
        legacy_nit = "{}-{}".format(nit, verification_digit) if verification_digit else ""
        return self.find_by_external_id("nit", legacy_nit)

    def canonical_group_institutions(self, institution):
        """Resolve a group institution to authoritative principal parents."""
        queue = [institution]
        seen = set()
        authoritative = []
        while queue:
            current = queue.pop(0)
            if not current or current.get("_id") in seen:
                continue
            seen.add(current.get("_id"))
            is_authoritative = any(
                item.get("source") == "ror" or
                (item.get("source") == "snies"
                 and item.get("provenance") == "snies")
                for item in current.get("external_ids", []))
            if is_authoritative:
                authoritative.append(current)
            for relation in current.get("relations", []):
                parent = self.collection.find_one({
                    "_id": relation.get("id"),
                    "types.type": {"$ne": "group"},
                })
                if parent:
                    queue.append(parent)
        return authoritative or [institution]

    def group_parent_institutions(self, scienti, group_id):
        parents = []
        seen = set()
        for reg in scienti.find({"group.COD_ID_GRUPO": group_id}):
            for group in reg.get("group", []):
                if group.get("COD_ID_GRUPO") != group_id:
                    continue
                for inst in group.get("institution", []):
                    parent = self.embedded_parent_institution(inst)
                    for canonical in (
                            self.canonical_group_institutions(parent)
                            if parent else []):
                        if canonical["_id"] not in seen:
                            parents.append(canonical)
                            seen.add(canonical["_id"])
        return parents

    def enrich_group_parents(self, entry, parents):
        changed = False
        parent_ids = {parent["_id"] for parent in parents}
        repaired_ids = getattr(self, "repaired_affiliation_ids", set())
        relations = entry.get("relations", [])
        clean_relations = [
            relation for relation in relations
            if relation.get("id") not in repaired_ids
            or relation.get("id") in parent_ids
        ]
        if clean_relations != relations:
            entry["relations"] = clean_relations
            changed = True
        for parent in parents:
            name = ""
            for item in parent.get("names", []):
                if item.get("name") and item.get("lang") == "es":
                    name = item["name"]
                    break
                if item.get("name") and not name:
                    name = item["name"]
            relation = {
                "name": name,
                "id": parent["_id"],
                "types": parent.get("types", []),
            }
            current = next(
                (item for item in entry.get("relations", [])
                 if item.get("id") == parent["_id"]), None)
            if current is None:
                entry["relations"].append(relation)
                changed = True
            elif current != relation:
                current.update(relation)
                changed = True
        return inherit_parent_addresses(entry, parents) or changed

    def process_scienti_groups(self, config, verbose=0):
        client = MongoClient(config["database_url"])
        db = client[config["database_name"]]
        scienti = db[config["collection_name"]]
        label = "groups {}.{}".format(config["database_name"], config["collection_name"])
        self._progress_log("{}: listing unique group IDs".format(label), verbose)
        group_ids = scienti.distinct(
            "group.COD_ID_GRUPO", {"group.COD_ID_GRUPO": {"$ne": None}})
        for group_id in self._progress(
                group_ids, label, total=len(group_ids), verbose=verbose):
            db_reg = self.collection.find_one({"external_ids.id": group_id})
            parent_institutions = self.group_parent_institutions(
                scienti, group_id)
            group = {}
            if not db_reg or db_reg.get("year_established") in {None, "", -1}:
                source = scienti.find_one(
                    {"group.COD_ID_GRUPO": group_id},
                    {"group": {"$elemMatch": {"COD_ID_GRUPO": group_id}}},
                )
                group = ((source or {}).get("group") or [{}])[0]
            if db_reg:
                updates = {}
                if self.enrich_group_parents(db_reg, parent_institutions):
                    updates.update(
                        addresses=db_reg["addresses"],
                        relations=db_reg["relations"],
                    )
                year = scienti_group_year_established(group)
                if year is not None and db_reg.get("year_established") in {None, "", -1}:
                    updates["year_established"] = year
                if updates:
                    self.collection.update_one(
                        {"_id": db_reg["_id"]},
                        {"$set": updates},
                    )
                continue
            entry = self.empty_affiliation()
            entry["updated"].append({"source": "scienti", "time": int(time())})
            entry["external_ids"].append(
                {"source": "minciencias", "id": group_id})
            entry["types"].append({"source": "scienti", "type": "group"})

            if group:
                entry["external_ids"].append(
                    {"source": "scienti", "id": group["NRO_ID_GRUPO"]})
                entry["names"].append(
                    {"name": group["NME_GRUPO"], "lang": "es", "source": "scienti"})
                entry["year_established"] = scienti_group_year_established(group)
                if group["STA_ELIMINADO"] == "F":
                    entry["status"].append(
                        {"source": "minciencias", "status": "activo"})
                if group["STA_ELIMINADO"] == "T" or group["STA_ELIMINADO"] == "V":
                    entry["status"].append(
                        {"source": "minciencias", "status": "eliminado"})

                entry["description"].append({
                    "source": "scienti",
                    "description": {
                        "TXT_PLAN_TRABAJO": group["TXT_PLAN_TRABAJO"] if "TXT_PLAN_TRABAJO" in group.keys() else "",
                        "TXT_ESTADO_ARTE": group["TXT_ESTADO_ARTE"] if "TXT_ESTADO_ARTE" in group.keys() else "",
                        "TXT_OBJETIVOS": group["TXT_OBJETIVOS"]if "TXT_OBJETIVOS" in group.keys() else "",
                        "TXT_PROD_DESTACADA": group["TXT_PROD_DESTACADA"]if "TXT_PROD_DESTACADA" in group.keys() else "",
                        "TXT_RETOS": group["TXT_RETOS"]if "TXT_RETOS" in group.keys() else "",
                        "TXT_VISION": group["TXT_VISION"] if "TXT_VISION" in group.keys() else ""
                    }
                })

                if "TXT_CLASIF" in group.keys() and "DTA_CLASIF" in group.keys():
                    entry["ranking"].append({
                        "source": "scienti",
                        "rank": normalize_scienti_rank(group["TXT_CLASIF"]),
                        "from_date": check_date_format(group["DTA_CLASIF"]),
                        "to_date": check_date_format(group["DTA_FIN_CLASIF"])
                    })

                subjects = self.extract_subject([], group["knowledge_area"][0])
                if len(subjects) > 0:
                    entry["subjects"].append({
                        "source": "OCDE",
                        "subjects": subjects
                    })

                self.enrich_group_parents(entry, parent_institutions)
                entry["_id"] = group_id
                self.collection.insert_one(order_document(entry, self.empty_affiliation()))
        client.close()

    def run(self):
        start_time = monotonic()
        configs = self.config["scienti_affiliations"]["databases"]
        self.process_scienti_institutions(configs, verbose=self.verbose)
        for config in configs:
            self.process_scienti_groups(config, verbose=self.verbose)
        self._progress_log("DONE en {}".format(format_duration(monotonic() - start_time)))
        return 0
