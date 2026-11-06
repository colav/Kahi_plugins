from copy import deepcopy
from datetime import datetime
import re
from time import time

from kahi.KahiBase import KahiBase
from kahi.progress import ProgressLogger
from kahi_impactu_utils.String import title_case
from pymongo import MongoClient, TEXT
from thefuzz import fuzz
from unidecode import unidecode


SOURCE = "snies"
COUNTRY = "Colombia"
COUNTRY_CODE = "CO"
PROTECTED_NAME_SEGMENTS = {
    "ALEJANDRIA",
    "CALI",
    "CAPITAN ANDRES M DIAZ",
    "CARTAGENA",
    "COLEGIO MAYOR DE CUNDINAMARCA",
    "DE CALI",
    "DIEGO LUIS CORDOBA",
    "ELIAS BECHARA ZAINUM",
    "FRANCISCO JOSE DE CALDAS",
    "JESUS OVIEDO PEREZ",
    "JORGE TADEO LOZANO",
    "MISION PAZ",
    "MONSENOR ABRAHAM ESCUDERO MONTOYA",
    "NUEVA GRANADA",
    "SAN MATEO EDUCACION SUPERIOR",
    "UNION AMERICANA",
    "UNIVERSITARIA DE BOGOTA",
    "UNIVERSITARIA DE COLOMBIA",
}
TEMPORARY_SEARCH_ALIASES = {
    "universidad fucs": "Fundación Universitaria de Ciencias de la Salud",
}
GENERIC_INSTITUTION_TOKENS = {
    "asociacion", "centro", "colegio", "corporacion", "educacion", "escuela",
    "fundacion", "institucion", "instituto", "superior", "tecnica", "tecnico",
    "tecnologica", "tecnologico", "universidad", "universitaria", "universitario",
}
NAME_STOPWORDS = {
    "a", "de", "del", "e", "el", "en", "la", "las", "los", "para", "por", "y",
}


def clean_text(value):
    """
    Return a stripped string or an empty string for missing source values.
    """
    if value is None:
        return ""
    return str(value).strip()


def normalize_name(value):
    """
    Normalize names for text/fuzzy comparison without changing stored values.
    """
    value = unidecode(clean_text(value).lower())
    for token in ["(", ")", ".", ",", "-", "_", "/"]:
        value = value.replace(token, " ")
    return " ".join(value.split())


def distinctive_name_tokens(value):
    """Exclude legal-form words that cannot identify an institution alone."""
    return set(normalize_name(value).split()) - (
        GENERIC_INSTITUTION_TOKENS | NAME_STOPWORDS)


def get_first(reg, keys):
    """
    Return the first non-empty value found in a SNIES raw record.
    """
    for key in keys:
        value = clean_text(reg.get(key))
        if value:
            return value
    return ""


def add_unique(items, item):
    """
    Append an item to a list only when it is complete and not already present.
    """
    if item and item not in items:
        items.append(item)


def snies_title(value):
    """
    Format SNIES uppercase names as Spanish title text.
    """
    name = title_case(strip_snies_acronyms(clean_text(value))[0])
    name_map = {
        "Colegio Mayor de Nuestra Señora del Rosario": "Universidad del Rosario",
        "Universidad Fucs": "Universidad FUCS",
    }
    return name_map.get(name, name)


def clean_acronym(value):
    """
    Normalize an acronym candidate from a SNIES institution name segment.
    """
    value = clean_text(value).strip(" -.,;:()[]{}")
    return re.sub(r"\s+", " ", value)


def is_acronym(value):
    """
    Return True for short uppercase acronym-like segments.

    This intentionally rejects long multi-word segments, so founder names such as
    "ELIAS BECHARA ZAINUM" remain part of the institution name.
    """
    value = clean_acronym(value)
    normalized_value = unidecode(value).upper().replace(".", "")
    if normalized_value in PROTECTED_NAME_SEGMENTS:
        return False

    compact = re.sub(r"[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9]", "", value)
    if not 2 <= len(compact) <= 15:
        return False

    words = value.split()
    if len(words) > 2:
        return False

    letters = [char for char in compact if char.isalpha()]
    return len(letters) >= 2 and compact.upper() == compact


def split_acronym_segment(segment):
    """
    Split acronym-bearing segments such as "CEDOC DEL EJERCITO NACIONAL".
    """
    segment = clean_acronym(segment)
    words = segment.split()
    if words and is_acronym(words[0]) and len(words[0]) >= 4:
        return " ".join(words[1:]), words[0]
    return segment, ""


def strip_snies_acronyms(value):
    """
    Remove acronym segments from a SNIES name and return the acronyms separately.
    """
    value = clean_text(value)
    if not value:
        return "", []

    parts = [clean_acronym(part) for part in re.split(r"\s*-\s*", value) if clean_acronym(part)]
    if not parts:
        return value, []

    name_parts = []
    acronyms = []
    for index, part in enumerate(parts):
        if index == 0 and is_acronym(part) and len(parts) > 1:
            acronyms.append(clean_acronym(part))
            continue

        if index > 0 and len(clean_acronym(part)) == 1:
            continue

        if index > 0 and is_acronym(part):
            acronyms.append(clean_acronym(part))
            continue

        name_parts.append(part)

    return " - ".join(name_parts), acronyms


def build_text_query(name):
    """
    Build a compact text-search query from the institution name.
    """
    stopwords = {
        "de",
        "del",
        "la",
        "las",
        "los",
        "el",
        "y",
        "en",
        "para",
        "universidad",
        "institucion",
        "institución",
        "corporacion",
        "corporación",
        "fundacion",
        "fundación",
        "instituto",
    }
    return " ".join(token for token in normalize_name(name).split() if token not in stopwords)


def get_candidate_names(affiliation):
    """
    Return unique names from an affiliation document.
    """
    names = []
    for item in affiliation.get("names", []):
        name = item.get("name")
        if name and name not in names:
            names.append(name)
    return names


def find_by_snies_code(collection, code):
    """
    Find an affiliation already linked to the SNIES institution code.
    """
    if not code:
        return None
    affiliation = collection.find_one({
        "external_ids": {"$elemMatch": {
            "source": SOURCE, "id": code, "provenance": SOURCE,
        }},
    })
    if affiliation:
        return affiliation
    return collection.find_one({"_id": f"snies_{code}"})


def snies_parent_code(reg):
    return normalize_snies_code(get_first(
        reg, ["CÓDIGO_IES_PADRE", "CODIGO_IES_PADRE"]))


def is_snies_sectional(reg):
    code = normalize_snies_code(get_first(
        reg, ["CÓDIGO_INSTITUCIÓN", "CODIGO_INSTITUCION"]))
    parent = snies_parent_code(reg)
    return bool(code and parent and code != parent)


def snies_campus(reg):
    """Map an official SNIES sectional into compact parent metadata."""
    code = normalize_snies_code(get_first(
        reg, ["CÓDIGO_INSTITUCIÓN", "CODIGO_INSTITUCION"]))
    if not code:
        return None
    address = snies_address(reg)
    status = snies_status(reg)
    return {
        "id": "snies_{}".format(code),
        "name": snies_title(get_first(
            reg, ["NOMBRE_INSTITUCIÓN", "NOMBRE_INSTITUCION"])),
        "types": [{
            "provenance": SOURCE, "source": SOURCE, "type": "campus",
        }],
        "external_ids": snies_external_ids(reg),
        "addresses": [address] if address else [],
        "status": [status] if status else [],
    }


def embed_snies_campus(collection, reg):
    """Insert or refresh a sectional inside its principal affiliation."""
    if not is_snies_sectional(reg):
        return False
    parent = find_by_snies_code(collection, snies_parent_code(reg))
    campus = snies_campus(reg)
    if not parent or not campus:
        return False
    campuses = parent.get("campuses", [])
    current = next((item for item in campuses
                    if item.get("id") == campus["id"]), None)
    if current == campus:
        return False
    if current:
        current.clear()
        current.update(campus)
    else:
        campuses.append(campus)
    collection.update_one(
        {"_id": parent["_id"]}, {"$set": {"campuses": campuses}})
    return True


def find_by_name(collection, name, code="", country=COUNTRY, verbose=0):
    """
    Find an affiliation by name using the same text/fuzzy approach used by other Kahi plugins.
    """
    if not name:
        return None

    direct_match = find_by_name_value(collection, name, country=country)
    if (direct_match and distinctive_name_tokens(name)
            and not has_different_snies_code(direct_match, code)):
        return direct_match

    alias = TEMPORARY_SEARCH_ALIASES.get(normalize_name(name))
    if alias:
        alias_match = find_by_name(
            collection, alias, code=code, country=country, verbose=verbose)
        if alias_match:
            return alias_match

    query = build_text_query(name) or normalize_name(name)
    source_name = normalize_name(name)
    match = match_name_candidates(
        get_name_candidates(collection, query, country), source_name, code)
    if match:
        return match

    for token in query.split():
        if len(token) < 4:
            continue
        regex_filter = {"names.name": {"$regex": token[:4], "$options": "i"}}
        if country:
            regex_filter["addresses.country"] = country
        match = match_name_candidates(
            collection.find(regex_filter).limit(100), source_name, code)
        if match:
            return match

    if verbose > 4:
        print(f"SNIES name not matched: {name}")
    return None


def get_name_candidates(collection, query, country=COUNTRY):
    """
    Get affiliation candidates using exact text search first, then broader text search.
    """
    projection = {
        "updated": 1,
        "names": 1,
        "aliases": 1,
        "abbreviations": 1,
        "types": 1,
        "year_established": 1,
        "status": 1,
        "relations": 1,
        "addresses": 1,
        "external_ids": 1,
        "external_urls": 1,
        "subjects": 1,
        "ranking": 1,
        "description": 1,
        "citation_count": 1,
        "products_count": 1,
        "score": {"$meta": "textScore"},
    }
    base_query = {
        "types.type": {"$ne": "group"},
    }
    if country:
        base_query["addresses.country"] = country

    candidates = []
    seen = set()
    for search in [f'"{query}"', query]:
        search_query = base_query.copy()
        search_query["$text"] = {"$search": search}
        cursor = collection.find(search_query, projection).sort(
            [("score", {"$meta": "textScore"})]
        ).limit(100)
        for candidate in cursor:
            if candidate["_id"] not in seen:
                candidates.append(candidate)
                seen.add(candidate["_id"])
    return candidates


def find_by_name_value(collection, name, country=COUNTRY):
    """
    Find a direct case-insensitive name match before applying temporary aliases.
    """
    if not name:
        return None

    regex_filter = {"names.name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}}
    if country:
        regex_filter["addresses.country"] = country
    return collection.find_one(regex_filter)


def has_different_snies_code(candidate, code):
    """Prevent a name match from attaching a second SNIES institution code."""
    codes = {
        str(item.get("id"))
        for item in candidate.get("external_ids", [])
        if item.get("source") == SOURCE and item.get("id")
    }
    return bool(code and codes and str(code) not in codes)


def candidate_match_score(candidate, source_name):
    """
    Score one candidate using the improved DAM affiliation matching logic.
    """
    best = None
    distinctive_source_tokens = distinctive_name_tokens(source_name)

    for name in get_candidate_names(candidate):
        candidate_name = normalize_name(name)
        if not candidate_name:
            continue

        distinctive_candidate_tokens = distinctive_name_tokens(candidate_name)
        distinctive_token_overlap = len(
            distinctive_source_tokens & distinctive_candidate_tokens
        ) / max(1, min(len(distinctive_source_tokens), len(distinctive_candidate_tokens)))
        scores = {
            "ratio": fuzz.ratio(candidate_name, source_name),
            "token_sort_ratio": fuzz.token_sort_ratio(candidate_name, source_name),
            "token_set_ratio": fuzz.token_set_ratio(candidate_name, source_name),
            "wratio": fuzz.WRatio(candidate_name, source_name),
        }
        score = max(scores.values())
        current = {
            "name": name,
            "normalized_name": candidate_name,
            "score": score,
            "scores": scores,
            "distinctive_token_overlap": distinctive_token_overlap,
            "distinctive_name_token_count": len(distinctive_candidate_tokens),
        }
        if best is None or current["score"] > best["score"]:
            best = current

    return best


def is_candidate_match(match, source_name):
    """
    Apply the improved matching thresholds from the DAM affiliation work.
    """
    if not match:
        return False

    scores = match["scores"]
    has_distinctive_overlap = match["distinctive_token_overlap"] > 0
    exact_match = (
        match["normalized_name"] == source_name
        and match["distinctive_name_token_count"] > 0
    )
    high_direct_score = has_distinctive_overlap and (
        scores["ratio"] >= 92 or scores["token_sort_ratio"] >= 95)
    token_subset_match = all(
        [
            scores["token_set_ratio"] >= 92,
            match["distinctive_token_overlap"] >= 0.75,
            match["distinctive_name_token_count"] >= 3,
        ]
    )
    strong_token_set_match = all(
        [
            scores["token_set_ratio"] >= 98,
            match["distinctive_token_overlap"] >= 0.75,
        ]
    )
    strong_weighted_match = all(
        [
            scores["wratio"] >= 94,
            match["distinctive_token_overlap"] >= 0.75,
        ]
    )
    return any(
        [
            exact_match,
            high_direct_score,
            token_subset_match,
            strong_token_set_match,
            strong_weighted_match,
        ]
    )


def match_name_candidates(candidates, source_name, code=""):
    """
    Return the best candidate that passes the SNIES name matching thresholds.
    """
    matches = []
    for candidate in candidates:
        if has_different_snies_code(candidate, code):
            continue
        match = candidate_match_score(candidate, source_name)
        if is_candidate_match(match, source_name):
            matches.append((candidate, match))
    if not matches:
        return None
    matches.sort(key=lambda item: item[1]["score"], reverse=True)
    candidate = matches[0][0]
    if "_collection" in candidate:
        full_candidate = candidate["_collection"].find_one({"_id": candidate["_id"]})
        if full_candidate:
            return full_candidate
    return candidate


def snies_external_ids(reg):
    """
    Convert stable SNIES identifiers into standard affiliation external_ids.
    """
    external_ids = []
    code = normalize_snies_code(get_first(reg, ["CÓDIGO_INSTITUCIÓN", "CODIGO_INSTITUCION"]))
    nit = get_first(reg, ["NÚM_IDENTIFIC_TRIBUTARIA_NIT", "NUM_IDENTIFIC_TRIBUTARIA_NIT"])

    if code:
        external_ids.append({"provenance": SOURCE, "source": SOURCE, "id": code})

    normalized_nit = normalize_nit(nit)
    if normalized_nit:
        external_ids.append({"provenance": SOURCE, "source": "nit", "id": normalized_nit})

    return external_ids


def normalize_nit(value):
    """
    Return a plausible Colombian NIT without its verification digit.

    SNIES contains placeholders and malformed values that must never be used
    as identity evidence.
    """
    value = clean_text(value)
    if not value:
        return ""

    value = value.split("-", maxsplit=1)[0]
    value = "".join(char for char in value if char.isdigit())
    invalid = {
        "000000000", "000000010", "111111111", "123456789", "999999999",
    }
    if not 7 <= len(value) <= 10:
        return ""
    if value in invalid or len(set(value)) == 1:
        return ""
    return value


def normalize_snies_code(value):
    """
    Return a valid numeric SNIES institution code.
    """
    value = clean_text(value)
    return value if value.isdigit() else ""


def snies_types(reg):
    """
    Map SNIES sector values to the standard affiliation types list.
    """
    sector = clean_text(reg.get("SECTOR"))
    sector_map = {
        "Oficial": "public",
        "Privado": "private",
    }
    if sector not in sector_map:
        return []
    return [{"provenance": SOURCE, "source": "type", "type": sector_map[sector]}]


def snies_address(reg):
    """
    Build an affiliation address from SNIES domicile fields.
    """
    state = get_first(reg, ["DEPARTAMENTO_DOMICILIO"])
    city = get_first(reg, ["MUNICIPIO_DOMICILIO"])

    if not any([state, city]):
        return None

    return {
        "lat": "",
        "lng": "",
        "postcode": "",
        "state": state,
        "city": city,
        "country": COUNTRY,
        "country_code": COUNTRY_CODE,
    }


def snies_external_urls(reg):
    """
    Build standard external_urls entries from SNIES public URL fields.
    """
    website = normalize_website(get_first(reg, ["PÁGINA_WEB", "PAGINA_WEB"]))
    if not website:
        return []
    return [{"provenance": SOURCE, "source": "site", "url": website}]


def normalize_website(value):
    """Normalize a usable SNIES website and reject placeholders."""
    value = re.sub(r"\s+", "", clean_text(value))
    if not value:
        return ""

    scheme = "http" if value.lower().startswith("http://") else "https"
    host_and_path = re.sub(
        r"^(?:(?:https?)://)+", "", value, flags=re.IGNORECASE
    ).strip("/")
    host = host_and_path.split("/", 1)[0].lower()
    if (
        "." not in host
        or normalize_name(host_and_path) in {
            "no disponible", "no aplica", "n a", "na", "ninguna", "ninguno", "1"
        }
    ):
        return ""
    return f"{scheme}://{host_and_path}"


def parse_unix_date(value):
    """
    Convert a date-like SNIES value to a Unix timestamp.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return int(value.timestamp())
    try:
        return int(datetime.fromisoformat(str(value)).timestamp())
    except ValueError:
        return None


def parse_int(value):
    """
    Convert numeric-looking SNIES values to int, returning None for missing values.
    """
    value = clean_text(value)
    if not value:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def format_resolution(value):
    """
    Format accreditation resolution values as strings without a float suffix.
    """
    parsed = parse_int(value)
    if parsed is not None:
        return str(parsed)
    return clean_text(value)


def snies_accreditation_ranking(reg):
    """
    Build the SNIES accreditation status ranking entry.
    """
    status = clean_text(reg.get("ACREDITADA_ALTA_CALIDAD")).upper() == "S"
    return {
        "provenance": SOURCE,
        "sources": "accreditation_status",
        "status": status,
        "accreditation_date": parse_unix_date(reg.get("FECHA_ACREDITACIÓN")),
        "accreditation_resolution": format_resolution(reg.get("RESOLUCIÓN_DE_LA_ACREDITACIÓN")),
        "accreditation_validity": parse_int(reg.get("VIGENCIA_DE_LA_ACREDITACIÓN")),
    }


def snies_status(reg):
    """
    Build the normalized SNIES status. SNIES status has priority over existing status values.
    """
    status = clean_text(reg.get("ESTADO"))
    status_map = {
        "Activa": "active",
        "Inactiva": "inactive",
    }
    return status_map.get(status, "")


def mark_updated(entry):
    """
    Add or refresh the SNIES updated marker.
    """
    now = int(time())
    for updated in entry["updated"]:
        if updated.get("source") == SOURCE:
            updated["time"] = now
            return
    entry["updated"].append({"source": SOURCE, "time": now})


def apply_snies_data(entry, reg):
    """
    Add SNIES values to an affiliation document without creating custom fields.
    """
    # SNIES names are Spanish, including names already supplied by SNIES.
    names = []
    for item in entry["names"]:
        if item.get("source") == SOURCE:
            item["lang"] = "es"
        add_unique(names, item)
    entry["names"] = names
    name = get_first(reg, ["NOMBRE_INSTITUCIÓN", "NOMBRE_INSTITUCION"])
    if name:
        add_unique(entry["names"], {"source": SOURCE, "name": snies_title(name), "lang": "es"})
        for acronym in strip_snies_acronyms(name)[1]:
            add_unique(entry["abbreviations"], acronym)

    for external_id in snies_external_ids(reg):
        add_unique(entry["external_ids"], external_id)

    for typ in snies_types(reg):
        add_unique(entry["types"], typ)

    address = snies_address(reg)
    if address:
        if not entry["addresses"]:
            entry["addresses"].append(address)
        else:
            first_address = entry["addresses"][0]
            if not first_address.get("state") and address.get("state"):
                first_address["state"] = address["state"]
            if not first_address.get("city") and address.get("city"):
                first_address["city"] = address["city"]

    for url in snies_external_urls(reg):
        add_unique(entry["external_urls"], url)

    status = snies_status(reg)
    if status:
        entry["status"] = [status]

    add_unique(entry["ranking"], snies_accreditation_ranking(reg))
    mark_updated(entry)
    return entry


def update_affiliation(collection, affiliation, reg, verbose=0):
    """
    Update an existing affiliation with SNIES identifiers and basic public metadata.
    """
    affiliation = apply_snies_data(affiliation, reg)
    collection.update_one(
        {"_id": affiliation["_id"]},
        {
            "$set": {
                "updated": affiliation["updated"],
                "names": affiliation["names"],
                "external_ids": affiliation["external_ids"],
                "types": affiliation["types"],
                "addresses": affiliation["addresses"],
                "external_urls": affiliation["external_urls"],
                "abbreviations": affiliation["abbreviations"],
                "status": affiliation["status"],
                "ranking": affiliation["ranking"],
            }
        },
    )
    if verbose > 4:
        print(f"Updated SNIES affiliation {affiliation['_id']}")


def insert_affiliation(collection, empty_affiliation, reg, verbose=0):
    """
    Insert a new affiliation from one raw SNIES institution record.
    """
    code = normalize_snies_code(get_first(reg, ["CÓDIGO_INSTITUCIÓN", "CODIGO_INSTITUCION"]))
    if not code:
        return

    entry = apply_snies_data(deepcopy(empty_affiliation), reg)
    entry["_id"] = f"snies_{code}"
    collection.insert_one(entry)

    if verbose > 4:
        print(f"Inserted SNIES affiliation {entry['_id']}")


def process_one(reg, collection, empty_affiliation, verbose=0):
    """
    Process one raw SNIES institution record.

    Matching order:
    1. Exact match by SNIES institution code in external_ids.
    2. Fuzzy/text match by institution name.
    3. Insert as a new affiliation when no match is found.
    """
    code = normalize_snies_code(get_first(reg, ["CÓDIGO_INSTITUCIÓN", "CODIGO_INSTITUCION"]))
    name = snies_title(get_first(reg, ["NOMBRE_INSTITUCIÓN", "NOMBRE_INSTITUCION"]))

    if not code and not name:
        return "omitidos"
    # Official sectionals are compact metadata of the principal institution;
    # link_snies_hierarchy embeds them after every principal has been loaded.
    if is_snies_sectional(reg):
        return "seccionales"

    affiliation = find_by_snies_code(collection, code)
    if not affiliation and not is_snies_sectional(reg):
        affiliation = find_by_name(
            collection, name, code=code, verbose=verbose)

    if affiliation:
        update_affiliation(collection, affiliation, reg, verbose=verbose)
        return "actualizados"
    insert_affiliation(collection, empty_affiliation, reg, verbose=verbose)
    return "nuevos"


def link_snies_hierarchy(collection, records):
    """Embed each official SNIES sectional in its principal institution."""
    return sum(embed_snies_campus(collection, reg) for reg in records)


def repair_snies_code_fusions(collection, records, verbose=0):
    """Remove unrelated SNIES codes previously merged into a ROR affiliation."""
    names_by_code = {}
    records_by_code = {}
    for reg in records:
        code = normalize_snies_code(get_first(
            reg, ["CÓDIGO_INSTITUCIÓN", "CODIGO_INSTITUCION"]))
        name = snies_title(get_first(
            reg, ["NOMBRE_INSTITUCIÓN", "NOMBRE_INSTITUCION"]))
        if code and name:
            names_by_code.setdefault(code, set()).add(name)
            records_by_code.setdefault(code, []).append(reg)

    repaired = set()
    query = {
        "external_ids": {"$elemMatch": {"source": SOURCE}},
        "types.type": {"$ne": "group"},
    }
    for affiliation in collection.find(query):
        codes = {
            str(item.get("id"))
            for item in affiliation.get("external_ids", [])
            if item.get("source") == SOURCE
            and str(item.get("id")) in names_by_code
        }
        if len(codes) < 2:
            continue
        canonical = [
            distinctive_name_tokens(item.get("name"))
            for item in affiliation.get("names", [])
            if item.get("source") == "ror" and item.get("name")
        ]
        canonical = [tokens for tokens in canonical if tokens]
        if not canonical:
            continue
        retained = {
            code for code in codes
            if any(
                distinctive_name_tokens(name) in canonical
                for name in names_by_code[code]
            )
        }
        if not retained or retained == codes:
            continue

        entry = deepcopy(affiliation)
        for field in [
                "updated", "names", "abbreviations", "types", "status",
                "addresses", "external_urls", "external_ids", "ranking"]:
            entry.setdefault(field, [])
        entry["names"] = [
            item for item in entry.get("names", [])
            if item.get("source") != SOURCE
        ]
        for field in ["external_ids", "external_urls", "types", "ranking"]:
            entry[field] = [
                item for item in entry.get(field, [])
                if item.get("provenance") != SOURCE
            ]
        entry["updated"] = [
            item for item in entry.get("updated", [])
            if item.get("source") != SOURCE
        ]
        source_acronyms = {
            acronym
            for code in codes
            for reg in records_by_code[code]
            for acronym in strip_snies_acronyms(get_first(
                reg, ["NOMBRE_INSTITUCIÓN", "NOMBRE_INSTITUCION"]))[1]
        }
        entry["abbreviations"] = [
            item for item in entry.get("abbreviations", [])
            if item not in source_acronyms
        ]
        for code in sorted(retained):
            for reg in records_by_code[code]:
                apply_snies_data(entry, reg)
        collection.update_one(
            {"_id": affiliation["_id"]},
            {"$set": {
                "updated": entry["updated"],
                "names": entry["names"],
                "types": entry["types"],
                "external_urls": entry["external_urls"],
                "external_ids": entry["external_ids"],
                "ranking": entry["ranking"],
                "abbreviations": entry["abbreviations"],
                "addresses": entry["addresses"],
                "status": entry["status"],
            }},
        )
        repaired.add(affiliation["_id"])
        if verbose > 0:
            print(
                "Repaired SNIES affiliation {}: retained {} code(s), separated {}".format(
                    affiliation["_id"], len(retained), len(codes - retained)),
                flush=True,
            )
    return repaired


class Kahi_snies_affiliations(KahiBase):
    """
    Kahi plugin to insert or update affiliations from raw SNIES institution records.
    """

    config = {}

    def __init__(self, config):
        self.config = config
        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("campuses.external_ids.id")
        self.collection.create_index("names.name")
        self.collection.create_index("types.type")
        self.collection.create_index([("names.name", TEXT)])

        self.snies_client = MongoClient(config["snies_affiliations"]["database_url"])
        source_db_name = config["snies_affiliations"]["database_name"]
        source_collection_name = config["snies_affiliations"]["collection_name"]

        if source_db_name not in self.snies_client.list_database_names():
            raise Exception(
                "Database {} not found in {}".format(
                    source_db_name, config["snies_affiliations"]["database_url"]
                )
            )

        self.snies_db = self.snies_client[source_db_name]
        if source_collection_name not in self.snies_db.list_collection_names():
            raise Exception(
                "Collection {}.{} not found in {}".format(
                    source_db_name,
                    source_collection_name,
                    config["snies_affiliations"]["database_url"],
                )
            )

        self.snies_collection = self.snies_db[source_collection_name]
        self.verbose = config["snies_affiliations"].get("verbose", 0)

        self.snies_collection.create_index("CÓDIGO_INSTITUCIÓN")
        self.snies_collection.create_index("NOMBRE_INSTITUCIÓN")
        self.client.close()

    def process_snies(self):
        """
        Process all raw SNIES institution records from the configured source collection.
        """
        institutions = list(self.snies_collection.find())
        progress = ProgressLogger("snies_affiliations", total=len(institutions)).start(
            "origen={}.{}".format(
                self.config["snies_affiliations"]["database_name"],
                self.config["snies_affiliations"]["collection_name"],
            )
        )

        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            collection = db["affiliations"]
            repaired = repair_snies_code_fusions(
                collection, institutions, verbose=self.verbose)

            ordered = sorted(institutions, key=is_snies_sectional)
            for reg in ordered:
                progress.step(process_one(
                    reg, collection, self.empty_affiliation(), self.verbose))
            linked = link_snies_hierarchy(collection, ordered)
        progress.done(fusiones_reparadas=len(repaired), seccionales_enlazadas=linked)

    def run(self):
        self.process_snies()
        self.snies_client.close()
        return 0
