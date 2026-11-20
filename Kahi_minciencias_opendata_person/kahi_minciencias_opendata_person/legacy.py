from kahi.KahiBase import KahiBase
from kahi.progress import ProgressLogger
from pymongo import ASCENDING, DESCENDING, MongoClient, TEXT
from time import time
from re import search, sub
from joblib import Parallel, delayed
from kahi_impactu_utils.Utils import get_id_from_url, get_id_type_from_url, parse_sex, check_date_format, split_names, doi_processor
from bs4 import BeautifulSoup
from unicodedata import normalize as unicode_normalize
import html as ihtml
import re


def ensure_query_index(collection, keys):
    """Reuse an existing plain index even if it has a custom name."""
    for index in collection.list_indexes():
        if (list(index["key"].items()) == keys
                and not index.get("partialFilterExpression")
                and not index.get("sparse")
                and not index.get("hidden")):
            return index["name"]
    return collection.create_index(keys)


def parse_ids(product_id, regex, values):
    """
    depending of the product type, the id is parsed in different ways. This function is used to parse the id of the product
    to extract the different ids that are used in the scienti database.

    Parameters
    ----------
    product_id : str
        The id of the product.
    regex : str
        The regex to be used to parse the id.
    values : list
        The values to be extracted from the id.
    """
    match = search(regex, product_id)
    ids = {}
    if match:
        for i, value in enumerate(values):
            ids[value] = match.group(i + 1)
    return ids


DOI_RE = re.compile(r"\b10\.\d{4,9}/[^\s<>\"{}|\\^`\[\]]+", re.IGNORECASE)
YEAR_RE = re.compile(r"(?<!\d)(19\d{2}|20\d{2})(?!\d)")
ISSN_RE = re.compile(
    r"\bISSN\s*:\s*([\dXx]{4}-?[\dXx]{3}[\dXx])",
    re.IGNORECASE)
ISBN_RE = re.compile(
    r"\bISBN\s*:\s*([0-9Xx](?:[0-9Xx]-?){8,16}[0-9Xx])",
    re.IGNORECASE)

ALLOWED_CVLAC_SECTIONS = {
    "trabajos dirigidos/tutorias": "Trabajos dirigidos/tutorías",
    "articulos": "Artículos",
    "libros": "Libros",
    "capitulos de libro": "Capitulos de libro",
    "libros de divulgacion y/o compilacion de divulgacion": "Libros de divulgación y/o Compilación de divulgación",
    "libro de formacion": "Libro de Formación",
    "publicaciones editoriales no especializadas": "Publicaciones editoriales no especializadas",
    "textos en publicaciones no cientificas": "Publicaciones editoriales no especializadas",
    "patentes": "Patentes",
}

LABEL_ALIASES = {
    "nombre del producto": "product_name",
    "nombre del libro": "book_name",
    "fecha de presentacion": "presentation_date",
    "palabras": "keywords",
    "areas": "areas",
    "sectores": "sectors",
    "estado": "status",
    "dirigio como": "advisor_role",
    "persona(s) orientada(s)": "oriented_people",
    "tutor(es)/cotutor(es)": "tutors",
    "isbn": "isbn_label",
    "institucion": "institution",
    "via de solicitud": "request_route",
    "nombre del solicitante de la patente": "patent_applicant",
    "gaceta industrial de publicacion": "industrial_publication_gazette",
}
LABEL_ORDER = sorted(LABEL_ALIASES, key=len, reverse=True)
LABEL_RE = re.compile(r"(?P<label>" + "|".join(re.escape(x)
                      for x in LABEL_ORDER) + r")\s*:", re.IGNORECASE)

KNOWN_INLINE_TYPES = [
    "Otro capítulo de libro publicado",
    "Capítulo de libro",
    "Libro resultado de investigación",
    "Libros de divulgación y/o Compilación de divulgación",
    "Libros de formación",
]
KNOWN_INLINE_TYPES_RE = re.compile(
    r"\bTipo\s*:\s*(?:" +
    "|".join(
        re.escape(item) for item in KNOWN_INLINE_TYPES) +
    r")\s*",
    re.IGNORECASE,
)
RECOGNIZED_DOCUMENT_TYPES = {
    "cedula de ciudadania": "Cédula de Ciudadanía",
    "cedula de extranjeria": "Cédula de Extranjería",
    "pasaporte": "Pasaporte",
    "tarjeta de identidad": "Tarjeta de Identidad",
}
DOCUMENT_ID_SOURCES = {
    "Cédula de Ciudadanía", "Cédula de Extranjería",
    "Pasaporte", "Passport", "Tarjeta de Identidad",
}


def repair_unicode_text(value: str) -> str:
    """Return text that can always be encoded as UTF-8 by PyMongo."""
    if not any(0xD800 <= ord(char) <= 0xDFFF for char in value):
        return value
    return value.encode("utf-16", "surrogatepass").decode("utf-16", "replace")


def sanitize_mongo_value(value):
    """Recursively repair strings before sending a payload to MongoDB."""
    if isinstance(value, str):
        return repair_unicode_text(value)
    if isinstance(value, dict):
        return {
            repair_unicode_text(key) if isinstance(
                key,
                str) else key: sanitize_mongo_value(item) for key,
            item in value.items()}
    if isinstance(value, list):
        return [sanitize_mongo_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(sanitize_mongo_value(item) for item in value)
    return value


def norm_text(value: str) -> str:
    value = repair_unicode_text(
        ihtml.unescape(
            value or "")).replace(
        "\xa0",
        " ")
    value = unicode_normalize("NFKD", value)
    value = "".join(ch for ch in value if not re.match(r"[\u0300-\u036f]", ch))
    value = value.lower().strip()
    return re.sub(r"\s+", " ", value)


def clean_text(value: str) -> str:
    value = repair_unicode_text(
        ihtml.unescape(
            value or "")).replace(
        "\xa0",
        " ")
    return re.sub(r"\s+", " ", value).strip()


def strip_value(value: str) -> str:
    return clean_text(value).strip(" ,.;:-")


def title_key(value: str) -> str:
    value = norm_text(value)
    return re.sub(r"[^a-z0-9]+", " ", value).strip()


def normalized_document(value):
    value = clean_text(str(value or ""))
    if value.endswith(".0") and value[:-2].isdigit():
        value = value[:-2]
    return value


def recognized_document_source(value):
    return RECOGNIZED_DOCUMENT_TYPES.get(title_key(value))


def compatible_person_name(left, right):
    left_tokens = set(title_key(left).split())
    right_tokens = set(title_key(right).split())
    if not left_tokens or not right_tokens:
        return False
    return left_tokens == right_tokens or (
        len(left_tokens & right_tokens) >= 2
        and (left_tokens <= right_tokens or right_tokens <= left_tokens)
    )


def uniq_keep_order(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        value = strip_value(value)
        if not value:
            continue
        key = norm_text(value)
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def split_terms(value: str) -> list[str]:
    parts = re.split(r"\s*,\s*|\s*;\s*", value or "")
    return uniq_keep_order([p for p in parts if len(strip_value(p)) > 1])


def split_areas(value: str) -> list[str]:
    value = re.split(
        r"\bsectores\s*:",
        value or "",
        maxsplit=1,
        flags=re.IGNORECASE)[0]
    area_paths = re.sub(
        r"\s*,\s*(?=(?:ciencias|humanidades|ingenieria|ingeniería)\b)",
        "|||",
        value,
        flags=re.IGNORECASE,
    ).split("|||")

    levels = []
    for area_path in area_paths:
        levels.extend(re.split(r"\s*--\s*", area_path))
    return uniq_keep_order(
        [level for level in levels if len(strip_value(level)) > 1])


def split_people(value: str) -> list[str]:
    parts = re.split(r"\s*,\s*|\s+;\s+", value or "")
    return uniq_keep_order([p for p in parts if len(strip_value(p)) > 2])


def normalize_isbn(value: str) -> str:
    match = re.search(r"[0-9Xx](?:[0-9Xx]-?){8,16}[0-9Xx]", value or "")
    return match.group(0).upper() if match else ""


def normalize_doi(value: str) -> str:
    doi = doi_processor(value.strip().rstrip(".,;:)]}"))
    return doi or ""


def extract_valid_dois(text: str) -> list[str]:
    dois = []
    for candidate in DOI_RE.findall(text or ""):
        doi = normalize_doi(candidate)
        if doi:
            dois.append(doi)
    return uniq_keep_order(dois)


def clean_inline_type_prefix(value: str) -> str:
    return strip_value(KNOWN_INLINE_TYPES_RE.sub("", value or ""))


def extract_inline_type(text: str) -> str:
    for item in KNOWN_INLINE_TYPES:
        pattern = r"(?:^|\b)Tipo\s*:\s*" + re.escape(item) + r"\b"
        if re.search(pattern, text, flags=re.IGNORECASE):
            return item
    match = re.search(
        r"(?:^|\b)Tipo\s*:\s*([^,.;\n]{2,80})",
        text,
        flags=re.IGNORECASE)
    return strip_value(match.group(1)) if match else ""


def first_person_before_comma(text: str) -> list[str]:
    if "," not in text:
        return []
    first = clean_inline_type_prefix(text.split(",", 1)[0])
    return split_people(first)


def parse_labeled_fields(text: str) -> dict:
    original = clean_text(text)
    text_n = norm_text(original)
    matches = list(LABEL_RE.finditer(text_n))
    fields = {}

    for i, match in enumerate(matches):
        canonical = LABEL_ALIASES.get(norm_text(match.group("label")))
        if not canonical:
            continue
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text_n)
        value = strip_value(original[start:end])
        if value:
            fields[canonical] = value
    return fields


def extract_product_type(section_title: str, blockquote) -> str:
    if norm_text(section_title) == "patentes":
        return "Patente"

    parent_tr = blockquote.find_parent("tr")
    if parent_tr:
        previous_tr = parent_tr.find_previous_sibling("tr")
        if previous_tr and not previous_tr.find("blockquote"):
            b = previous_tr.find("b")
            if b:
                candidate = clean_text(b.get_text(" ", strip=True))
                if candidate and not candidate.endswith(":"):
                    return candidate

    detail_text = clean_text(blockquote.get_text(" ", strip=True))
    inline_type = extract_inline_type(detail_text)
    return inline_type or section_title


def classify_type_impactu(section_title: str, product_type: str) -> str:
    section_n = norm_text(section_title)
    product_n = norm_text(product_type)
    blob_n = f"{section_n} | {product_n}"

    if section_n == "trabajos dirigidos/tutorias":
        if any(
            token in blob_n for token in [
                "tesis de doctorado",
                "trabajo de grado de maestria",
                "especialidad clinica"]):
            return "Tesis de posgrado"
        if any(
            token in blob_n for token in [
                "trabajos de grado de pregrado",
                "tesis de pregrado"]):
            return "Tesis de pregrado"
        return "Trabajos dirigidos/tutorías"
    if section_n == "articulos":
        return "Artículo de revista"
    if section_n == "capitulos de libro":
        return "Capítulo de libro"
    if section_n in {
        "libros",
        "libros de divulgacion y/o compilacion de divulgacion",
            "libro de formacion"}:
        return "Libro"
    if section_n in {
        "publicaciones editoriales no especializadas",
            "textos en publicaciones no cientificas"}:
        return "Publicaciones editoriales no especializadas"
    if section_n == "patentes":
        return "Patente"
    return section_title


def normalize_advisor_role(value: str) -> str:
    value_n = norm_text(value)
    if value_n in {"tutor principal", "coturor/asesor", "cotutor/asesor"}:
        return "advisor"
    return strip_value(value)


def extract_patent_title(text: str) -> str:
    head = re.split(
        r"\bInstitución\s*:",
        text,
        maxsplit=1,
        flags=re.IGNORECASE)[0]
    head = strip_value(head)
    title = head.split(" - ", 1)[1] if " - " in head else head
    title = re.sub(
        r"^\([A-Z0-9 /._-]+(?:\s*\([A-Z0-9 /._-]+\))?\)\s*-?\s*",
        "",
        title,
        flags=re.IGNORECASE)
    return strip_value(title)


def extract_patent_applicant(text: str, fields: dict) -> list[str]:
    match = re.search(
        r"Nombre del solicitante de la patente\s*:\s*(.*?)(?:,\s*\.\s*Gaceta|\.\s*Gaceta|$)",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return split_people(match.group(1))
    return split_people(str(fields.get("patent_applicant", "")))


def extract_title(text: str, fields: dict, section_title: str = "") -> str:
    if norm_text(section_title) == "patentes":
        return extract_patent_title(text)

    for key in ["product_name", "book_name"]:
        if fields.get(key):
            return strip_value(str(fields[key]))

    quoted = re.search(r'"([^"\n]{3,500})"', text)
    if quoted:
        return strip_value(quoted.group(1))

    if "Estado:" in text and "," in text:
        after_first_author = text.split(",", 1)[1]
        title_part = re.split(
            r"\s+(?:UNIVERSIDAD|Universidad|FUNDACION|Fundación|Fundacion|COLEGIO|Colegio)\b|\s+Estado\s*:",
            after_first_author,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        title = strip_value(title_part)
        if title:
            return title

    plain_match = re.search(
        r"^(?:[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ'´`\-]+(?:\s+|,\s*)){2,},?\s*(?P<title>.+?)\s+"
        r"(?:UNIVERSIDAD|Universidad|Estado\s*:|Finalidad\s*:|Nombre comercial\s*:|\.\s*En\s*:)",
        text,
    )
    if plain_match:
        return strip_value(plain_match.group("title"))
    return ""


def extract_authors(
        text: str,
        title: str,
        fields: dict,
        product_type: str,
        section_title: str = "") -> list[str]:
    if norm_text(section_title) == "patentes":
        return extract_patent_applicant(text, fields)

    if fields.get("product_name") or fields.get("book_name"):
        return []

    if norm_text(product_type).startswith("trabajos dirigidos/tutorias"):
        return first_person_before_comma(text)

    if '"' in text:
        prefix = clean_inline_type_prefix(text.split('"', 1)[0])
        return split_people(prefix)

    prefix = re.split(
        r"\b(?:Estado|Finalidad|Nombre comercial|\.\s*En|Tipo de trabajo presentado)\s*:",
        text,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    prefix = clean_inline_type_prefix(prefix)
    if title and title in prefix:
        prefix = prefix.split(title, 1)[0]
    return split_people(prefix)


def extract_year(text: str, fields: dict):
    presentation_date = str(fields.get("presentation_date", ""))
    match = re.search(r"(19\d{2}|20\d{2})", presentation_date)
    if match:
        return int(match.group(1))

    main_text = re.split(
        r"\bDOI\s*:",
        text,
        maxsplit=1,
        flags=re.IGNORECASE)[0]
    priority_patterns = [
        r",\s*(19\d{2}|20\d{2})(?:-\d{2}-\d{2})?\s+[0-9:.]+\s*,",
        r",\s*(19\d{2}|20\d{2})\s*,?\s*(?:Palabras\s*:|Areas\s*:|Sectores\s*:|$)",
        r",\s*(19\d{2}|20\d{2})\s*,?\s*$",
        r",\s*(19\d{2}|20\d{2})\s*\.\s*(?:Dirigió|Dirigio|Areas\s*:|Sectores\s*:|$)",
        r"\b(19\d{2}|20\d{2})\.\s*(?:ed\s*:|pags?\.|Palabras\s*:|Areas\s*:|Sectores\s*:|$)",
    ]
    for pattern in priority_patterns:
        matches = re.findall(pattern, main_text, flags=re.IGNORECASE)
        if matches:
            return int(matches[-1])

    years = [int(year) for year in YEAR_RE.findall(main_text)]
    plausible_years = [year for year in years if 1950 <= year <= 2100]
    return plausible_years[-1] if plausible_years else (
        years[-1] if years else None)


def parse_cvlac_blockquote(
        profile_id: str,
        section_title: str,
        blockquote) -> dict:
    text = clean_text(blockquote.get_text(" ", strip=True))
    fields = parse_labeled_fields(text)
    product_type = extract_product_type(section_title, blockquote)
    title = extract_title(text, fields, section_title)

    return {
        "profile_id": profile_id,
        "product_type": product_type,
        "type_impactu": classify_type_impactu(section_title, product_type),
        "title": title,
        "authors": extract_authors(text, title, fields, product_type, section_title),
        "year": extract_year(text, fields),
        "keywords": split_terms(str(fields.get("keywords", ""))),
        "areas": split_areas(str(fields.get("areas", ""))),
        "advisor_role": normalize_advisor_role(str(fields.get("advisor_role", ""))),
        "oriented_people": split_people(str(fields.get("oriented_people", ""))),
        "doi": extract_valid_dois(text),
        "issn": uniq_keep_order([match.upper() for match in ISSN_RE.findall(text)]),
        "isbn": uniq_keep_order(
            [normalize_isbn(match) for match in ISBN_RE.findall(text)]
            + ([normalize_isbn(str(fields["isbn_label"]))] if fields.get("isbn_label") else [])
        ),
    }


def cvlac_record_to_related_work(record: dict, doi=None) -> dict:
    source = "doi" if doi else "cvlac_stage_raw"
    related_id = doi if doi else {
        "title": title_key(
            record.get(
                "title",
                "")),
        "year": record.get("year")}
    related_work = {
        "provenance": "minciencias",
        "source": source,
        "id": related_id,
        "product_type": record.get("product_type", ""),
        "type_impactu": record.get("type_impactu", ""),
        "title": record.get("title", ""),
        "authors": record.get("authors", []),
        "year": record.get("year"),
        "keywords": record.get("keywords", []),
        "areas": record.get("areas", []),
        "advisor_role": record.get("advisor_role", ""),
        "oriented_people": record.get("oriented_people", []),
        "doi": record.get("doi", []),
        "issn": record.get("issn", []),
        "isbn": record.get("isbn", []),
    }
    return related_work


def freeze_identity(value):
    if isinstance(value, dict):
        return tuple(sorted((key, freeze_identity(item))
                     for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(freeze_identity(item) for item in value)
    return value


def related_work_key(related_work: dict) -> tuple:
    source = related_work.get("source", "")
    identifier = related_work.get("id")
    if identifier not in (None, "", {}, []):
        if source == "doi" and isinstance(identifier, str):
            identifier = identifier.lower()
        return ("id", source, freeze_identity(identifier))
    title = related_work.get("title") or ""
    if title:
        return ("title", title_key(title))
    return ("source", source)


def merge_related_work_metadata(existing: dict, enriched: dict) -> None:
    """
    Enrich an already inserted related_work in-place without changing its identity.
    This is mainly for previous DOI-only records created by older plugin runs.
    """
    for key, value in enriched.items():
        if key in {"provenance", "source", "id"}:
            continue
        if value and not existing.get(key):
            existing[key] = value


def add_related_work(entry: dict, new_item: dict) -> dict:
    new_key = related_work_key(new_item)
    for related_work in entry["related_works"]:
        if related_work_key(related_work) == new_key:
            return related_work
    entry["related_works"].append(new_item)
    return new_item


def ensure_doi_related_work_aliases(entry: dict, dois: list[str]) -> None:
    """Keep DOI identities discoverable by unicity after title-based enrichment."""
    for value in dois:
        doi = normalize_doi(value)
        if doi:
            add_related_work(
                entry,
                {"provenance": "minciencias", "source": "doi", "id": doi},
            )


def get_works_by_id(
        author_id,
        cvlac_html_profiles,
        group_product_names=None,
        existing_doi_ids=None) -> list[dict]:
    """
    Extract structured CVLAC HTML works for an author.

    Works with DOI keep the historical shape {'provenance', 'source': 'doi', 'id': doi}
    and add metadata. Works without DOI are added as source 'cvlac_stage_raw'.
    Title-based deduplication against groups_production is handled later in
    process_info_from_works, where the scienti related_work object is available.
    """
    doc = cvlac_html_profiles.find_one({"_id": author_id})
    if not doc or "html" not in doc:
        return []

    soup = BeautifulSoup(doc.get("html") or "", "html.parser")
    related_works = []
    seen = set()
    for blockquote in soup.find_all("blockquote"):
        text = clean_text(blockquote.get_text(" ", strip=True))
        if len(text) < 20:
            continue

        section_h3 = blockquote.find_previous("h3")
        section_title = clean_text(
            section_h3.get_text(
                " ", strip=True)) if section_h3 else ""
        canonical_section = ALLOWED_CVLAC_SECTIONS.get(
            norm_text(section_title))
        if not canonical_section:
            continue

        record = parse_cvlac_blockquote(author_id, section_title, blockquote)
        title_norm = title_key(record.get("title", ""))
        if not title_norm:
            continue

        dois = record.get("doi") or []
        # Legacy mode retains only DOI evidence for person unification.  The
        # full products are ingested by the entity-specific plugins.
        candidates = [
            cvlac_record_to_related_work(record, doi) for doi in dois]

        for related_work in candidates:
            key = related_work_key(related_work)
            if key in seen:
                continue
            seen.add(key)
            related_works.append(related_work)
    return related_works


def process_info_from_works(
        db,
        author,
        entry,
        groups_production,
        cvlac_html_dois):
    # Load only this author's latest occurrence of each product.  The former
    # implementation materialized all products and scanned them for every
    # person, which was both memory-heavy and quadratic.
    papers = []
    seen_products = set()
    cursor = groups_production.find({
        "id_persona_pd": author["id_persona_pr"]
    }).sort([("id_producto_pd", ASCENDING), ("ano_convo", DESCENDING)])
    for product in cursor:
        product_id = product.get("id_producto_pd")
        if product_id in seen_products:
            continue
        seen_products.add(product_id)
        papers.append(product)
    group_related_works_by_title = {}

    def add_group_related_work(reg, new_item):
        related_work = add_related_work(entry, new_item)
        title_norm = title_key(reg.get("nme_producto_pd", ""))
        if title_norm:
            group_related_works_by_title.setdefault(title_norm, related_work)
        return related_work

    if papers:
        groups_cod = []
        inst_cod = []
        for reg in papers:
            if reg["cod_grupo_gr"] in groups_cod:
                continue
            groups_cod.append(reg["cod_grupo_gr"])
            group_db = db["affiliations"].find_one(
                {"external_ids.id": reg["cod_grupo_gr"]})
            if group_db:
                name = group_db["names"][0]["name"]
                for n in group_db["names"]:
                    if n["lang"] == "es":
                        name = n["name"]
                        break
                    elif n["lang"] == "en":
                        name = n["name"]
                aff = {
                    "name": name,
                    "id": group_db["_id"],
                    "types": group_db["types"],
                    "start_date": check_date_format(reg["fcreacion_pd"]),
                    "end_date": "",
                    "position": ""
                }
                found = False
                for i in entry["affiliations"]:
                    if i["id"] == aff["id"]:
                        found = True
                        break
                if not found:
                    entry["affiliations"].append(aff)
                if "relations" in group_db.keys():
                    if group_db["relations"]:
                        for rel in group_db["relations"]:
                            if rel["id"] in inst_cod:
                                continue
                            inst_cod.append(rel["id"])
                            if "names" in rel.keys():
                                name = rel["names"][0]["name"]
                                for n in rel["names"]:
                                    if n["lang"] == "es":
                                        name = n["name"]
                                        break
                                    elif n["lang"] == "en":
                                        name = n["name"]
                            else:
                                name = rel["name"]
                            aff = {
                                "name": name,
                                "id": rel["id"],
                                "types": rel["types"] if "types" in rel.keys() else [],
                                # A group's product date does not establish an
                                # author's institutional affiliation interval.
                                "start_date": None,
                                "end_date": -1,
                                "years": [],
                                "position": ""}

                            found = False
                            for i in entry["affiliations"]:
                                if i["id"] == aff["id"]:
                                    found = True
                                    break
                            if not found:
                                entry["affiliations"].append(aff)

    # Extract related works from cvlac_html_profiles.
    # When a CVLAC HTML title matches groups_production, enrich the scienti
    # related_work itself instead of creating a duplicate DOI/HTML work.
    cvlac_works_with_dois = get_works_by_id(
        author["id_persona_pr"], cvlac_html_dois)
    if cvlac_works_with_dois:
        existing_by_key = {
            related_work_key(work): work
            for work in entry["related_works"]
        }
        for related_work in cvlac_works_with_dois:
            title_norm = title_key(related_work.get("title", ""))
            group_related_work = group_related_works_by_title.get(title_norm)
            if group_related_work:
                merge_related_work_metadata(group_related_work, related_work)
                duplicate = existing_by_key.get(related_work_key(related_work))
                if duplicate and duplicate is not group_related_work:
                    merge_related_work_metadata(group_related_work, duplicate)
                    if duplicate in entry["related_works"]:
                        entry["related_works"].remove(duplicate)
                ensure_doi_related_work_aliases(
                    entry, related_work.get("doi", [])
                )
                continue

            key = related_work_key(related_work)
            if key in existing_by_key:
                merge_related_work_metadata(existing_by_key[key], related_work)
            else:
                entry["related_works"].append(related_work)
                existing_by_key[key] = related_work


def cvlac_profile_from_html(document):
    """Build the legacy profile subset directly from the immutable raw HTML."""
    if not document or not document.get("html"):
        return None
    profile_id = str(document.get("_id") or "")
    soup = BeautifulSoup(document["html"], "html.parser")
    anchor = soup.find("a", {"name": "datos_generales"})
    if not anchor:
        return None
    table = anchor.find_next("table")
    if not table:
        return None
    general = {"Nombre": ""}
    for row in table.find_all("tr"):
        cells = [clean_text(cell.get_text(" ", strip=True))
                 for cell in row.find_all(["th", "td"])]
        if len(cells) >= 2 and cells[0]:
            general[cells[0]] = cells[1]
    identifiers = {}
    for anchor_name in ("redes_identificadores", "red_identificadores"):
        section = soup.find("a", {"name": anchor_name})
        id_table = section.find_next("table") if section else None
        if id_table:
            for link in id_table.find_all("a", href=True):
                identifiers[clean_text(link.get_text(" ", strip=True))] = link["href"]
    return {
        "id_persona_pr": profile_id,
        "datos_generales": general,
        "red_identificadores": identifiers,
        "redes_identificadores": identifiers,
    }


def process_one(
        author_entry,
        db,
        collection,
        empty_person,
        cvlac_profile,
        groups_production,
        privates,
        cvlac_html_dois,
        verbose):

    if not author_entry:
        return

    # If the author is a dictionary, it is a private profile, otherwise it is
    # a cvlac_profile.
    authors = [author_entry] if privates else author_entry
    works_processed = False

    # Iterate over the authors
    for author in authors:
        # Define the author as a dictionary if it is not to permit the use of
        # the same function for the cvlac_profile and the private_profiles.
        author = author if isinstance(author, dict) else {
            "id_persona_pr": author}

        reg_db = collection.find_one(
            {"external_ids.id.COD_RH": author["id_persona_pr"]})

        if reg_db:
            # Updated
            sources = [x["source"] for x in reg_db["updated"]]
            if "minciencias" not in sources:
                reg_db["updated"].append({
                    "source": "minciencias",
                    "time": int(time())})

            if cvlac_profile:
                # Identifiers
                ids = set()
                if "red_identificadores" in cvlac_profile.keys():
                    if cvlac_profile["red_identificadores"]:
                        for rid in cvlac_profile["red_identificadores"].values(
                        ):
                            ids.add(rid)
                if "redes_identificadoes" in cvlac_profile.keys():
                    if cvlac_profile["redes_identificadoes"]:
                        for rid in cvlac_profile["redes_identificadoes"].values(
                        ):
                            ids.add(rid)
                if ids:
                    for _id in list(ids):
                        if isinstance(_id, str):
                            value = get_id_from_url(_id)
                            if value:
                                rec = {
                                    "provenance": "minciencias",
                                    "source": get_id_type_from_url(_id),
                                    "id": value
                                }
                                if rec["id"] not in [x["id"]
                                                     for x in reg_db["external_ids"]]:
                                    if rec not in reg_db["external_ids"]:
                                        reg_db["external_ids"].append(rec)

                general = cvlac_profile.get("datos_generales") or {}
                sex_value = next((value for key, value in general.items()
                                  if clean_text(key).casefold() == "sexo"), None)
                if sex_value and not reg_db.get("sex"):
                    reg_db["sex"] = parse_sex(str(sex_value).lower())

            # Subjects
            if "nme_gran_area_pr" and "nme_area_pr" in author.keys():
                subjects_entry = {"provenance": "minciencias",
                                  "source": "OECD",
                                  "subjects": [{"level": 0,
                                                "name": author["nme_gran_area_pr"],
                                                "id": "",
                                                "external_ids": [{"source": "OECD",
                                                                  "id": author["id_area_con_pr"][0]}]},
                                               {"level": 1,
                                                "name": author["nme_area_pr"],
                                                "id": "",
                                                "external_ids": [{"source": "OECD",
                                                                  "id": author["id_area_con_pr"][1]}]},
                                               ]}
                if subjects_entry not in reg_db["subjects"]:
                    reg_db["subjects"].append(subjects_entry)

            # Ranking
            if "nme_clasificacion_pr" in author.keys():
                entry_rank = {
                    "source": "minciencias",
                    "rank": author["nme_clasificacion_pr"],
                    "id": author["id_clas_pr"],
                    "order": author["orden_clas_pr"],
                    "date": check_date_format(author["ano_convo"])
                }
                if entry_rank not in reg_db["ranking"]:
                    reg_db["ranking"].append(entry_rank)

            # Products are identical across an author's historical convocatorias.
            # Rankings still run for every author row, but products run only
            # once.
            if not works_processed:
                process_info_from_works(
                    db, author, reg_db, groups_production, cvlac_html_dois)
                works_processed = True
            # Update the record
            update_fields = sanitize_mongo_value({
                "updated": reg_db["updated"],
                "external_ids": reg_db["external_ids"],
                "sex": reg_db.get("sex", ""),
                "subjects": reg_db["subjects"],
                "related_works": reg_db["related_works"],
                "ranking": reg_db["ranking"],
                "affiliations": reg_db["affiliations"]
            })
            collection.update_one(
                {"_id": reg_db["_id"]},
                {"$set": update_fields})
            continue

        # Without HTML there is no reliable name for creating a new person.
        # Authors created previously from staff/ScienTI are enriched above.
        if not cvlac_profile:
            continue

        entry = empty_person.copy()
        entry["updated"].append({
            "source": "minciencias",
            "time": int(time())})

        # Author creation
        if cvlac_profile:
            if "datos_generales" in cvlac_profile.keys():
                if "0000000082" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Mujer"
                if "0001385093" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Mujer"
                if "0001506130" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0001393305" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0001353302" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0001165976" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0001437782" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0000287938" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0001511182" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0000037796" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0001386076" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0000346748" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0000327220" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0001317792" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0001103741" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0000059161" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
                if "0000896519" in author["id_persona_pr"]:
                    cvlac_profile["datos_generales"]["Sexo"] = "Hombre"

            entry["external_ids"].append({
                "provenance": "minciencias",
                "source": "scienti",
                "id": {"COD_RH": cvlac_profile["id_persona_pr"]}
            })

            if "datos_generales" in cvlac_profile.keys(
            ) and cvlac_profile["datos_generales"]:
                full_name = sub(
                    r'\s+',
                    ' ',
                    cvlac_profile["datos_generales"]["Nombre"].replace(
                        ".",
                        " ")).strip()
                full_name = split_names(full_name)

                entry["full_name"] = full_name["full_name"]
                entry["first_names"] = full_name["first_names"]
                entry["last_names"] = full_name["last_names"]
                entry["initials"] = full_name["initials"]

            general = cvlac_profile.get("datos_generales") or {}
            sex_value = next((value for key, value in general.items()
                              if clean_text(key).casefold() == "sexo"), None)
            if sex_value:
                entry["sex"] = parse_sex(str(sex_value).lower())

            # all the ids are mixed, so we need to check each one in the next
            # columns
            ids = set()
            if "red_identificadores" in cvlac_profile.keys():
                if cvlac_profile["red_identificadores"]:
                    for rid in cvlac_profile["red_identificadores"].values():
                        ids.add(rid)

            if "redes_identificadoes" in cvlac_profile.keys():
                if cvlac_profile["redes_identificadoes"]:
                    for rid in cvlac_profile["redes_identificadoes"].values():
                        ids.add(rid)

            if ids:
                for _id in list(ids):
                    if isinstance(_id, str):
                        value = get_id_from_url(_id)
                        if value:
                            rec = {
                                "provenance": "minciencias",
                                "source": get_id_type_from_url(_id),
                                "id": value
                            }
                            if rec not in entry["external_ids"]:
                                entry["external_ids"].append(rec)
        # degrees
        # Pending to add the degrees

        # subjects
        if "nme_gran_area_pr" and "nme_area_pr" in author.keys():
            entry["subjects"].append({"provenance": "minciencias",
                                      "source": "OECD",
                                      "subjects": [{"level": 0,
                                                    "name": author["nme_gran_area_pr"],
                                                    "id": "",
                                                    "external_ids": [{"source": "OECD",
                                                                      "id": author["id_area_con_pr"][0]}]},
                                                   {"level": 1,
                                                    "name": author["nme_area_pr"],
                                                    "id": "",
                                                    "external_ids": [{"source": "OECD",
                                                                      "id": author["id_area_con_pr"][1]}]},
                                                   ]})

        # affiliations and related works
        if not works_processed:
            process_info_from_works(
                db, author, entry, groups_production, cvlac_html_dois)
            works_processed = True

        # Ranking
        if "nme_clasificacion_pr" in author.keys():
            entry_rank = {
                "source": "minciencias",
                "rank": author["nme_clasificacion_pr"],
                "id": author["id_clas_pr"],
                "order": author["orden_clas_pr"],
                "date": check_date_format(author["ano_convo"])
            }
            entry["ranking"].append(entry_rank)

        collection.insert_one(sanitize_mongo_value(entry))


_PROCESS_MONGO_RESOURCES = {}


def process_one_multiprocessing(
        author_entry,
        author_id,
        empty_person,
        target_url,
        target_database,
        source_url,
        source_database,
        groups_collection,
        html_collection,
        privates,
        verbose):
    """Run one author with MongoDB connections owned by the worker process."""
    key = (
        target_url, target_database, source_url, source_database,
        groups_collection, html_collection,
    )
    resources = _PROCESS_MONGO_RESOURCES.get(key)
    if resources is None:
        target_client = MongoClient(target_url)
        source_client = MongoClient(source_url)
        target_db = target_client[target_database]
        source_db = source_client[source_database]
        resources = {
            "target_client": target_client,
            "source_client": source_client,
            "target_db": target_db,
            "person": target_db["person"],
            "groups": source_db[groups_collection],
            "html": source_db[html_collection],
        }
        _PROCESS_MONGO_RESOURCES[key] = resources

    html = resources["html"]
    return process_one(
        author_entry,
        resources["target_db"],
        resources["person"],
        empty_person,
        cvlac_profile_from_html(html.find_one({"_id": author_id})),
        resources["groups"],
        privates,
        html,
        verbose,
    )


class Kahi_minciencias_opendata_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        self.openadata_client = MongoClient(
            config["minciencias_opendata_person"]["database_url"])
        if config["minciencias_opendata_person"]["database_name"] not in self.openadata_client.list_database_names():
            raise Exception(
                "Database {} not found in {}".format(
                    config["minciencias_opendata_person"]['database_name'],
                    config["minciencias_opendata_person"]["database_url"]))
        self.openadata_db = self.openadata_client[config["minciencias_opendata_person"]["database_name"]]

        if config["minciencias_opendata_person"]["researchers"] not in self.openadata_db.list_collection_names():
            raise Exception(
                "Collection {} not found in {}".format(
                    config["minciencias_opendata_person"]['researchers'],
                    config["minciencias_opendata_person"]["database_url"]))
        self.researchers_collection = self.openadata_db[
            config["minciencias_opendata_person"]["researchers"]]

        if config["minciencias_opendata_person"]["groups_production"] not in self.openadata_db.list_collection_names():
            raise Exception(
                "Collection {} not found in {}".format(
                    config["minciencias_opendata_person"]['groups_production'],
                    config["minciencias_opendata_person"]["database_url"]))
        self.groups_production = self.openadata_db[config["minciencias_opendata_person"]
                                                   ["groups_production"]]

        if config["minciencias_opendata_person"]["cvlac_html_profiles"] not in self.openadata_db.list_collection_names():
            raise Exception(
                "Collection {} not found in {}".format(
                    config["minciencias_opendata_person"]['cvlac_html_profiles'],
                    config["minciencias_opendata_person"]["database_url"]))
        self.cvlac_html_profiles = self.openadata_db[config["minciencias_opendata_person"]
                                                     ["cvlac_html_profiles"]]

        recognized_name = config["minciencias_opendata_person"].get(
            "recognized_researchers", "recognized_researchers")
        if recognized_name not in self.openadata_db.list_collection_names():
            raise Exception("Collection {} not found in {}".format(
                recognized_name,
                config["minciencias_opendata_person"]["database_url"]))
        self.recognized_researchers = self.openadata_db[recognized_name]

        self.researchers_collection.create_index(
            [("id_persona_pr", ASCENDING)], name="id_persona_pr_1")
        self.groups_production.create_index(
            [("id_persona_pd", ASCENDING), ("id_producto_pd", ASCENDING), ("ano_convo", DESCENDING)],
            name="id_persona_pd_1_id_producto_pd_1_ano_convo_-1",
        )
        self.groups_production.create_index(
            [("id_producto_pd", ASCENDING), ("ano_convo", DESCENDING)],
            name="id_producto_pd_1_ano_convo_-1",
        )
        ensure_query_index(self.recognized_researchers, [("cod_rh", ASCENDING)])
        ensure_query_index(
            self.recognized_researchers, [("documento_normalizado", ASCENDING)])

        self.n_jobs = config["minciencias_opendata_person"]["num_jobs"] if "num_jobs" in config["minciencias_opendata_person"].keys(
        ) else 1
        self.backend = config["minciencias_opendata_person"].get(
            "backend", "threading")
        if self.backend not in {"threading", "loky"}:
            raise ValueError("backend must be 'threading' or 'loky'")

        self.verbose = config["minciencias_opendata_person"][
            "verbose"] if "verbose" in config["minciencias_opendata_person"].keys() else 0
        self.progress_interval = max(1, int(
            config["minciencias_opendata_person"].get("progress_interval", 1000)))

    @staticmethod
    def _document_sources(source):
        if source in {"Pasaporte", "Passport"}:
            return {"Pasaporte", "Passport"}
        return {source} if source else DOCUMENT_ID_SOURCES

    @staticmethod
    def _person_codes(person):
        return {
            str(item.get("id", {}).get("COD_RH") or "")
            for item in person.get("external_ids", [])
            if isinstance(item.get("id"), dict)
        }

    @classmethod
    def _person_has_document(cls, person, document, source=None):
        allowed = cls._document_sources(source)
        return any(
            item.get("source") in allowed
            and normalized_document(item.get("id")) == document
            for item in person.get("external_ids", [])
            if isinstance(item, dict)
        )

    @staticmethod
    def _choose_recognized_person(candidates, name):
        if len(candidates) == 1:
            return candidates[0]
        compatible = [
            person for person in candidates
            if compatible_person_name(name, person.get("full_name"))
        ]
        return compatible[0] if len(compatible) == 1 else None

    def _recognized_candidates(self, code, document, source):
        document_values = [document]
        if document.isdigit():
            document_values.append(int(document))
        query = {"$or": [
            {"external_ids.id.COD_RH": code},
            {"external_ids": {"$elemMatch": {
                "source": {"$in": sorted(DOCUMENT_ID_SOURCES)},
                "id": {"$in": document_values},
            }}},
        ]}
        return list(self.collection.find(query))

    def _enrich_recognized_person(self, record, counters):
        code = str(record.get("cod_rh") or "").strip()
        document = normalized_document(record.get("documento_normalizado"))
        name = clean_text(record.get("nombre_completo"))
        if (not re.fullmatch(r"\d{10}", code) or code == "0000000000"
                or len(document) < 5 or document.casefold() in {
                    "none", "null", "nan", "no registra",
        }):
            counters["invalid"] += 1
            return
        source = recognized_document_source(record.get("tipo_consolidado"))
        candidates = self._recognized_candidates(code, document, source)
        by_code = [item for item in candidates if code in self._person_codes(item)]
        by_document = [
            item for item in candidates
            if self._person_has_document(item, document)
        ]
        code_person = self._choose_recognized_person(by_code, name)
        document_person = self._choose_recognized_person(by_document, name)
        if document_person and (
            (code not in self._person_codes(document_person)
             and not compatible_person_name(
                 name, document_person.get("full_name")))
            or self._person_codes(document_person) - {code}
            or (source and not self._person_has_document(
                document_person, document, source))
        ):
            document_person = None
        document_conflict = bool(by_document) and (
            len(by_document) != 1 or document_person is None
            or (code_person and code_person["_id"] != document_person["_id"])
        )
        if document_conflict:
            counters["identity_conflicts"] += 1
        person = code_person or (None if document_conflict else document_person)
        if not person:
            if (by_code or by_document) or not name:
                counters["ambiguous"] += 1
                return
            person = self.empty_person()
            parsed = split_names(name)
            person["full_name"] = parsed["full_name"]
            person["first_names"] = parsed["first_names"]
            person["last_names"] = parsed["last_names"]
            person["initials"] = parsed["initials"]
            counters["inserted"] += 1
            inserted = True
        else:
            inserted = False
            counters["updated"] += 1

        external_ids = person.setdefault("external_ids", [])
        if code not in self._person_codes(person):
            external_ids.append({
                "provenance": "minciencias", "source": "scienti",
                "id": {"COD_RH": code},
            })
            counters["cod_rh_added"] += 1
        existing_document_sources = {
            item.get("source") for item in external_ids
            if item.get("source") in DOCUMENT_ID_SOURCES
            and normalized_document(item.get("id")) == document
        }
        source_family = self._document_sources(source) if source else set()
        if document_conflict or (
            source and existing_document_sources
            and not existing_document_sources.issubset(source_family)
        ) or (not source and len({
            "Pasaporte" if item in {"Pasaporte", "Passport"} else item
            for item in existing_document_sources
        }) > 1):
            counters["identity_conflicts"] += int(not document_conflict)
            source = None
        elif existing_document_sources:
            source = sorted(existing_document_sources)[0]
        if source and not self._person_has_document(person, document, source):
            external_ids.append({
                "provenance": "minciencias", "source": source,
                "id": document,
            })
            counters["typed_documents_added"] += 1
        elif source:
            counters["typed_documents_confirmed"] += 1
        else:
            counters["documents_without_type"] += 1

        if name:
            person.setdefault("aliases", [])
            if name not in person["aliases"]:
                person["aliases"].append(name)
            if not person.get("full_name"):
                parsed = split_names(name)
                person["full_name"] = parsed["full_name"]
                person["first_names"] = parsed["first_names"]
                person["last_names"] = parsed["last_names"]
                person["initials"] = parsed["initials"]
        now = int(time())
        update = next((item for item in person.setdefault("updated", [])
                       if item.get("source") == "minciencias"), None)
        if update:
            update["time"] = now
        else:
            person["updated"].append({"source": "minciencias", "time": now})
        person = sanitize_mongo_value(person)
        if inserted:
            self.collection.insert_one(person)
        else:
            self.collection.replace_one({"_id": person["_id"]}, person)

    def process_recognized_researchers(self):
        counters = {
            "processed": 0, "inserted": 0, "updated": 0,
            "cod_rh_added": 0, "typed_documents_added": 0,
            "typed_documents_confirmed": 0, "documents_without_type": 0,
            "identity_conflicts": 0, "ambiguous": 0, "invalid": 0,
        }
        total = self.recognized_researchers.count_documents({})
        projection = {
            "cod_rh": 1, "documento_normalizado": 1,
            "tipo_consolidado": 1, "nombre_completo": 1,
        }
        progress = None
        if self.verbose > 0:
            progress = ProgressLogger(
                "minciencias_opendata_person:legacy_recognized",
                total=total, every=self.progress_interval,
            ).start()
        for record in self.recognized_researchers.find({}, projection):
            before = counters["inserted"], counters["updated"]
            self._enrich_recognized_person(record, counters)
            counters["processed"] += 1
            if progress is not None:
                progress.step(
                    "nuevos" if counters["inserted"] > before[0]
                    else "actualizados" if counters["updated"] > before[1]
                    else "omitidos")
        if progress is not None:
            progress.done(**{
                key: value for key, value in counters.items()
                if key not in {"processed", "inserted", "updated"}
            })
        return counters

    def process_openadata(self):

        # Authors aggregate
        if self.verbose > 4:
            print("Creating the aggregate for {} authors.".format(
                self.researchers_collection.count_documents({})))
        pipeline = [
            {"$group": {"_id": "$id_persona_pr", "docs": {"$push": "$$ROOT"}}}
        ]
        cvlac_authors_list = list(self.researchers_collection.aggregate(
            pipeline, allowDiskUse=True))

        cvlac_html_dois = self.cvlac_html_profiles

        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            person_collection = db["person"]
            # Process the authors with cvlac profile
            if self.verbose > 4:
                print("Processing {} authors in cvlac.".format(
                    len(cvlac_authors_list)))
            self._process_parallel(
                ((author["docs"], author["_id"])
                 for author in cvlac_authors_list),
                db, person_collection, False, cvlac_html_dois,
                "legacy_cvlac", len(cvlac_authors_list),
            )
            # Raw HTML replaces both historical cvlac_stage collections.
            cvlac_data_ids = set(
                self.researchers_collection.distinct("id_persona_pr"))
            authors_private_profile_list = [
                value for value in self.cvlac_html_profiles.distinct("_id")
                if value not in cvlac_data_ids
            ]
            if self.verbose > 4:
                print("Processing {} authors found only in raw HTML.".format(
                    len(authors_private_profile_list)))
            self._process_parallel(
                ((author, author) for author in authors_private_profile_list),
                db, person_collection, True, cvlac_html_dois,
                "legacy_html_only", len(authors_private_profile_list),
            )
            # Build and consume this cursor only after the previous stages.
            # Keeping it idle while those stages run makes MongoDB expire it.
            cvlac_data_ids = list(cvlac_data_ids)
            production_author_ids = {
                value for value in self.groups_production.distinct("id_persona_pd")
                if value and value != "0000000000"
            }
            authors_not_cvlac_ids = production_author_ids - set(cvlac_data_ids)
            if authors_not_cvlac_ids:
                # Extract the id_persona_pr id from the
                if self.verbose > 4:
                    print("Processing {} authors not in cvlac.".format(
                        len(authors_not_cvlac_ids)))
                self._process_parallel(
                    ((author, author) for author in authors_not_cvlac_ids),
                    db, person_collection, True, cvlac_html_dois,
                    "legacy_not_in_cvlac", len(authors_not_cvlac_ids),
                )
            client.close()

    def _process_parallel(
            self, authors, db, person_collection, privates, cvlac_html_dois,
            label="legacy", total=None):
        if self.backend == "threading":
            jobs = (
                delayed(process_one)(
                    author_entry,
                    db,
                    person_collection,
                    self.empty_person(),
                    cvlac_profile_from_html(
                        self.cvlac_html_profiles.find_one({"_id": author_id})),
                    self.groups_production,
                    privates,
                    cvlac_html_dois,
                    self.verbose,
                )
                for author_entry, author_id in authors
            )
        else:
            source_config = self.config["minciencias_opendata_person"]
            jobs = (
                delayed(process_one_multiprocessing)(
                    author_entry,
                    author_id,
                    self.empty_person(),
                    self.mongodb_url,
                    self.config["database_name"],
                    source_config["database_url"],
                    source_config["database_name"],
                    self.groups_production.name,
                    self.cvlac_html_profiles.name,
                    privates,
                    self.verbose,
                )
                for author_entry, author_id in authors
            )
        progress = ProgressLogger(
            "minciencias_opendata_person:{}".format(label), total=total,
        ).start("backend={} num_jobs={}".format(self.backend, self.n_jobs))
        # Results are counted in the parent so loky workers need no shared state.
        for _ in Parallel(
            n_jobs=self.n_jobs,
            verbose=0,
            backend=self.backend,
            return_as="generator_unordered",
        )(jobs):
            progress.step()
        progress.done()

    def run(self):
        self.process_openadata()
        self.process_recognized_researchers()
        return 0
