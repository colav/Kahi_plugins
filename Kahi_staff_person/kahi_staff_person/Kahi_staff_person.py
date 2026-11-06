from kahi.KahiBase import KahiBase
from kahi.progress import ProgressLogger
from kahi_impactu_utils.Mongo import order_document
from pymongo import MongoClient, TEXT
from pandas import read_excel, to_datetime, DataFrame
from time import time
from kahi_impactu_utils.Mapping import canonical_ror_url, ciarp_mapping
from kahi_impactu_utils.String import title_case
from kahi_impactu_utils.Utils import parse_sex, doi_processor, split_name_part, parse_orcid_id_from_url
from datetime import datetime as dt
from datetime import datetime, timezone
from copy import deepcopy
import hashlib
import re
import unicodedata


DOCUMENT_TYPES = {
    "cédula de ciudadanía": "Cédula de Ciudadanía",
    "cédula de extranjería": "Cédula de Extranjería",
    "pasaporte": "Pasaporte",
    "COD_RH": "scienti"
}

ORCID_PATTERN = re.compile(
    r"(?<![\dX])\d{4}-\d{4}-\d{4}-[\dX]{4}(?![\dX])", re.IGNORECASE)
COD_RH_PATTERN = re.compile(r"(?<!\d)\d{1,10}(?!\d)")
STAFF_UNIT_COLUMNS = {
    "código_unidad_académica", "unidad_académica",
    "código_subunidad_académica", "subunidad_académica",
}


def has_text(value):
    return value is not None and str(value).strip().lower() not in {
        "", "nan", "nat", "none"}


def normalized_unit_name(value):
    text = unicodedata.normalize("NFKD", str(value or "")).casefold()
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^\w]+", " ", text).split())


def unit_component(code, name):
    """Use the source code, or a stable name fingerprint when it is absent."""
    if has_text(code):
        return str(code).strip()
    if not has_text(name):
        return ""
    normalized = normalized_unit_name(name)
    if not normalized:
        return ""
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]
    return f"name_{digest}"


def staff_faculty_id(institution_id, record):
    component = unit_component(
        record.get("código_unidad_académica"),
        record.get("unidad_académica"),
    )
    return f"{institution_id}_{component}" if component else ""


def staff_department_id(institution_id, record):
    faculty_id = staff_faculty_id(institution_id, record)
    component = unit_component(
        record.get("código_subunidad_académica"),
        record.get("subunidad_académica"),
    )
    return f"{faculty_id}_{component}" if faculty_id and component else ""


def find_staff_unit(collection, unit_id, parent_id, unit_type):
    """Resolve one academic unit by deterministic identity and hierarchy."""
    if not unit_id:
        return None
    return collection.find_one({
        "_id": unit_id,
        "relations.id": parent_id,
        "types": {"$elemMatch": {"source": "staff", "type": unit_type}},
    })


def normalize_orcid(value):
    """Return the canonical ORCID URL when the value contains a valid shape."""
    if not has_text(value):
        return None
    match = ORCID_PATTERN.search(str(value).strip())
    if not match:
        return None
    return parse_orcid_id_from_url(match.group(0))


def normalize_cod_rh(value):
    """Extract a CvLAC COD_RH, preserving its ten-character representation."""
    if not has_text(value):
        return None
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    matches = COD_RH_PATTERN.findall(text)
    if not matches:
        return None
    return matches[-1].zfill(10)


def append_external_id(external_ids, source, identifier):
    if not identifier:
        return
    record = {
        "provenance": "staff",
        "source": source,
        "id": identifier
    }
    if record not in external_ids:
        external_ids.append(record)


def append_staff_external_ids(external_ids, record, person_id):
    """Add the document, ORCID and CvLAC identifiers supplied by staff."""
    doc_type = record.get("tipo_documento")
    if doc_type in DOCUMENT_TYPES:
        identifier = person_id
        if doc_type == "COD_RH":
            identifier = {"COD_RH": normalize_cod_rh(person_id)}
        append_external_id(external_ids, DOCUMENT_TYPES[doc_type], identifier)

    append_external_id(
        external_ids, "orcid", normalize_orcid(record.get("orcid")))
    cod_rh = normalize_cod_rh(record.get("cvlac"))
    if cod_rh:
        append_external_id(external_ids, "scienti", {"COD_RH": cod_rh})

    return doc_type in DOCUMENT_TYPES


def build_ciarp_related_work_map(data, categories):
    """Group valid CIARP DOI references by staff document identifier."""
    related = {}
    allowed = set(categories)
    for _, work in data.iterrows():
        if work.get("ranking") not in allowed:
            continue
        raw_doi = work.get("doi")
        work_doi = doi_processor(str(raw_doi)) if has_text(raw_doi) else None
        person_id = str(work.get("identificación", "")).strip()
        if not work_doi or not person_id:
            continue
        record = {
            "provenance": "ciarp",
            "source": "doi",
            "id": work_doi,
            "year": str(work.get("año", "")),
        }
        records = related.setdefault(person_id, [])
        if record not in records:
            records.append(record)
    return related


def staff_person_query(records, person_id):
    """Build a strict identifier query for one person from staff records."""
    clauses = []
    for record in records:
        external_ids = []
        append_staff_external_ids(external_ids, record, person_id)
        for external_id in external_ids:
            clause = {"external_ids": {"$elemMatch": {
                "source": external_id["source"],
                "id": external_id["id"],
            }}}
            if clause not in clauses:
                clauses.append(clause)
    return {"$or": clauses} if clauses else None


def staff_primary_person_query(records, person_id):
    """Build a query using only the primary document supplied by staff."""
    clauses = []
    for record in records:
        doc_type = record.get("tipo_documento")
        if doc_type not in DOCUMENT_TYPES:
            continue
        identifier = person_id
        if doc_type == "COD_RH":
            identifier = {"COD_RH": normalize_cod_rh(person_id)}
        clause = {"external_ids": {"$elemMatch": {
            "source": DOCUMENT_TYPES[doc_type],
            "id": identifier,
        }}}
        if clause not in clauses:
            clauses.append(clause)
    return {"$or": clauses} if clauses else None


def normalized_full_name(value):
    """Normalize a name for strict disambiguation inside one identifier set."""
    text = unicodedata.normalize("NFKD", str(value or "")).casefold()
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^\w]+", " ", text).split())


def staff_full_name(records):
    """Build the full name exactly as the regular staff ingestion does."""
    if not records:
        return ""
    record = records[0]
    parts = [
        record.get("nombres"),
        record.get("primer_apellido"),
        record.get("segundo_apellido"),
    ]
    return " ".join(str(part).strip() for part in parts if has_text(part))


def merge_staff_person(existing, incoming):
    """Merge staff evidence into an existing person without losing sources."""
    merged = deepcopy(existing)
    for field in ("updated", "external_ids", "aliases", "degrees", "ranking",
                  "related_works"):
        values = merged.setdefault(field, [])
        for value in incoming.get(field, []):
            if value not in values:
                values.append(value)

    affiliations = merged.setdefault("affiliations", [])
    affiliations_by_id = {}
    for item in affiliations:
        affiliations_by_id.setdefault(item.get("id"), item)
    for affiliation in incoming.get("affiliations", []):
        affiliation_id = affiliation.get("id")
        current = affiliations_by_id.get(affiliation_id)
        if current is None:
            affiliations.append(affiliation)
            affiliations_by_id[affiliation_id] = affiliation
            continue
        # Staff supplies the temporal evidence used by CIARP. Enrich the
        # canonical affiliation already contributed by another source.
        for field in ("start_date", "end_date", "years"):
            if field in affiliation:
                current[field] = affiliation[field]
        for field in ("name", "types", "relations"):
            if not current.get(field) and affiliation.get(field):
                current[field] = affiliation[field]

    for field in ("full_name", "first_names", "last_names", "initials",
                  "sex", "birthdate"):
        if not merged.get(field) and incoming.get(field):
            merged[field] = incoming[field]
    return merged


def staff_affiliation_interval(records, current_year=None):
    """Return the union interval and covered years for staff records."""
    current_year = current_year or datetime.now(timezone.utc).year
    starts = []
    ends = []
    years = set()
    active = False
    for record in records:
        start_value = record.get("fecha_inicial_vinculación")
        if not has_text(start_value):
            continue
        try:
            start = datetime.strptime(
                str(start_value), "%d/%m/%Y").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        starts.append(int(start.timestamp()))
        end_value = record.get("fecha_final_vinculación")
        if has_text(end_value):
            try:
                end = datetime.strptime(
                    str(end_value), "%d/%m/%Y").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            ends.append(int(end.timestamp()))
            end_year = end.year
        else:
            active = True
            end_year = current_year
        years.update(range(start.year, end_year + 1))
    if not starts:
        return None, -1, []
    end_date = -1 if active else (max(ends) if ends else -1)
    return min(starts), end_date, sorted(years)


class Kahi_staff_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("external_ids.id.COD_RH")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])
        self.db["affiliations"].create_index("relations.id")

        self.verbose = config["verbose"] if "verbose" in config else 0
        self.task = config["staff_person"].get("task")

        self.staff_required_columns = [
            "tipo_documento",
            "identificación",
            "primer_apellido",
            "segundo_apellido",
            "nombres",
            "nivel_académico",
            "tipo_contrato",
            "jornada_laboral",
            "categoría_laboral",
            "sexo",
            "fecha_nacimiento",
            "fecha_inicial_vinculación",
            "fecha_final_vinculación",
            "código_unidad_académica",
            "unidad_académica",
            "código_subunidad_académica",
            "subunidad_académica"]

        self.ciarp_required_columns = [
            "código_unidad_académica",
            "código_subunidad_académica",
            "tipo_documento",
            "identificación",
            "año",
            "título",
            "idioma",
            "revista",
            "editorial",
            "doi",
            "issn",
            "isbn",
            "volumen",
            "issue",
            "primera_página",
            "pais_producto",
            "última_página",
            "entidad_premiadora",
            "ranking"]

    def filter_research_products(
            self,
            df: DataFrame,
            categories: list,
            document: str) -> list:
        """
        Filter a DataFrame of research products by specified categories, identification code,
        and valid DOI field.

        A DOI is considered valid if it is returned by the doi_processor() function.

        Parameters:
        - df (pd.DataFrame): DataFrame containing research products data.
        - categories (list): List of categories to filter the 'ranking' column.
        - document (str): Id to filter the 'identificación' column.

        Returns:
        - list: A list of dictionaries, where each dictionary represents a filtered record.
        """
        # Filter the DataFrame where 'ranking' is in the provided categories list
        # and 'identificación' matches the given cod_rh code.
        filtered_df = df[(df['ranking'].isin(categories)) &
                         (df['identificación'] == document)]

        # Further filter the DataFrame to keep only records with a valid DOI.
        # A DOI is valid if doi_processor(doi) does not return None.
        filtered_df = filtered_df[filtered_df['doi'].apply(
            lambda x: bool(x) and doi_processor(x) is not None)]

        # Convert the filtered DataFrame into a list of dictionaries.
        return filtered_df.to_dict(orient='records')

    def _load_file_data(self, file_path, required_columns, optional_columns=()):
        dtype_mapping = {col: str for col in required_columns}
        dtype_mapping.update({"orcid": str, "cvlac": str})
        data = read_excel(file_path, dtype=dtype_mapping).fillna("")
        for col in required_columns:
            if col not in data.columns:
                if col in optional_columns:
                    data[col] = ""
                    continue
                print(
                    f"Column {col} found not in file {file_path}, and it is required.")
                raise ValueError(
                    f"Column {col} found not in file {file_path}")
        return data

    def _source_collection(self, config, collection_key):
        source_url = config.get("database_url", self.config["database_url"])
        source_db_name = config.get("database_name", "institutional_data")
        source_collection_name = config.get(
            f"{collection_key}_collection_name", collection_key)
        client = MongoClient(source_url)
        return client, client[source_db_name][source_collection_name], source_db_name, source_collection_name

    def _institution_ids(self):
        if "databases" in self.config["staff_person"]:
            return [
                config["institution_id"].replace("https://ror.org/", "")
                for config in self.config["staff_person"]["databases"]
            ]

        client, collection, _, _ = self._source_collection(
            self.config["staff_person"], "staff")
        try:
            return sorted(collection.distinct("institution_id"))
        finally:
            client.close()

    def _config_for_institution(self, institution_id):
        for config in self.config["staff_person"].get("databases", []):
            if config["institution_id"].replace(
                    "https://ror.org/", "") == institution_id:
                return config
        return self.config["staff_person"]

    def _load_collection_data(
            self,
            config,
            collection_key,
            institution_id,
            required_columns):
        client, collection, _, _ = self._source_collection(
            config, collection_key)
        try:
            records = list(collection.find(
                {"institution_id": institution_id}, {"_id": 0}))
        finally:
            client.close()

        data = DataFrame(records).fillna("")
        for col in required_columns:
            if col not in data.columns:
                data[col] = ""
            data[col] = data[col].astype(str)
        return data

    def _load_staff_data(self, config, institution_id):
        if "staff_file_path" in config:
            return self._load_file_data(
                config["staff_file_path"],
                self.staff_required_columns, optional_columns=STAFF_UNIT_COLUMNS)
        data = self._load_collection_data(
            config, "staff", institution_id, self.staff_required_columns)
        if data.empty:
            client, _, source_db_name, source_collection_name = self._source_collection(
                config, "staff")
            client.close()
            raise ValueError(
                f"No staff records found for institution {institution_id} in "
                f"{source_db_name}.{source_collection_name}")
        return data

    def _load_ciarp_data(self, config, institution_id):
        if "ciarp_file_path" in config:
            return self._load_file_data(
                config["ciarp_file_path"],
                self.ciarp_required_columns)
        return self._load_collection_data(
            config, "ciarp", institution_id, self.ciarp_required_columns)

    def process_staff(self):
        progress = ProgressLogger(
            f"staff_person:{self.staff_source_institution_id}",
            total=len(self.cedula_dep)).start(
            "institucion={}".format(next(
                (n.get("name") for n in self.staff_reg.get("names", [])), "")))
        # Iterate over the unique identifiers
        for idx in list(self.cedula_dep.keys()):
            # Get the current year
            current_year = datetime.now(timezone.utc).year
            # Check if the person is already in the database
            check_db = self.collection.find_one({"external_ids.id": idx})
            if not check_db:
                check_db = self.collection.find_one(
                    {"external_ids.id.COD_RH": idx})
            entry = self.empty_person()
            entry["updated"].append(
                {"time": int(time()), "source": "staff", "provenance": "staff"})
            entry["first_names"] = split_name_part(
                self.staff_data[self.staff_data["identificación"] == idx].iloc[0]["nombres"])
            first_lastname = self.staff_data[self.staff_data["identificación"]
                                             == idx].iloc[0]["primer_apellido"]
            entry["last_names"].append(first_lastname.strip())
            second_lastname = self.staff_data[self.staff_data["identificación"]
                                              == idx].iloc[0]["segundo_apellido"]
            if second_lastname.strip() != "":
                entry["last_names"].append(second_lastname.strip())
            entry["full_name"] = " ".join(
                entry["first_names"] + entry["last_names"])
            entry["initials"] = "".join([name[0]
                                        for name in entry["first_names"]])

            for i, reg in self.staff_data[self.staff_data["identificación"] == idx].iterrows(
            ):
                start_date = end_date = None
                start_date = reg.get("fecha_inicial_vinculación")
                end_date = reg.get("fecha_final_vinculación")
                if end_date:
                    end_date = int(
                        datetime.strptime(
                            end_date,
                            "%d/%m/%Y").timestamp())
                if start_date:
                    start_date = int(
                        datetime.strptime(
                            start_date,
                            "%d/%m/%Y").timestamp())

                name = self.staff_reg["names"][0]["name"]
                for n in self.staff_reg["names"]:
                    if n["lang"] == "es":
                        name = n["name"]
                        break
                    elif n["lang"] == "en":
                        name = n["name"]
                name = title_case(name)
                auth_aff = {
                    "id": self.staff_reg["_id"],
                    "name": name,
                    "types": self.staff_reg["types"],
                    "start_date": start_date,
                    "end_date": end_date if end_date else -1}
                if auth_aff["id"] not in [aff["id"]
                                          for aff in entry["affiliations"]]:
                    entry["affiliations"].append(auth_aff)
                doc_type = reg["tipo_documento"]
                if not append_staff_external_ids(
                        entry["external_ids"], reg, idx):
                    # Print an error message if the document type is invalid
                    print(
                        f"ERROR: tipo_documento must be one of {', '.join(DOCUMENT_TYPES.keys())}, not '{doc_type}'")

                if reg["nombres"].lower() not in entry["aliases"]:
                    entry["aliases"].append(reg["nombres"].lower())
                dep = None
                if staff_department_id(self.staff_source_institution_id, reg):
                    dep = find_staff_unit(
                        self.db["affiliations"],
                        staff_department_id(
                            self.staff_source_institution_id, reg),
                        self.staff_reg["_id"],
                        "department",
                    )
                if dep:
                    name = dep["names"][0]["name"]
                    for n in dep["names"]:
                        if n["lang"] == "es":
                            name = n["name"]
                            break
                        elif n["lang"] == "en":
                            name = n["name"]
                    name = title_case(name)
                    dep_affiliation = {
                        "id": dep["_id"],
                        "name": name,
                        "types": dep["types"],
                        "start_date": start_date,
                        "end_date": end_date if end_date else -1}
                    if dep_affiliation["id"] not in [aff["id"]
                                                     for aff in entry["affiliations"]]:
                        entry["affiliations"].append(dep_affiliation)
                fac = None
                if staff_faculty_id(self.staff_source_institution_id, reg):
                    fac = find_staff_unit(
                        self.db["affiliations"],
                        staff_faculty_id(
                            self.staff_source_institution_id, reg),
                        self.staff_reg["_id"],
                        "faculty",
                    )
                if fac:
                    name = fac["names"][0]["name"]
                    for n in fac["names"]:
                        if n["lang"] == "es":
                            name = n["name"]
                            break
                        elif n["lang"] == "en":
                            name = n["name"]
                    name = title_case(name)
                    fac_affiliation = {
                        "id": fac["_id"],
                        "name": name,
                        "types": fac["types"],
                        "start_date": start_date,
                        "end_date": end_date if end_date else -1}
                    if fac_affiliation["id"] not in [aff["id"]
                                                     for aff in entry["affiliations"]]:
                        entry["affiliations"].append(fac_affiliation)

                if reg["fecha_nacimiento"] != "":
                    entry["birthdate"] = int(
                        dt.strptime(
                            reg["fecha_nacimiento"],
                            "%d/%m/%Y").timestamp())
                sex = reg.get("sexo")
                if sex:
                    entry["sex"] = sex.capitalize() if sex.capitalize() in {
                        "Hombre", "Mujer", "Intersexual"} else parse_sex(sex)
                if reg["nivel_académico"]:
                    degree = {
                        "date": -1,
                        "degree": reg["nivel_académico"],
                        "id": "",
                        "institutions": [],
                        "source": "nivel_académico",
                        "provenance": "staff"}
                    if degree not in entry["degrees"]:
                        entry["degrees"].append(degree)
                if reg["tipo_contrato"]:
                    ranking = {
                        "date": start_date,
                        "rank": reg["tipo_contrato"],
                        "source": "tipo_contrato",
                        "provenance": "staff"}
                    if ranking not in entry["ranking"]:
                        entry["ranking"].append(ranking)
                if reg["jornada_laboral"]:
                    ranking = {
                        "date": start_date,
                        "rank": reg["jornada_laboral"],
                        "source": "jornada_laboral",
                        "provenance": "staff"}
                    if ranking not in entry["ranking"]:
                        entry["ranking"].append(ranking)
                if reg["categoría_laboral"]:
                    ranking = {
                        "date": start_date,
                        "rank": reg["categoría_laboral"],
                        "source": "categoría_laboral",
                        "provenance": "staff"}
                    if ranking not in entry["ranking"]:
                        entry["ranking"].append(ranking)

            # Keep actual covered years (including gaps) for every affiliation,
            # not just the first row seen for each faculty/department.
            records = self.staff_data[
                self.staff_data["identificación"] == idx
            ].to_dict(orient="records")
            for aff in entry["affiliations"]:
                if aff["id"] == self.staff_reg["_id"]:
                    covered = records
                else:
                    covered = [record for record in records if aff["id"] in {
                        staff_faculty_id(self.staff_source_institution_id, record),
                        staff_department_id(self.staff_source_institution_id, record),
                    }]
                aff["start_date"], aff["end_date"], aff["years"] = staff_affiliation_interval(
                    covered, current_year=current_year)

            # Get the research products for the author
            if self.allowed_categories:
                author_works = self.filter_research_products(
                    self.ciarp_data, self.allowed_categories, idx)
                if author_works:
                    for work in author_works:
                        work_doi = doi_processor(work["doi"])
                        if work_doi:
                            rec = {
                                "provenance": "ciarp",
                                "source": "doi",
                                "id": work_doi,
                                "year": work["año"]}
                            if rec not in entry["related_works"]:
                                entry["related_works"].append(rec)

            # Add staff history even when another source inserted the person
            # first. Skipping an existing person loses its parent institution.
            if check_db:
                merged = order_document(
                    merge_staff_person(check_db, entry), self.empty_person()
                )
                self.collection.replace_one(
                    {"_id": check_db["_id"]}, merged)
                progress.step("actualizados")
            else:
                self.collection.insert_one(entry)
                progress.step("nuevos")
        progress.done()

    def backfill_ciarp_related_works(self):
        """Add missing CIARP DOI evidence to existing, already unified people."""
        stats = {
            "identities": 0,
            "matched": 0,
            "missing_staff": 0,
            "missing_person": 0,
            "ambiguous_person": 0,
            "resolved_by_primary_id": 0,
            "resolved_by_name": 0,
            "references_added": 0,
        }
        for source_institution_id in self._institution_ids():
            config = self._config_for_institution(source_institution_id)
            self.staff_source_institution_id = source_institution_id
            institution_id = canonical_ror_url(source_institution_id)
            try:
                allowed_categories = ciarp_mapping(institution_id, "works")
            except ValueError:
                continue

            staff_data = self._load_staff_data(config, source_institution_id)
            ciarp_data = self._load_ciarp_data(config, source_institution_id)
            related_by_person = build_ciarp_related_work_map(
                ciarp_data, allowed_categories)
            staff_groups = {
                str(person_id).strip(): group.to_dict(orient="records")
                for person_id, group in staff_data.groupby("identificación")
            }

            progress = ProgressLogger(
                f"staff_person:ciarp_related_works:{source_institution_id}",
                total=len(related_by_person)).start()
            for person_id, references in related_by_person.items():
                stats["identities"] += 1
                records = staff_groups.get(person_id)
                if not records:
                    stats["missing_staff"] += 1
                    progress.step("omitidos")
                    continue
                query = staff_person_query(records, person_id)
                if not query:
                    stats["missing_person"] += 1
                    progress.step("omitidos")
                    continue
                candidates = list(self.collection.find(
                    query, {"_id": 1, "full_name": 1, "related_works": 1}))
                if not candidates:
                    stats["missing_person"] += 1
                    progress.step("omitidos")
                    continue
                if len(candidates) > 1:
                    primary_query = staff_primary_person_query(records, person_id)
                    primary_matches = list(self.collection.find(
                        primary_query,
                        {"_id": 1, "full_name": 1, "related_works": 1},
                    )) if primary_query else []
                    if len(primary_matches) == 1:
                        candidates = primary_matches
                        stats["resolved_by_primary_id"] += 1
                    elif primary_matches:
                        candidates = primary_matches

                if len(candidates) > 1:
                    source_name = normalized_full_name(staff_full_name(records))
                    name_matches = [
                        candidate for candidate in candidates
                        if normalized_full_name(candidate.get("full_name")) == source_name
                    ]
                    if source_name and len(name_matches) == 1:
                        candidates = name_matches
                        stats["resolved_by_name"] += 1
                    else:
                        stats["ambiguous_person"] += 1
                        progress.step("omitidos")
                        continue

                stats["matched"] += 1
                person = candidates[0]
                existing = person.get("related_works", [])
                missing = [record for record in references if record not in existing]
                if not missing:
                    progress.step("existentes")
                    continue
                self.collection.update_one(
                    {"_id": person["_id"]},
                    {"$addToSet": {"related_works": {"$each": missing}}},
                )
                stats["references_added"] += len(missing)
                progress.step("actualizados")
            progress.done()

        print(f"INFO: CIARP related works backfill summary: {stats}")
        return 0

    def run(self):
        if self.task == "ciarp_related_works":
            return self.backfill_ciarp_related_works()
        if self.task:
            raise ValueError(f"Unsupported staff_person task: {self.task}")

        for source_institution_id in self._institution_ids():
            config = self._config_for_institution(source_institution_id)
            self.staff_source_institution_id = source_institution_id
            institution_id = canonical_ror_url(source_institution_id)
            # Get the allowed categories for the institution
            try:
                self.allowed_categories = ciarp_mapping(
                    institution_id, "works")
            except ValueError:
                self.allowed_categories = []

            self.staff_reg = self.db["affiliations"].find_one(
                {"external_ids.id": institution_id})
            if not self.staff_reg:
                self.staff_reg = self.db["affiliations"].find_one(
                    {"_id": source_institution_id})
            if not self.staff_reg:
                print("Institution not found in database")
                raise ValueError(
                    f"Institution {institution_id} found not in database")

            self.staff_data = self._load_staff_data(
                config, source_institution_id)
            self.ciarp_data = self._load_ciarp_data(
                config, source_institution_id)

            # logs for higher verbosity
            self.facs_inserted = {}
            self.deps_inserted = {}
            self.fac_dep = []

            self.cedula_dep = {}
            self.cedula_fac = {}

            for idx, reg in self.staff_data.iterrows():
                self.cedula_fac[reg["identificación"]] = title_case(
                    reg["unidad_académica"])
                self.cedula_dep[reg["identificación"]] = title_case(
                    reg["subunidad_académica"])

            # convert dates to the correct format
            for col in [
                "fecha_nacimiento",
                "fecha_inicial_vinculación",
                    "fecha_final_vinculación"]:
                self.staff_data[col] = to_datetime(
                    self.staff_data[col], dayfirst=True, errors='coerce')
                self.staff_data[col] = self.staff_data[col].dt.strftime(
                    '%d/%m/%Y').fillna('')

            self.facs_inserted = {}
            self.deps_inserted = {}
            self.fac_dep = []

            self.process_staff()

        return 0
