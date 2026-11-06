from kahi_impactu_utils.Mongo import insert_only_update
from kahi_impactu_utils.Work import (
    work_titles_match,
    AmbiguousWorkDOI, canonical_work_doi, claim_work_doi, find_work_by_doi,
    insert_work_by_doi, prepare_work_doi, skip_work_doi_conflicts,
)
from time import time
from datetime import datetime, timezone
from kahi_impactu_utils.Utils import compare_author, split_names, split_names_fix, normalize_publication_year
from kahi_ciarp_works.parser import parse_ciarp
from bson import ObjectId
from threading import Lock
from copy import deepcopy


_INSERT_LOCKS = tuple(Lock() for _ in range(256))


def run_es_operation(semaphore, operation, *args, **kwargs):
    if semaphore is None:
        return operation(*args, **kwargs)
    with semaphore:
        return operation(*args, **kwargs)


def _preferred_source_name(source):
    names = source.get("names") or []
    if not names:
        return ""
    name = names[0].get("name", "")
    for language in ("es", "en"):
        candidate = next((
            item.get("name", "") for item in names
            if item.get("lang") == language and item.get("name")
        ), None)
        if candidate:
            return candidate
    return name


def _resolve_source(db, source):
    if not isinstance(source, dict):
        return None
    for external_id in source.get("external_ids") or []:
        identifier = external_id.get("id")
        if identifier in (None, ""):
            continue
        source_db = db["sources"].find_one({"external_ids.id": identifier})
        if source_db:
            return {
                "id": source_db["_id"],
                "name": _preferred_source_name(source_db),
            }
    return None


def merge_missing_core_metadata(colav_reg, entry, db):
    """Fill publication metadata without overwriting populated fields."""
    changed = {}
    year = normalize_publication_year(colav_reg.get("year_published"))
    if year is None:
        year = normalize_publication_year(entry.get("year_published"))
    if colav_reg.get("year_published") != year:
        colav_reg["year_published"] = year
        changed["year_published"] = year
    for field in ("doi", "date_published"):
        current = colav_reg.get(field)
        incoming = entry.get(field)
        if current in (None, "", 0) and incoming not in (None, "", 0):
            colav_reg[field] = incoming
            changed[field] = incoming
    current_source = colav_reg.get("source") or {}
    if not isinstance(current_source.get("id"), ObjectId):
        source = _resolve_source(db, entry.get("source"))
        if source:
            colav_reg["source"] = source
            changed["source"] = source
    return changed


def ciarp_record_id(entry):
    return next((
        ext.get("id") for ext in entry.get("external_ids", [])
        if ext.get("source") == "ciarp_record" and ext.get("id")
    ), None)


def ciarp_record_query(record_id):
    return {"external_ids": {"$elemMatch": {
        "source": "ciarp_record", "id": str(record_id)}}}


def ciarp_insert_lock(record_id):
    return _INSERT_LOCKS[hash(str(record_id)) % len(_INSERT_LOCKS)]


def build_es_work(entry):
    if not entry.get("titles") or not entry["titles"][0].get(
            "title", "").strip():
        return None
    bibliographic_info = entry.get("bibliographic_info", {})
    return {
        "title": entry["titles"][0]["title"],
        "source": entry.get("source", {}).get("name", ""),
        "year": entry.get("year_published") or 0,
        "volume": bibliographic_info.get("volume", ""),
        "issue": bibliographic_info.get("issue", ""),
        "first_page": bibliographic_info.get("start_page", ""),
        "last_page": bibliographic_info.get("end_page", ""),
        "authors": [
            author["full_name"]
            for author in entry.get("authors", [])[:5]
            if author.get("full_name")
        ],
        "provenance": "ciarp",
    }


def ensure_es_work(entry, work_id, es_handler, es_semaphore=None):
    if not es_handler or work_id is None:
        return
    document = build_es_work(entry)
    if document is None:
        return
    exists = run_es_operation(
        es_semaphore, es_handler.es.exists,
        index=es_handler.es_index, id=str(work_id))
    if not exists:
        run_es_operation(
            es_semaphore, es_handler.insert_work,
            _id=str(work_id), work=document)


def get_doi(reg):
    """
    Method to get the doi of a register.

    Parameters:
    ----------
    reg : dict
        Register from the ciarp database

    Returns:
    -------
    str
        doi of the register, False if not found.
    """
    for i in reg["external_ids"]:
        if i["source"] == 'doi':
            return i["id"]
    return False


def _date_year(value):
    """Return the UTC year for a stored affiliation timestamp."""
    if value in (None, "", -1):
        return None
    if isinstance(value, datetime):
        return value.year
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).year
    except (TypeError, ValueError, OSError):
        return None


def affiliation_covers_year(affiliation, year, current_year=None):
    """Use explicit covered years when present, otherwise a dated interval."""
    if "years" in affiliation:
        return year in (affiliation.get("years") or [])
    start = _date_year(affiliation.get("start_date"))
    if start is None:
        return False
    end_value = affiliation.get("end_date")
    end = ((current_year or datetime.now(timezone.utc).year)
           if end_value in (None, "", -1) else _date_year(end_value))
    return end is not None and start <= year <= end


def publication_year_in_parent_affiliation(
        author, parent_id, publication_year, current_year=None):
    """Accept the publication year or the previous year in the parent history."""
    work_year = normalize_publication_year(publication_year, current_year=current_year)
    if work_year is None:
        return False
    parents = [
        item for item in author.get("affiliations", [])
        if str(item.get("id")) == str(parent_id)
    ]
    for parent in parents:
        if any(affiliation_covers_year(parent, year, current_year)
               for year in (work_year, work_year - 1)):
            return True
    return False


def select_work_affiliations(author, institution, publication_year, db, current_year=None):
    """Choose the author's dated units; never infer them from CIARP row codes."""
    if not author:
        return []
    year = normalize_publication_year(publication_year, current_year=current_year)
    if year is None:
        return []
    parent_id = institution["_id"]
    parents = [aff for aff in author.get("affiliations", [])
               if str(aff.get("id")) == str(parent_id)]
    eligible_years = [candidate for candidate in (year, year - 1)
                      if any(affiliation_covers_year(parent, candidate, current_year)
                             for parent in parents)]
    if not eligible_years:
        return []
    selected = [{"id": parent_id, "name": _preferred_source_name(institution),
                 "types": deepcopy(institution.get("types", []))}]
    units = {"faculty": [], "department": []}
    for aff in author.get("affiliations", []):
        types = {str(item.get("type", "")).lower() for item in aff.get("types", [])}
        kind = next((name for name in units if name in types), None)
        if kind is None or not aff.get("id"):
            continue
        valid_years = [candidate for candidate in eligible_years
                       if affiliation_covers_year(aff, candidate, current_year)]
        if not valid_years:
            continue
        # Units must belong to this institution; homonyms and units of other
        # universities must never be attached just because dates coincide.
        unit = db["affiliations"].find_one({"_id": aff["id"]})
        if not unit or not any(str(rel.get("id")) == str(parent_id)
                               for rel in unit.get("relations", [])):
            continue
        units[kind].append((valid_years, {
            "id": unit["_id"], "name": _preferred_source_name(unit) or aff.get("name", ""),
            "types": deepcopy(unit.get("types", aff.get("types", []))),
        }))
    seen = {parent_id}
    for candidates in units.values():
        for candidate_year in eligible_years:
            matches = [unit for years, unit in candidates if candidate_year in years]
            if not matches:
                continue
            for unit in matches:
                if unit["id"] not in seen:
                    selected.append(unit)
                    seen.add(unit["id"])
            break
    return selected


def resolve_ciarp_author(db, author):
    """Resolve identity without treating an empty name as a valid identifier."""
    projection = {"full_name": 1, "affiliations": 1}
    for external in author.get("external_ids", []):
        identifier = external.get("id")
        if identifier in (None, ""):
            continue
        person = db["person"].find_one({"external_ids.id": identifier}, projection)
        if not person:
            person = db["person"].find_one({"external_ids.id.COD_RH": identifier}, projection)
        if person:
            return person
    if author.get("full_name", "").strip():
        return db["person"].find_one({"full_name": author["full_name"]}, projection)
    return None


def merge_author_affiliations(colav_reg, author_id, affiliations):
    """Add CIARP affiliations to an existing author without removing others."""
    for author in colav_reg.get("authors", []):
        if author.get("id") != author_id:
            continue
        current = author.setdefault("affiliations", [])
        current_ids = {aff.get("id") for aff in current}
        for affiliation in affiliations:
            affiliation_id = affiliation.get("id")
            if affiliation_id and affiliation_id not in current_ids:
                current.append(affiliation)
                current_ids.add(affiliation_id)
        return True
    return False


def process_author(entry, colav_reg, db, verbose=0,
                   ciarp_affiliations_allowed=False):
    """
    Function to compare the authors of a register from the ciarp database with the authors of a register from the colav database.
    If the authors match, the author from the colav register is replaced with the author from the ciarp register.

    Parameters
    ----------
    entry : dict
        Register from the ciarp database
    colav_reg : dict
        Register from the colav database (kahi database for impactu)
    db : pymongo.collection.Collection
        Database where ETL result is stored
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    ciarp_author = entry['authors'][0]
    if ciarp_author:
        author_db = None
        if 'external_ids' in ciarp_author.keys():
            author_ids = ciarp_author['external_ids'][0]
            author_db = db['person'].find_one(
                {
                    'external_ids.id': author_ids["id"]},
                {
                    "_id": 1,
                    "full_name": 1,
                    "affiliations": 1,
                    "first_names": 1,
                    "last_names": 1,
                    "initials": 1,
                    "external_ids": 1})
            if not author_db:
                author_db = db['person'].find_one({'external_ids.id.COD_RH': author_ids["id"]}, {
                    "_id": 1, "full_name": 1, "affiliations": 1, "first_names": 1, "last_names": 1, "initials": 1, "external_ids": 1})
        if author_db:
            name_match = None
            affiliation_match = None
            for i, author in enumerate(colav_reg['authors']):
                if author['id'] == author_db['_id']:
                    # adding the group for the author
                    groups = []
                    for aff in (ciarp_author["affiliations"]
                                if ciarp_affiliations_allowed else []):
                        if aff["types"]:
                            for t in aff["types"]:
                                if t["type"] == "group":
                                    groups.append(aff)
                    for group in groups:
                        if group not in author["affiliations"]:
                            author["affiliations"].append(group)
                    colav_reg["authors"][i] = {
                        "id": author_db["_id"],
                        "full_name": author_db["full_name"],
                        "affiliations": author["affiliations"]
                    }
                    continue
                author_reg = None
                if author['id'] == "":
                    if verbose >= 4:
                        print(
                            f"WARNING: author with id '' found in colav register: {author} using split_names")
                    author_reg = split_names(author["full_name"])
                else:
                    author_reg = db['person'].find_one(
                        # this is required to get  first_names and last_names
                        {'_id': author['id']}, {"_id": 1, "full_name": 1, "first_names": 1, "last_names": 1, "initials": 1, "external_ids": 1})
                    if author_reg is None:
                        print(
                            f"ERROR: author with id {author['id']} found not in colav database")
                        continue

                # Note: even in openalex names are bad splitted, so we need to fix them
                # ex: 'full_name': 'Claudia Marcela-Vélez', 'first_names': ['Claudia'], 'last_names': ['Marcela', 'Vélez']  where Marcela is the first name and Vélez is the last name
                # then we need to compare the names after fixing them.
                author_reg_fix = split_names_fix(author_reg, author_db)
                if author_reg_fix:
                    author_reg["full_name"] = author_reg_fix["full_name"]
                    author_reg["first_names"] = author_reg_fix["first_names"]
                    author_reg["last_names"] = author_reg_fix["last_names"]
                    author_reg["initials"] = author_reg_fix["initials"]

                name_match = compare_author(
                    author_reg, author_db, len(colav_reg['authors']))

                doi1 = get_doi(entry)
                doi2 = get_doi(colav_reg)
                if doi1 and doi2:
                    if doi1 == doi2:
                        affiliation_match = True
                else:
                    if author['affiliations']:
                        affiliations_person = [str(aff['id'])
                                               for aff in author_db['affiliations']]
                        author_affiliations = [str(aff['id'])
                                               for aff in author['affiliations']]
                        affiliation_match = any(
                            affil in author_affiliations for affil in affiliations_person)
                if name_match and author['affiliations'] == []:
                    affiliation_match = True  # noqa: F841

                # if name_match and affiliation_match:
                if name_match and affiliation_match:
                    # replace the author in the colav register
                    # adding the group, faculty and department for the author
                    groups = []
                    affs_ids = [aff_id["id"]
                                for aff_id in author_db["affiliations"]]
                    for aff in (ciarp_author["affiliations"]
                                if ciarp_affiliations_allowed else []):
                        if aff["types"]:
                            for t in aff["types"]:
                                if (str(t.get("type", "")).lower()
                                        == "education"
                                        and aff["id"] not in affs_ids):
                                    if aff not in author["affiliations"]:
                                        author["affiliations"].append(aff)
                                if t["type"] == "group":
                                    groups.append(aff)
                                elif t["type"] == "faculty" or t["type"] == "department":
                                    if aff["name"] != "":
                                        author["affiliations"].append(aff)
                    for group in groups:
                        if group not in author["affiliations"]:
                            author["affiliations"].append(group)
                    colav_reg["authors"][i] = {
                        "id": author_db["_id"],
                        "full_name": author_db["full_name"],
                        "affiliations": author["affiliations"]
                    }
                    break


def process_one_update(
        ciarp_reg,
        colav_reg,
        db,
        collection,
        affiliation,
        empty_work,
        verbose=0):
    """
    Method to update a register in the kahi database from ciarp database if it is found.
    This means that the register is already on the kahi database and it is being updated with new information.


    Parameters
    ----------
    ciarp_reg : dict
        Register from the ciarp database
    colav_reg : dict
        Register from the colav database (kahi database for impactu)
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    affiliation : dict
        Affiliation of the author
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    """
    # updated
    entry = parse_ciarp(
        ciarp_reg, affiliation, empty_work.copy())
    doi = prepare_work_doi(entry, "ciarp")
    if doi:
        claim_work_doi(collection, colav_reg, doi)
    core_updates = merge_missing_core_metadata(colav_reg, entry, db)
    # Add updated time
    if not any(
            update.get("source") == "ciarp"
            for update in colav_reg.get("updated", [])):
        colav_reg["updated"].append(
            {"source": "ciarp", "time": int(time())})
    # titles
    colav_reg["titles"].extend(
        title for title in entry["titles"] if title not in colav_reg["titles"])
    # external_ids
    ext_ids = [ext["id"] for ext in colav_reg["external_ids"]]
    for ext in entry["external_ids"]:
        if ext["id"] not in ext_ids:
            colav_reg["external_ids"].append(ext)
            ext_ids.append(ext["id"])
    # Select dated affiliations from the person's history, not CIARP unit codes.
    author = entry["authors"][0]
    author_db = resolve_ciarp_author(db, author)
    if author_db:
        author["id"] = author_db["_id"]
        author["full_name"] = author_db["full_name"]
    author["affiliations"] = select_work_affiliations(
        author_db, affiliation, colav_reg.get("year_published"), db)
    process_author(
        entry, colav_reg, db, verbose,
        ciarp_affiliations_allowed=bool(author["affiliations"]),
    )
    if author_db and author["affiliations"]:
        merge_author_affiliations(
            colav_reg, author_db["_id"], author["affiliations"])
    # Check if author is already in the register
    colav_reg_author_ids = [auth["id"] for auth in colav_reg["authors"]]
    for author in entry["authors"]:
        if author["id"] and author["id"] not in colav_reg_author_ids:
            colav_reg["authors"].append(author)
        for key in ["external_ids", "types"]:
            author.pop(key, None)
    # Update author count
    colav_reg["author_count"] = len(colav_reg["authors"])
    # Update register
    fields = {
        "updated": colav_reg["updated"],
        "titles": colav_reg["titles"],
        "external_ids": colav_reg["external_ids"],
        "authors": colav_reg["authors"],
        "author_count": colav_reg["author_count"],
    }
    fields.update(core_updates)
    additions = {field: {"$each": fields.pop(field)}
                 for field in ("updated", "titles", "external_ids")}
    collection.update_one(
        {"_id": colav_reg["_id"]},
        {"$set": fields, "$addToSet": additions}
    )


@skip_work_doi_conflicts
def process_one_insert(ciarp_reg, db, collection, affiliation, empty_work,
                       es_handler, verbose=0, es_semaphore=None):
    """
    Function to insert a new register in the database if it is not found in the colav(kahi works) database.
    This means that the register is not on the database and it is being inserted.

    For similarity purposes, the register is also inserted in the elasticsearch index,
    all the elastic search fields are filled with the information from the register and it is
    handled by Mohan's Similarity class.

    Parameters:
    ----------
    ciarp_reg: dict
        Register from the ciarp database
    db: pymongo.database.Database
        Database where the collection is stored (kahi database)
    collection: pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    affiliation: dict
        Affiliation of the author
    empty_work: dict
        Empty dictionary with the structure of a register in the database
    es_handler: Mohan.Similarity.Similarity
        Handler for the elasticsearch index
    verbose: int
        Verbosity level
    """
    # parse
    entry = parse_ciarp(
        ciarp_reg, affiliation, empty_work)
    if not entry.get("titles") or not entry["titles"][0].get(
            "title", "").strip():
        return "omitidos"

    source_db = None
    if "external_ids" in entry["source"].keys():
        for ext in entry["source"]["external_ids"]:
            source_db = db["sources"].find_one(
                {"external_ids.id": ext["id"]})
            if source_db:
                break
    if source_db:
        name = source_db["names"][0]["name"]
        for n in source_db["names"]:
            if n["lang"] == "es":
                name = n["name"]
                break
            if n["lang"] == "en":
                name = n["name"]
        entry["source"] = {
            "id": source_db["_id"],
            "name": name
        }
    else:
        if len(entry["source"]["external_ids"]) == 0:
            if verbose > 4:
                print(
                    f'Register with doi: {ciarp_reg["doi"]} does not provide a source')
        else:
            if verbose > 4:
                print("No source found for\n\t",
                      entry["source"]["external_ids"])
        if entry["source"]["name"] == "":
            entry["source"] = {}
        else:
            entry["source"] = {
                "id": "",
                "name": entry["source"]["name"]
            }
    # Resolve identity and use the same temporal affiliation selection as updates.
    for i, author in enumerate(entry["authors"]):
        person = resolve_ciarp_author(db, author)
        entry["authors"][i] = {
            "id": person["_id"] if person else "",
            "full_name": person["full_name"] if person else author.get("full_name", ""),
            "affiliations": select_work_affiliations(
                person, affiliation, entry.get("year_published"), db),
        }
        if not person:
            entry["authors"][i]["external_ids"] = deepcopy(author.get("external_ids", []))
    entry["author_count"] = len(entry["authors"])
    # insert idempotently while preserving the historical timestamp ID
    record_id = ciarp_record_id(entry)
    doi = prepare_work_doi(entry, "ciarp")
    outcome = "nuevos"
    if doi:
        existing = (collection.find_one(ciarp_record_query(record_id))
                    if record_id else None)
        if existing and not work_titles_match(entry.get("titles"), existing.get("titles")):
            existing = None
        owner = find_work_by_doi(collection, doi, titles=entry.get("titles"))
        if existing and owner and existing["_id"] != owner["_id"]:
            raise AmbiguousWorkDOI(
                f"CIARP record {record_id} and DOI {doi} identify different works")
        existing = existing or owner
        if existing:
            inserted_id = None
        else:
            # A unique MongoDB key arbitrates writers in different processes.
            inserted_id, existing = insert_work_by_doi(collection, entry)
        if existing:
            process_one_update(ciarp_reg, existing, db, collection,
                               affiliation, empty_work, verbose)
            inserted_id = existing["_id"]
            outcome = "actualizados"
    elif record_id:
        with ciarp_insert_lock(record_id):
            response = collection.update_one(
                ciarp_record_query(record_id),
                insert_only_update(entry),
                upsert=True)
            inserted_id = response.upserted_id
        if inserted_id is None:
            outcome = "existentes"
            existing = collection.find_one(
                ciarp_record_query(record_id), {"_id": 1})
            inserted_id = existing.get("_id") if existing else None
    else:
        response = collection.insert_one(entry)
        inserted_id = response.inserted_id
    ensure_es_work(entry, inserted_id, es_handler, es_semaphore)
    return outcome


@skip_work_doi_conflicts
def process_one(ciarp_reg, db, collection, affiliation, empty_work, similarity,
                es_handler, verbose=0, es_semaphore=None):
    """
    Function to process a single register from the ciarp database.
    This function is used to insert or update a register in the colav(kahi works) database.

    Parameters:
    ----------
    ciarp_reg: dict
        Register from the ciarp database
    db: pymongo.database.Database
        Database where the collection is stored (kahi database)
    collection: pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    affiliation: dict
        Affiliation of the author
    empty_work: dict
        Empty dictionary with the structure of a register in the database
    similarity: bool
        Flag to indicate if the register should be inserted in the elasticsearch index
    es_handler: Mohan.Similarity.Similarity
        Handler for the elasticsearch index
    verbose: int
        Verbosity level

    Returns:
    -------
    str
        Outcome for progress reporting: "nuevos", "actualizados",
        "existentes" or "omitidos".
    """
    title = ciarp_reg.get("título")
    if not isinstance(title, str) or not title.strip():
        return "omitidos"

    outcome = "omitidos"
    doi = canonical_work_doi(ciarp_reg.get("doi"))
    if doi:
        # is the doi in colavdb?
        colav_reg = find_work_by_doi(collection, doi, titles=[{"title": title}])
        record_id = ciarp_reg.get("source_record_id")
        by_record = (collection.find_one(ciarp_record_query(record_id))
                     if record_id else None)
        if by_record and not work_titles_match([{"title": title}], by_record.get("titles")):
            by_record = None
        if colav_reg and by_record and colav_reg["_id"] != by_record["_id"]:
            raise AmbiguousWorkDOI(
                f"CIARP record {record_id} and DOI {doi} identify different works")
        colav_reg = colav_reg or by_record
        if colav_reg:  # update the register
            process_one_update(ciarp_reg, colav_reg, db,
                               collection, affiliation, empty_work, verbose)
            outcome = "actualizados"
        else:  # insert a new register
            outcome = process_one_insert(ciarp_reg, db, collection,
                                         affiliation, empty_work, es_handler, verbose,
                                         es_semaphore=es_semaphore)
    elif similarity:  # does not have a doi identifier
        # elasticsearch section
        entry = parse_ciarp(ciarp_reg, affiliation, empty_work)
        record_id = ciarp_record_id(entry)
        existing = (collection.find_one(
            ciarp_record_query(record_id)) if record_id else None)
        if existing:
            process_one_update(
                ciarp_reg, existing, db, collection, affiliation,
                empty_work, verbose)
            ensure_es_work(entry, existing["_id"], es_handler, es_semaphore)
            return "actualizados"
        if es_handler:
            work = build_es_work(entry)
            authors = []
            for author in entry['authors']:
                if len(authors) >= 5:
                    break
                if "full_name" in author.keys():
                    # Find author in person collection
                    author_db = db["person"].find_one({
                        "external_ids.id": author["external_ids"][0]["id"]})
                    if author_db:
                        author["full_name"] = author_db["full_name"]
                    authors.append(author["full_name"])
            work["authors"] = authors
            response = run_es_operation(
                es_semaphore, es_handler.search_work,
                title=work["title"],
                source=work["source"],
                year=work["year"],
                authors=authors,
                volume=work["volume"],
                issue=work["issue"],
                page_start=work["first_page"],
                page_end=work["last_page"]
            )

            if response:  # register already on db... update accordingly
                colav_reg = collection.find_one(
                    {"_id": ObjectId(response["_id"])})
                if colav_reg:
                    process_one_update(
                        ciarp_reg,
                        colav_reg,
                        db,
                        collection,
                        affiliation,
                        empty_work,
                        verbose)
                    outcome = "actualizados"
                else:
                    if verbose > 4:
                        print("Register with {} not found in mongodb".format(
                            response["_id"]))
            else:  # insert new register
                # print("Inserting new register")
                outcome = process_one_insert(
                    ciarp_reg, db, collection, affiliation, empty_work,
                    es_handler, verbose, es_semaphore=es_semaphore)
        else:
            if verbose > 4:
                print("No elasticsearch index provided")
    return outcome
