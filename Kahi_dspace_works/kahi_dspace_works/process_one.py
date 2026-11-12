from kahi_impactu_utils.Mongo import insert_only_update
from kahi_impactu_utils.Affiliations import get_units_affiliations
from kahi_impactu_utils.Work import (
    work_titles_match,
    AmbiguousWorkDOI, canonical_work_doi, claim_work_doi,
    find_work_by_doi, insert_work_by_doi, prepare_work_doi, skip_work_doi_conflicts,
)
from kahi_impactu_utils.Utils import compare_author, normalize_publication_year
from kahi_dspace_works.parser import parse_dspace
from kahi_dspace_works.utils import set_source, get_doi, check_work, str_normilize, title_similarity
from bson import ObjectId
from unidecode import unidecode
from threading import Lock
from unicodedata import normalize as unicode_normalize
import re


_DSPACE_LOCKS = tuple(Lock() for _ in range(1024))


def _dspace_lock(value):
    return _DSPACE_LOCKS[hash(value) % len(_DSPACE_LOCKS)]


def run_es_operation(semaphore, operation, *args, **kwargs):
    if semaphore is None:
        return operation(*args, **kwargs)
    with semaphore:
        return operation(*args, **kwargs)


def dspace_query(value):
    return {
        "external_ids": {
            "$elemMatch": {"source": "dspace", "id": value}
        }
    }


def _prepare_dspace_doi(entry):
    # Preserve get_doi's rejection of unresolved repository templates even
    # when a parsed record reaches an insert/update through similarity.
    value = entry.get("doi")
    if isinstance(value, str) and re.search(r"%(?![0-9A-Fa-f]{2})", value):
        entry.pop("_doi_key", None)
        return None
    return prepare_work_doi(entry, "dspace")


def _normalized_title(value):
    if not isinstance(value, str):
        return ""
    return " ".join(unicode_normalize("NFKC", value).casefold().split())


def build_es_work(entry):
    titles = entry.get("titles") or []
    title = titles[0].get("title", "").strip() if titles else ""
    if not title:
        return None
    bibliography = entry.get("bibliographic_info") or {}
    return {
        "title": title,
        "source": (entry.get("source") or {}).get("name", ""),
        "year": entry.get("year_published") or "0",
        "volume": bibliography.get("volume", ""),
        "issue": bibliography.get("issue", ""),
        "first_page": bibliography.get("start_page", ""),
        "last_page": bibliography.get("end_page", ""),
        "authors": [
            author.get("full_name", "")
            for author in entry.get("authors", [])[:5]
            if author.get("full_name")
        ],
        "provenance": "dspace",
    }


def ensure_es_work(entry, work_id, es_handler, es_semaphore=None):
    if not es_handler or work_id is None:
        return
    work = build_es_work(entry)
    if work is None:
        return
    es_client = getattr(es_handler, "es", None)
    es_index = getattr(es_handler, "es_index", None)
    exists = False
    if es_client is not None and es_index:
        exists = run_es_operation(
            es_semaphore, es_client.exists, index=es_index, id=str(work_id)
        )
    if not exists:
        run_es_operation(
            es_semaphore, es_handler.insert_work, _id=str(work_id), work=work
        )


def find_work_by_es_id(collection, value):
    if value is None:
        return None
    found = collection.find_one({"_id": value})
    if found:
        return found
    try:
        return collection.find_one({"_id": ObjectId(value)})
    except Exception:
        return None


def process_one_update(entry, colav_reg, affiliation, db, collection, verbose):
    """
    Update the entry in the works collection.

    Parameters:
    -----------
    entry : dict
        entry from dspace.
    colav_reg : dict
        entry to be updated.
    affiliation : dict | None
        trusted repository affiliation, or None if it is not mapped.
    db : pymongo.database.Database
        database object to kahi(ETL) database.
    collection : pymongo.collection.Collection
        collection object to kahi(ETL) database.
    verbose : int
        verbosity level.
    """
    doi = _prepare_dspace_doi(entry)
    if doi:
        claim_work_doi(collection, colav_reg, doi)
    # merging the two entries
    colav_reg["updated"].extend(entry["updated"])

    existing_titles = {
        _normalized_title(title.get("title"))
        for title in colav_reg["titles"]
    }
    for title in entry["titles"]:
        if _normalized_title(title.get("title")) not in existing_titles:
            colav_reg["titles"].append(title)
            existing_titles.add(_normalized_title(title.get("title")))

    year = normalize_publication_year(colav_reg.get("year_published"))
    if year is None:
        year = normalize_publication_year(entry.get("year_published"))
    colav_reg["year_published"] = year

    if entry["doi"] and not colav_reg["doi"]:
        colav_reg["doi"] = entry["doi"]

    for abstract in entry["abstracts"]:
        if abstract not in colav_reg["abstracts"]:
            colav_reg["abstracts"].append(abstract)

    for dtype in entry["types"]:
        if dtype not in colav_reg["types"]:
            colav_reg["types"].append(dtype)

    for did in entry["external_ids"]:
        if did not in colav_reg["external_ids"]:
            colav_reg["external_ids"].append(did)

    for url in entry["external_urls"]:
        if url not in colav_reg["external_urls"]:
            colav_reg["external_urls"].append(url)

    if entry["source"] and not colav_reg["source"]:
        colav_reg["source"] = entry["source"]

    for right in entry["rights"]:
        if right not in colav_reg["rights"]:
            colav_reg["rights"].append(right)

    # merging autorship only to add the type
    # affiliation is not considered in this merge
    for author_reg in entry["authors"]:
        name_match = False
        for author in colav_reg["authors"]:
            if author['id'] == "":
                if verbose >= 4:
                    print(
                        f"WARNING: author with id '' found in colav register: {author}")
                author_db = db['person'].find_one(
                    # this is required to get  first_names and last_names
                    {'full_name': author['full_name']}, {"_id": 1, "full_name": 1, "first_names": 1, "last_names": 1, "initials": 1, "updated": 1, "affiliations": 1})
            else:
                # only the name can be compared, because we dont have the
                # affiliation of the author from the paper in author_others
                author_db = db['person'].find_one(
                    # this is required to get  first_names and last_names
                    {'_id': author['id']}, {"_id": 1, "full_name": 1, "first_names": 1, "last_names": 1, "initials": 1, "updated": 1, "affiliations": 1})
            if author_db:
                name_match = compare_author(
                    author_reg, author_db, len(colav_reg["authors"]))
            if name_match:
                author["type"] = author_reg["type"]
                existing_ids = {
                    item.get("id") for item in author["affiliations"]}
                if affiliation is not None:
                    if affiliation.get("id") not in existing_ids:
                        author["affiliations"].append(affiliation)
                        existing_ids.add(affiliation.get("id"))
                units = get_units_affiliations(
                    db, author_db, author["affiliations"],
                    colav_reg.get("year_published") or entry.get("year_published"))
                for unit in units:
                    if unit.get("id") not in existing_ids:
                        author["affiliations"].append(unit)
                        existing_ids.add(unit.get("id"))
                break
        if not name_match:
            new_author = {"id": "",
                          "full_name": author_reg["full_name"],
                          "type": author_reg["type"],
                          "affiliations": author_reg["affiliations"],
                          }
            if new_author not in colav_reg["authors"]:
                # como no hay match se añade, ver
                # https://github.com/colav/impactu/issues/494
                colav_reg["authors"].append(new_author)
    colav_reg["author_count"] = len(colav_reg["authors"])
    collection.update_one(
        {"_id": colav_reg["_id"]},
        {"$set": {
            "abstracts": colav_reg["abstracts"],
            "doi": colav_reg["doi"],
            "year_published": colav_reg["year_published"],
            "external_urls": colav_reg["external_urls"],
            "types": colav_reg["types"],
            "authors": colav_reg["authors"],
            "author_count": colav_reg["author_count"],
            "source": colav_reg["source"],
            "rights": colav_reg["rights"],
        }, "$addToSet": {
            "updated": {"$each": colav_reg["updated"]},
            "titles": {"$each": colav_reg["titles"]},
            "external_ids": {"$each": colav_reg["external_ids"]},
        }}
    )


@skip_work_doi_conflicts
def process_one_insert(entry, affiliation, db, collection, es_handler, verbose,
                       es_semaphore=None, lock_dspace_id=True):
    """
    Insert the entry in the works collection.

    Parameters:
    -----------
    entry : dict
        entry to be inserted.
    affiliation : dict | None
        affiliation record, or None if affiliation is not found.
    db : pymongo.database.Database
        database object to kahi(ETL) database.
    collection : pymongo.collection.Collection
        collection object to kahi(ETL) database.
    es_handler : elasticsearch.Elasticsearch
        elasticsearch handler.
    verbose : int
        verbosity level.
    """
    # Verificar por ROR
    # Que tenga dos apellidos

    # removing unnecessary fields in the work
    for author in entry['authors']:
        # verifiying more than one last name
        # https://github.com/colav/impactu/issues/494
        if len(author.get("last_names", [])) > 1:
            author_normalized = author["full_name"]
            author_normalized = unidecode(author_normalized).lower()
            priotiry_source = ["staff", "scienti", "minciencias"]
            author_found = None
            if affiliation is not None:
                author_found = db['person'].find_one(
                    {"full_name": author_normalized, "affiliations.id": affiliation["id"], "updated.source": {
                        "$in": priotiry_source}},
                    {"full_name": 1, "updated": 1, "affiliations": 1},
                    collation={"locale": "es", "strength": 1}
                )
                if not author_found:
                    author_found = db['person'].find_one(
                        {"full_name": author_normalized,
                            "affiliations.id": affiliation["id"]},
                        {"full_name": 1, "updated": 1, "affiliations": 1},
                        collation={"locale": "es", "strength": 1}
                    )
            if author_found:
                author["id"] = author_found["_id"]
                if affiliation is not None:
                    existing_ids = {
                        item.get("id") for item in author["affiliations"]}
                    if affiliation.get("id") not in existing_ids:
                        author["affiliations"].append(affiliation)
                        existing_ids.add(affiliation.get("id"))
                    units = get_units_affiliations(
                        db, author_found, author["affiliations"],
                        entry.get("year_published"))
                    for unit in units:
                        if unit.get("id") not in existing_ids:
                            author["affiliations"].append(unit)
                            existing_ids.add(unit.get("id"))
        author.pop("first_names", None)
        author.pop("last_names", None)
        author.pop("initials", None)

    dspace_id = next((
        item.get("id") for item in entry.get("external_ids", [])
        if item.get("source") == "dspace" and item.get("id")
    ), None)
    doi = _prepare_dspace_doi(entry)
    outcome = "nuevos"
    if doi:
        existing = (collection.find_one(dspace_query(dspace_id))
                    if dspace_id else None)
        if existing and not work_titles_match(entry.get("titles"), existing.get("titles")):
            existing = None
        owner = find_work_by_doi(collection, doi, titles=entry.get("titles"),
                                 allow_distinct_titles=True)
        if existing and owner and existing["_id"] != owner["_id"]:
            raise AmbiguousWorkDOI(
                f"DSpace record {dspace_id} and DOI {doi} identify different works")
        existing = existing or owner
        if existing:
            work_id = None
        else:
            work_id, existing = insert_work_by_doi(collection, entry,
                                                  allow_distinct_titles=True)
        if existing:
            process_one_update(entry, existing, affiliation, db,
                               collection, verbose)
            work_id = existing["_id"]
            outcome = "actualizados"
    elif dspace_id:
        def upsert_by_dspace_id():
            response = collection.update_one(
                dspace_query(dspace_id), insert_only_update(entry), upsert=True
            )
            work_id = response.upserted_id
            if work_id is None:
                existing = collection.find_one(
                    dspace_query(dspace_id), {"_id": 1})
                work_id = existing.get("_id") if existing else None
            return work_id

        if lock_dspace_id:
            with _dspace_lock(dspace_id):
                work_id = upsert_by_dspace_id()
        else:
            work_id = upsert_by_dspace_id()
    else:
        response = collection.insert_one(entry)
        work_id = response.inserted_id
    ensure_es_work(entry, work_id, es_handler, es_semaphore)
    return outcome


@skip_work_doi_conflicts
def process_one(dspace_reg, affiliation, base_url, db, collection, empty_work,
                es_handler, similarity, thresholds, verbose=0,
                es_semaphore=None, repository=None):
    """
    Dspace record is parsed and processed to be inserted/updated in the works collection.

    Parameters:
    -----------
    dspace_reg : dict
        dspace record.
    affiliation : dict | None
        affiliation record, or None if affiliation is not found.
    base_url : str
        base url of the dspace instance.
    db : pymongo.database.Database
        database object to kahi(ETL) database.
    collection : pymongo.collection.Collection
        collection object to kahi(ETL) database.
    empty_work : dict
        empty work record.
    es_handler : elasticsearch.Elasticsearch
        elasticsearch handler.
    similarity : bool
        if True, it will search for similar works in the works collection.
    verbose : int
        verbosity level.
    repository : dict | None
        repository metadata for source_ref (name, url, affiliation, source_id).
    """
    dspace_id = dspace_reg.get("_id")
    if not dspace_id:
        return "omitidos"
    existing = collection.find_one(dspace_query(dspace_id))
    doi = canonical_work_doi(get_doi(dspace_reg))
    if existing and not doi:
        ensure_es_work(existing, existing["_id"], es_handler, es_semaphore)
        return "existentes"
    # processing bad dois as well
    entry = parse_dspace(dspace_reg, empty_work,
                         base_url, verbose, repository=repository)
    entry["titles"] = [
        title for title in entry.get("titles", [])
        if isinstance(title.get("title"), str) and title["title"].strip()
    ]
    if len(entry["titles"]) == 0:
        if verbose > 4:
            print(
                "No title found in dspace record {}".format(
                    dspace_reg["_id"]))
        return "omitidos"
    set_source(entry, db)  # setting source to entry
    if doi:
        # DOI lookup also precedes title similarity for theses with DOI.
        if existing and not work_titles_match(entry.get("titles"), existing.get("titles")):
            existing = None
        with _dspace_lock(doi):
            owner = find_work_by_doi(collection, doi, titles=entry.get("titles"),
                                     allow_distinct_titles=True)
            if owner and existing and owner["_id"] != existing["_id"]:
                raise AmbiguousWorkDOI(
                    f"DSpace record {dspace_id} and DOI {doi} identify different works")
            target = owner or existing
            if target:
                process_one_update(entry, target, affiliation, db,
                                   collection, verbose)
                ensure_es_work(target, target["_id"], es_handler, es_semaphore)
                outcome = "actualizados"
            else:
                outcome = process_one_insert(entry, affiliation, db, collection,
                                             es_handler, verbose, es_semaphore,
                                             lock_dspace_id=False)
        return outcome
    if existing:
        ensure_es_work(existing, existing["_id"], es_handler, es_semaphore)
        return "existentes"
    # setting affiliation to authors
    # set_affiliation(entry, affiliation, db['person'])
    if similarity:
        if es_handler is None:
            return process_one_insert(entry, affiliation, db,
                                      collection, es_handler, verbose, es_semaphore)
        work = {}
        work["title"] = entry["titles"][0]["title"]
        work["source"] = (
            entry["source"]["name"] if "name" in entry["source"].keys() else ""
        )
        work["year"] = entry["year_published"] if entry["year_published"] else "0"
        work["volume"] = (
            entry["bibliographic_info"]["volume"]
            if "volume" in entry["bibliographic_info"].keys()
            else ""
        )
        work["issue"] = (
            entry["bibliographic_info"]["issue"]
            if "issue" in entry["bibliographic_info"].keys()
            else ""
        )
        work["first_page"] = (
            entry["bibliographic_info"]["first_page"]
            if "first_page" in entry["bibliographic_info"].keys()
            else ""
        )
        work["last_page"] = (
            entry["bibliographic_info"]["last_page"]
            if "last_page" in entry["bibliographic_info"].keys()
            else ""
        )
        authors = []
        for author in entry["authors"]:
            if len(authors) >= 5:
                break
            if "full_name" in author.keys():
                authors.append(str_normilize(author["full_name"]))
        work["authors"] = authors
        work["provenance"] = "dspace"
        responses = run_es_operation(es_semaphore, es_handler.search_work,
                                     title=str_normilize(work["title"]),
                                     source=work["source"],
                                     year=str(work["year"]),
                                     authors=authors,
                                     volume=work["volume"],
                                     issue=work["issue"],
                                     page_start=work["first_page"],
                                     page_end=work["last_page"],
                                     use_es_thold=True,
                                     es_thold=0,
                                     hits=20
                                     )
        if responses:
            found = False
            for response in responses:
                found = check_work(
                    work["title"], authors, response, thresholds)
                if found:
                    colav_reg = find_work_by_es_id(
                        collection, response.get("_id"))
                    if colav_reg:
                        process_one_update(
                            entry, colav_reg, affiliation, db, collection,
                            verbose)
                        return "actualizados"
                    else:
                        if verbose > 4:
                            print(
                                "Register with {} not found in mongodb".format(
                                    response["_id"]))
                        found = False
                        continue
            if not found:
                return process_one_insert(
                    entry,
                    affiliation,
                    db,
                    collection,
                    es_handler,
                    verbose,
                    es_semaphore)
        else:  # insert new register
            return process_one_insert(entry, affiliation, db,
                                      collection, es_handler, verbose, es_semaphore)
    else:
        print(f"WARNING: invalid doi found in dspace record {dspace_reg['_id']} ")
        return "omitidos"
