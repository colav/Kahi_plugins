
from kahi_openalex_works.parser import parse_openalex
from time import time
from bson import ObjectId
from pymongo import MongoClient
from mohan.Similarity import Similarity


def get_units_affiations(db, author_db, affiliations):
    """
    Method to get the units of an author in a register. ex: faculty, department and group.

    Parameters:
    ----------
    db : pymongo.database.Database
        Database connection to colav database.
    author_db : dict
        record from person
    affiliations : list
        list of affiliations from the parse_openalex method

    Returns:
    -------
    list
        list of units of an author (entries from using affiliations)
    """
    institution_id = None
    # verifiying univeristy
    for j, aff in enumerate(affiliations):
        aff_db = None
        if "external_ids" in aff.keys():
            for ext in aff["external_ids"]:
                aff_db = db["affiliations"].find_one(
                    {"external_ids.id": ext["id"]}, {"_id": 1, "types": 1})
                if aff_db:
                    types = [i["type"] for i in aff_db["types"]]
                    if "group" in types or "department" in types or "faculty" in types:
                        aff_db = None
                        continue
                    else:
                        break
        if aff_db:
            count = db["person"].count_documents(
                {"_id": author_db["_id"], "affiliations.id": aff_db["_id"]})
            if count > 0:
                institution_id = aff_db["_id"]
                break
    units = []
    for aff in author_db["affiliations"]:
        if aff["id"] == institution_id:
            continue
        count = db["affiliations"].count_documents(
            {"_id": aff["id"], "relations.id": institution_id})
        if count > 0:
            types = [i["type"] for i in aff["types"]]
            if "department" in types or "faculty" in types:
                units.append(aff)
    return units


def process_one_update(oa_reg, colav_reg, db, collection, empty_work, verbose=0):
    """
    Method to update a register in the database if it is found in the openalex database.
    This means that the register is already on the database and it is being updated with new information.

    Parameters
    ----------
    oa_reg : dict
        A record from openalex
    colav_reg : dict
        Register from the colav database (kahi database for impactu)
    db : pymongo.database.Database
        Database connection to colav database.
    collection : pymongo.collection.Collection
        Collection to insert the register. Colav database collection for works.
    empty_work : dict
        A template for a work entry, with empty fields.
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    # updated
    for upd in colav_reg["updated"]:
        if upd["source"] == "openalex":
            return None  # Register already on db
            # Could be updated with new information when openalex database changes
    entry = parse_openalex(oa_reg, empty_work.copy(), verbose=verbose)
    colav_reg["updated"].append(
        {"source": "openalex", "time": int(time())})
    # titles
    colav_reg["titles"].extend(entry["titles"])
    # external_ids
    ext_ids = [ext["id"] for ext in colav_reg["external_ids"]]
    for ext in entry["external_ids"]:
        if ext["id"] not in ext_ids:
            colav_reg["external_ids"].append(ext)
            ext_ids.append(ext["id"])
    # types
    colav_reg["types"].extend(entry["types"])
    # open access info
    colav_reg["open_acess"] = entry["open_acess"]
    # external urls
    urls_sources = [url["source"]
                    for url in colav_reg["external_urls"]]
    if "open_access" not in urls_sources:
        oa_url = None
        for ext in entry["external_urls"]:
            if ext["source"] == "open_access":
                oa_url = ext["url"]
                break
        if oa_url:
            colav_reg["external_urls"].append(
                {"provenance": "openalex", "source": "open_access", "url": entry["external_urls"][0]["url"]})
    # citations by year
    if "counts_by_year" in entry.keys():
        colav_reg["citations_by_year"] = entry["counts_by_year"]
    # citations count
    if entry["citations_count"]:
        colav_reg["citations_count"].extend(entry["citations_count"])
    # subjects
    subject_list = []
    for subjects in entry["subjects"]:
        for i, subj in enumerate(subjects["subjects"]):
            for ext in subj["external_ids"]:
                sub_db = db["subjects"].find_one(
                    {"external_ids.id": ext["id"]})
                if sub_db:
                    name = sub_db["names"][0]["name"]
                    for n in sub_db["names"]:
                        if n["lang"] == "en":
                            name = n["name"]
                            break
                        elif n["lang"] == "es":
                            name = n["name"]
                    subject_list.append({
                        "id": sub_db["_id"],
                        "name": name,
                        "level": sub_db["level"]
                    })
                    break
    colav_reg["subjects"].append(
        {"source": "openalex", "subjects": subject_list})

    # authors
    for i, author in enumerate(entry["authors"]):
        author_db = None
        for ext in author["external_ids"]:
            author_db = db["person"].find_one(
                {"external_ids.id": ext["id"]})
            if author_db:
                break
        if author_db:
            aff_units = get_units_affiations(
                db, author_db, author["affiliations"])
            for aff_unit in aff_units:
                if aff_unit not in author["affiliations"]:
                    colav_reg["authors"][i]["affiliations"].append(aff_unit)
    collection.update_one(
        {"_id": colav_reg["_id"]},
        {"$set": {
            "updated": colav_reg["updated"],
            "titles": colav_reg["titles"],
            "external_ids": colav_reg["external_ids"],
            "types": colav_reg["types"],
            "bibliographic_info": colav_reg["bibliographic_info"],
            "external_urls": colav_reg["external_urls"],
            "subjects": colav_reg["subjects"],
            "citations_count": colav_reg["citations_count"],
            "citations_by_year": colav_reg["citations_by_year"],
            "authors": colav_reg["authors"]
        }}
    )


def process_one_insert(oa_reg, db, collection, empty_work, es_handler, verbose=0):
    """
    ""
    Function to insert a new register in the database if it is not found in the colav(kahi works) database.
    This means that the register is not on the database and it is being inserted.

    For similarity purposes, the register is also inserted in the elasticsearch index,
    all the elastic search fields are filled with the information from the register and it is
    handled by Mohan's Similarity class.

    The register is also linked to the source of the register, and the authors and affiliations are searched in the database.

    Parameters
    ----------
    scholar_reg : dict
        Register from the openalex database
    db : pymongo.database.Database
        Database where the colav collections are stored, used to search for authors and affiliations.
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    es_handler : Similarity
        Elasticsearch handler to insert the register in the elasticsearch index, Mohan's Similarity class.
    verbose : int, optional
        Verbosity level. The default is 0
    """

    # parse
    entry = parse_openalex(oa_reg, empty_work.copy(), verbose=verbose)
    # link
    source_db = None
    if entry["source"]:
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
        if entry["source"]:
            if len(entry["source"]["external_ids"]) == 0:
                print(
                    f'Register with doi: {oa_reg["doi"]} does not provide a source')
            else:
                print("No source found for\n\t",
                      entry["source"]["external_ids"])
            entry["source"] = {
                "id": "",
                "name": entry["source"]["name"]
            }
    for subjects in entry["subjects"]:
        for i, subj in enumerate(subjects["subjects"]):
            for ext in subj["external_ids"]:
                sub_db = db["subjects"].find_one(
                    {"external_ids.id": ext["id"]})
                if sub_db:
                    name = sub_db["names"][0]["name"]
                    for n in sub_db["names"]:
                        if n["lang"] == "en":
                            name = n["name"]
                            break
                        elif n["lang"] == "es":
                            name = n["name"]
                    entry["subjects"][0]["subjects"][i] = {
                        "id": sub_db["_id"],
                        "name": name,
                        "level": sub_db["level"]
                    }
                    break

    # search authors and affiliations in db
    for i, author in enumerate(entry["authors"]):
        author_db = None
        for ext in author["external_ids"]:  # given priority to scienti person
            author_db = db["person"].find_one(
                {"external_ids.id": ext["id"], "updated.source": "scienti"})
            if author_db:
                break
        if not author_db:  # if not found ids with scienti, let search it with openalex
            for ext in author["external_ids"]:
                author_db = db["person"].find_one(
                    {"external_ids.id": ext["id"], "updated.source": "openalex"})
                if author_db:
                    break
        if not author_db:  # if not found ids with scienti/openalex, let search it with other sources
            for ext in author["external_ids"]:
                author_db = db["person"].find_one(
                    {"external_ids.id": ext["id"]})
                if author_db:
                    break
        if author_db:
            sources = [ext["source"]
                       for ext in author_db["external_ids"]]
            ids = [ext["id"] for ext in author_db["external_ids"]]
            for ext in author["external_ids"]:
                if ext["id"] not in ids:
                    author_db["external_ids"].append(ext)
                    sources.append(ext["source"])
                    ids.append(ext["id"])
            entry["authors"][i] = {
                "id": author_db["_id"],
                "full_name": author_db["full_name"],
                "affiliations": author["affiliations"]
            }
            aff_units = get_units_affiations(
                db, author_db, author["affiliations"])
            for aff_unit in aff_units:
                if aff_unit not in author["affiliations"]:
                    author["affiliations"].append(aff_unit)

            if "external_ids" in author.keys():
                del (author["external_ids"])
        else:
            if verbose > 1:
                print(
                    f"WARNING: author not found in db {author} maybe deleted author in openalex, trying to find by name")
            author_db = db["person"].find_one(
                {"full_name": author["full_name"]})
            if author_db:
                sources = [ext["source"]
                           for ext in author_db["external_ids"]]
                ids = [ext["id"] for ext in author_db["external_ids"]]
                for ext in author["external_ids"]:
                    if ext["id"] not in ids:
                        author_db["external_ids"].append(ext)
                        sources.append(ext["source"])
                        ids.append(ext["id"])
                entry["authors"][i] = {
                    "id": author_db["_id"],
                    "full_name": author_db["full_name"],
                    "affiliations": author["affiliations"]
                }
                aff_units = get_units_affiations(
                    db, author_db, author["affiliations"])
                for aff_unit in aff_units:
                    if aff_unit not in author["affiliations"]:
                        author["affiliations"].append(aff_unit)

            else:
                entry["authors"][i] = {
                    "id": "",
                    "full_name": author["full_name"],
                    "affiliations": author["affiliations"]
                }
        for j, aff in enumerate(author["affiliations"]):
            aff_db = None
            if "types" in aff.keys():  # if not types it not group, department or faculty
                types = [i["type"] for i in aff["types"]]
                if "group" in types or "department" in types or "faculty" in types:
                    continue
            if "external_ids" in aff.keys():
                for ext in aff["external_ids"]:
                    aff_db = db["affiliations"].find_one(
                        {"external_ids.id": ext["id"]})
                    if aff_db:
                        break
            if aff_db:
                name = aff_db["names"][0]["name"]
                for n in aff_db["names"]:
                    if n["source"] == "ror":
                        name = n["name"]
                        break
                    if n["lang"] == "en":
                        name = n["name"]
                    if n["lang"] == "es":
                        name = n["name"]
                entry["authors"][i]["affiliations"][j] = {
                    "id": aff_db["_id"],
                    "name": name,
                    "types": aff_db["types"]
                }
            else:
                aff_db = db["affiliations"].find_one(
                    {"names.name": aff["name"]})
                if aff_db:
                    name = aff_db["names"][0]["name"]
                    for n in aff_db["names"]:
                        if n["source"] == "ror":
                            name = n["name"]
                            break
                        if n["lang"] == "en":
                            name = n["name"]
                        if n["lang"] == "es":
                            name = n["name"]
                    entry["authors"][i]["affiliations"][j] = {
                        "id": aff_db["_id"],
                        "name": name,
                        "types": aff_db["types"]
                    }
                else:
                    entry["authors"][i]["affiliations"][j] = {
                        "id": "",
                        "name": aff["name"],
                        "types": []
                    }

    entry["author_count"] = len(entry["authors"])
    # insert in mongo
    inserted_id = None
    openalex_id = oa_reg["id"] if "id" in oa_reg.keys() else None
    if openalex_id:
        response = collection.update_one(
            {"external_ids": {"$elemMatch": {"source": "openalex", "id": openalex_id}}},
            {"$setOnInsert": entry},
            upsert=True
        )
        inserted_id = response.upserted_id
    else:
        response = collection.insert_one(entry)
        inserted_id = response.inserted_id
    # insert in elasticsearch
    if es_handler and inserted_id:
        work = {}
        work["title"] = entry["titles"][0]["title"]
        work["source"] = entry["source"]["name"] if "name" in entry["source"].keys() else ""
        work["year"] = entry["year_published"]
        work["volume"] = entry["bibliographic_info"]["volume"] if "volume" in entry["bibliographic_info"].keys() else ""
        work["issue"] = entry["bibliographic_info"]["issue"] if "issue" in entry["bibliographic_info"].keys() else ""
        work["first_page"] = entry["bibliographic_info"]["first_page"] if "first_page" in entry["bibliographic_info"].keys() else ""
        work["last_page"] = entry["bibliographic_info"]["last_page"] if "last_page" in entry["bibliographic_info"].keys() else ""
        authors = []
        for author in entry['authors']:
            if len(authors) >= 5:
                break
            if "full_name" in author.keys():
                authors.append(author["full_name"])
        work["authors"] = authors
        work["provenance"] = "openalex"

        es_handler.insert_work(_id=str(inserted_id), work=work)


def process_one(oa_reg, config, empty_work, client, es_handler, backend, verbose=0):
    """
    Function to process a single register from the scholar database.
    This function is used to insert or update a register in the colav(kahi works) database.

    Parameters
    ----------
    oa_reg : dict
        Register from the openalex database
    db : pymongo.database.Database
        Database where the colav collections are stored, used to search for authors and affiliations.
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    es_handler : Similarity
        Elasticsearch handler to insert the register in the elasticsearch index, Mohan's Similarity class.
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    if backend != "threading":
        client = MongoClient(config["database_url"])
    db = client[config["database_name"]]
    collection = db["works"]

    if backend != "threading":
        es_handler = None
        if "es_index" in config["openalex_works"].keys() and "es_url" in config["openalex_works"].keys() and "es_user" in config["openalex_works"].keys() and "es_password" in config["openalex_works"].keys():
            es_index = config["openalex_works"]["es_index"]
            es_url = config["openalex_works"]["es_url"]
            if config["openalex_works"]["es_user"] and config["openalex_works"]["es_password"]:
                es_auth = (config["openalex_works"]["es_user"],
                           config["openalex_works"]["es_password"])
            else:
                es_auth = None
            es_handler = Similarity(
                es_index, es_uri=es_url, es_auth=es_auth, es_req_timeout=300, es_max_retries=5, es_retry_on_timeout=True)
        else:
            es_handler = None
            print("WARNING: No elasticsearch configuration provided")

    colav_reg = None
    openalex_id = oa_reg["id"] if "id" in oa_reg.keys() else None
    if openalex_id:
        colav_reg = collection.find_one(
            {"external_ids": {"$elemMatch": {"source": "openalex", "id": openalex_id}}}
        )

    doi = oa_reg["doi"]
    if colav_reg:
        process_one_update(
            oa_reg, colav_reg, db, collection, empty_work, verbose=verbose)
    elif doi:
        # fallback: is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id": doi})
        if colav_reg:  # update the register
            process_one_update(
                oa_reg, colav_reg, db, collection, empty_work, verbose=verbose)
        else:  # insert a new register
            process_one_insert(
                oa_reg, db, collection, empty_work, es_handler, verbose=verbose)
    else:  # does not have a doi identifier
        # elasticsearch section
        if es_handler:
            # Search in elasticsearch
            authors = []
            for author in oa_reg['authorships']:
                if "display_name" in author["author"].keys():
                    authors.append(author["author"]["display_name"])
            source = ""
            if oa_reg["primary_location"]:
                if "source" in oa_reg["primary_location"].keys():
                    if oa_reg["primary_location"]["source"]:
                        if "display_name" in oa_reg["primary_location"]["source"].keys():
                            source = oa_reg["primary_location"]["source"]["display_name"]
            response = es_handler.search_work(
                title=oa_reg["title"],
                source=source,
                year=str(oa_reg["publication_year"]),
                authors=authors,
                volume=oa_reg["biblio"]["volume"],
                issue=oa_reg["biblio"]["issue"],
                page_start=oa_reg["biblio"]["first_page"],
                page_end=oa_reg["biblio"]["last_page"],
            )

            if response:  # register already on db... update accordingly
                found = collection.count_documents(
                    # we are assuming here, all works of apenalex are unique.
                    # to avoid things like https://github.com/colav/impactu/issues/181
                    {"external_ids.id": oa_reg["id"]})
                if found:
                    colav_reg = collection.find_one(
                        {"_id": ObjectId(response["_id"])})
                    if colav_reg:
                        process_one_update(oa_reg, colav_reg, db,
                                           collection, empty_work, verbose=verbose)
                    else:
                        if verbose > 4:
                            print("Register with {} not found in mongodb".format(
                                response["_id"]))
                            print(response)
                else:
                    process_one_insert(oa_reg, db, collection,
                                       empty_work, es_handler, verbose=0)

            else:  # insert new register
                if verbose > 4:
                    print("INFO: found no register in elasticsearch")
                process_one_insert(oa_reg, db, collection,
                                   empty_work, es_handler, verbose=0)
        else:
            if verbose > 4:
                print("No elasticsearch index provided")
    if backend != "threading":
        client.close()
        if es_handler:
            es_handler.close()
