from kahi_impactu_utils.Utils import doi_processor
from time import time
from kahi_scholar_works.parser import parse_scholar
from bson import ObjectId


def process_one_update(scholar_reg, colav_reg, collection, empty_work, verbose=0):
    """
    Method to update a register in the database if it is found in the scholar database.
    This means that the register is already on the database and it is being updated with new information.


    Parameters
    ----------
    scholar_reg : dict
        Register from the scholar database
    colav_reg : dict
        Register from the colav database (kahi database for impactu)
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    # updated
    for upd in colav_reg["updated"]:
        if upd["source"] == "scholar":
            return None  # Register already on db
            # Could be updated with new information when scholar database changes
    entry = parse_scholar(
        scholar_reg, empty_work.copy(), verbose=verbose)
    colav_reg["updated"].append(
        {"source": "scholar", "time": int(time())})
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
    # bibliographic info
    if "start_page" not in colav_reg["bibliographic_info"].keys():
        if "start_page" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["start_page"] = entry["bibliographic_info"]["start_page"]
    if "end_page" not in colav_reg["bibliographic_info"].keys():
        if "end_page" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["end_page"] = entry["bibliographic_info"]["end_page"]
    if "volume" not in colav_reg["bibliographic_info"].keys():
        if "volume" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["volume"] = entry["bibliographic_info"]["volume"]
    if "issue" not in colav_reg["bibliographic_info"].keys():
        if "issue" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["issue"] = entry["bibliographic_info"]["issue"]
    # bibtex
    if "bibtex" in entry["bibliographic_info"].keys():
        colav_reg["bibliographic_info"]["bibtex"] = entry["bibliographic_info"]["bibtex"]

    # external urls
    urls_sources = [url["source"]
                    for url in colav_reg["external_urls"]]
    for ext in entry["external_urls"]:
        if ext["url"] not in urls_sources:
            colav_reg["external_urls"].append(ext)
            urls_sources.append(ext["url"])

    # citations count
    if entry["citations_count"]:
        colav_reg["citations_count"].extend(entry["citations_count"])

    collection.update_one(
        {"_id": colav_reg["_id"]},
        {"$set": {
            "updated": colav_reg["updated"],
            "titles": colav_reg["titles"],
            "external_ids": colav_reg["external_ids"],
            "types": colav_reg["types"],
            "bibliographic_info": colav_reg["bibliographic_info"],
            "external_urls": colav_reg["external_urls"],
            "citations_count": colav_reg["citations_count"]
        }}
    )


def process_one_insert(scholar_reg, db, collection, empty_work, es_handler, verbose=0):
    """
    Function to insert a new register in the database if it is not found in the colav(kahi works) database.
    This means that the register is not on the database and it is being inserted.

    For similarity purposes, the register is also inserted in the elasticsearch index,
    all the elastic search fields are filled with the information from the register and it is
    handled by Mohan's Similarity class.

    The register is also linked to the source of the register, and the authors and affiliations are searched in the database.

    Parameters
    ----------
    scholar_reg : dict
        Register from the scholar database
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
    entry = parse_scholar(
        scholar_reg, empty_work.copy(), verbose=verbose)
    # link
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
                    f'Register with doi: {scholar_reg["doi"]} does not provide a source')
        else:
            if verbose > 4:
                print("No source found for\n\t",
                      entry["source"]["external_ids"])
        entry["source"] = {
            "id": "",
            "name": entry["source"]["name"]
        }

    # search authors and affiliations in db
    for i, author in enumerate(entry["authors"]):
        author_db = None
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
            if "external_ids" in author.keys():
                del (author["external_ids"])
        else:
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
            else:
                entry["authors"][i] = {
                    "id": "",
                    "full_name": author["full_name"],
                    "affiliations": author["affiliations"]
                }
        for j, aff in enumerate(author["affiliations"]):
            aff_db = None
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
    response = collection.insert_one(entry)
    # insert in elasticsearch
    if es_handler:
        work = {}
        work["title"] = entry["titles"][0]["title"]
        work["source"] = entry["source"]["name"]
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
        work["provenance"] = "scholar"

        es_handler.insert_work(_id=str(response.inserted_id), work=work)
    else:
        if verbose > 4:
            print("No elasticsearch index provided")


def process_one(scholar_reg, db, collection, empty_work, similarity, es_handler, verbose=0):
    """
    Function to process a single register from the scholar database.
    This function is used to insert or update a register in the colav(kahi works) database.

    Parameters
    ----------
    scholar_reg : dict
        Register from the scholar database
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
    doi = None
    # register has doi
    if scholar_reg["doi"]:
        if isinstance(scholar_reg["doi"], str):
            doi = doi_processor(scholar_reg["doi"])
    if doi:
        # is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id": doi})
        if colav_reg:  # update
            process_one_update(
                scholar_reg, colav_reg, collection, empty_work, verbose=verbose)
        else:  # insert a new register
            process_one_insert(
                scholar_reg, db, collection, empty_work, es_handler, verbose=verbose)
    elif similarity:
        if es_handler:
            # Search in elasticsearch
            year = str(scholar_reg["year"])
            response = es_handler.search_work(
                title=scholar_reg["title"],
                source=scholar_reg["journal"],
                year=year if year != "" else "-1",  # FIXME, term in ES have to be fixed
                authors=[auth.split(", ")[-1] + " " + auth.split(", ")[0]
                         for auth in scholar_reg["author"].split(" and ")],
                volume=scholar_reg["volume"] if "volume" in scholar_reg.keys(
                ) else "",
                issue=scholar_reg["issue"] if "issue" in scholar_reg.keys(
                ) else "",
                page_start=scholar_reg["pages"].split(
                    "--")[0] if "pages" in scholar_reg.keys() else "",
                page_end=scholar_reg["pages"].split(
                    "--")[-1] if "pages" in scholar_reg.keys() else "",
            )

            if response:  # register already on db... update accordingly
                colav_reg = collection.find_one(
                    {"_id": ObjectId(response["_id"])})
                if colav_reg:
                    process_one_update(scholar_reg, colav_reg,
                                       collection, empty_work, verbose=0)
                else:
                    if verbose > 4:
                        print("Register with {} not found in mongodb".format(
                            response["_id"]))
                        print(response)
            else:  # insert new register
                if verbose > 4:
                    print("INFO: found no register in elasticsearch")
                process_one_insert(scholar_reg, db, collection,
                                   empty_work, es_handler, verbose=0)
        else:
            if verbose > 4:
                print("No elasticsearch index provided")
