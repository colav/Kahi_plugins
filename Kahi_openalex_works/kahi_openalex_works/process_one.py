
from kahi_openalex_works.parser import parse_openalex
from time import time
from bson import ObjectId


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
    # open access
    if "is_open_acess" not in colav_reg["bibliographic_info"].keys():
        if "is_open_access" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["is_open_acess"] = entry["bibliographic_info"]["is_open_access"]
    if "open_access_status" not in colav_reg["bibliographic_info"].keys():
        if "open_access_status" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["open_access_status"] = entry["bibliographic_info"]["open_access_status"]
    # external urls
    urls_sources = [url["source"]
                    for url in colav_reg["external_urls"]]
    if "oa" not in urls_sources:
        oa_url = None
        for ext in entry["external_urls"]:
            if ext["source"] == "oa":
                oa_url = ext["url"]
                break
        if oa_url:
            colav_reg["external_urls"].append(
                {"source": "oa", "url": entry["external_urls"][0]["url"]})
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
            "citations_by_year": colav_reg["citations_by_year"]
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

        es_handler.insert_work(_id=str(response.inserted_id), work=work)


def process_one(oa_reg, db, collection, empty_work, es_handler, verbose=0):
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
    doi = None
    # register has doi
    if "doi" in oa_reg.keys():
        if oa_reg["doi"]:
            doi = oa_reg["doi"].split(".org/")[-1].lower()
    if doi:
        # is the doi in colavdb?
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
            else:  # insert new register
                if verbose > 4:
                    print("INFO: found no register in elasticsearch")
                process_one_insert(oa_reg, db, collection,
                                   empty_work, es_handler, verbose=0)
        else:
            if verbose > 4:
                print("No elasticsearch index provided")
