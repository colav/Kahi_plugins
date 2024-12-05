from kahi_minciencias_opendata_works.parser import parse_minciencias_opendata
from kahi_impactu_utils.Utils import compare_author
from thefuzz import process, fuzz
from unidecode import unidecode
from time import time
from bson import ObjectId
from re import search, sub


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
        list of affiliations from the parse_minciencias_opendata method

    Returns:
    -------
    list
        list of units of an author (entries from using affiliations)
    """
    institution_id = None
    # verifiying univeristy
    for j, aff in enumerate(affiliations):
        aff_db = db["affiliations"].find_one(
            {"_id": aff["id"]}, {"_id": 1, "types": 1})
        if aff_db:
            types = [i["type"] for i in aff_db["types"]]
            if "group" in types or "department" in types or "faculty" in types:
                aff_db = None
                continue
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


def process_one_update(openadata_reg, colav_reg, db, collection, empty_work, verbose=0):
    """
    Method to update a register in the kahi database from minciencias opendata database if it is found.
    This means that the register is already on the kahi database and it is being updated with new information.


    Parameters
    ----------
    openadata_reg : dict
        Register from the minciencias opendata database
    colav_reg : dict
        Register from the colav database (kahi database for impactu)
    db : pymongo.database.Database
        Database where the colav collections are stored, used to search for authors and affiliations.
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    entry = parse_minciencias_opendata(
        openadata_reg, empty_work.copy(), verbose=verbose)
    # updated
    for upd in colav_reg["updated"]:
        if upd["source"] == "minciencias":
            return None  # Register already on db
    colav_reg["updated"].append(
        {"source": "minciencias", "time": int(time())})
    # titles
    if 'minciencias' not in [title['source'] for title in colav_reg["titles"]]:
        lang = entry["titles"][0]["lang"]
        colav_reg["titles"].append(
            {"title": entry["titles"][0]["title"], "lang": lang, "source": "minciencias"})
    # external_ids
    exts = [ext_ for ext_ in colav_reg["external_ids"]]
    for ext in entry["external_ids"]:
        if ext not in exts:
            colav_reg["external_ids"].append(ext)
            exts.append(ext["id"])
    # types
    types = [ext["source"] for ext in colav_reg["types"]]
    for typ in entry["types"]:
        if typ["source"] not in types:
            colav_reg["types"].append(typ)
    # authors
    minciencias_author = ""
    if "authors" in entry.keys():
        if entry["authors"]:
            minciencias_author = entry["authors"][0]
    author_db = None
    if minciencias_author:
        author_found = False
        if "external_ids" in minciencias_author.keys() and minciencias_author["affiliations"]:
            for ext in minciencias_author["external_ids"]:
                author_db = db["person"].find_one(
                    {"external_ids.id": ext["id"]})
                if author_db:
                    group_id = minciencias_author["affiliations"][0]['external_ids'][0]['id']

                    affiliations_db = db["affiliations"].find_one(
                        {"external_ids.source": "scienti", "external_ids.id": group_id})
                    if not affiliations_db:
                        affiliations_db = db["affiliations"].find_one(
                            {"external_ids.id": group_id})

                    if affiliations_db:
                        for i, author in enumerate(colav_reg["authors"]):
                            if author_db["_id"] != author["id"]:
                                continue
                            # Adding group to existing author in colav register
                            author_affiliations = [str(aff['id'])
                                                   for aff in author['affiliations']]
                            if str(affiliations_db["_id"]) not in author_affiliations:
                                author["affiliations"].append(
                                    {
                                        "id": affiliations_db["_id"],
                                        "name": affiliations_db["names"][0]["name"],
                                        "types": affiliations_db["types"]})
                                author_found = True
                                if verbose > 4:
                                    print("group added to author: {}".format(
                                        affiliations_db["names"][0]["name"]))
                                break
                            else:
                                author_found = True
                                if verbose > 4:
                                    print("group already in author")
                                break

                        if not author_found:
                            affiliation_match = False
                            for i, author in enumerate(colav_reg["authors"]):
                                if author['id'] == "":
                                    if verbose >= 4:
                                        print(
                                            f"WARNING: author with id '' found in colav register: {author}")
                                    continue
                                # only the name can be compared, because we dont have the affiliation of the author from the paper in author_others
                                author_reg = db['person'].find_one(
                                    # this is required to get  first_names and last_names
                                    {'_id': author['id']}, {"_id": 1, "full_name": 1, "first_names": 1, "last_names": 1, "initials": 1})

                                name_match = compare_author(
                                    author_reg, author_db, len(colav_reg["authors"]))
                                # If the author matches, replace the id and full_name of the record in process so as not to break the aggregation of affiliations.
                                if name_match:
                                    author["id"] = author_db["_id"]
                                    author["full_name"] = author_db["full_name"]

                                if author['affiliations']:
                                    affiliations_person = [str(aff['id'])
                                                           for aff in author_db['affiliations']]
                                    author_affiliations = [str(aff['id'])
                                                           for aff in author['affiliations']]
                                    affiliation_match = any(
                                        affil in author_affiliations for affil in affiliations_person)
                                if name_match and affiliation_match:
                                    author["affiliations"].append({
                                        "id": affiliations_db["_id"],
                                        "name": affiliations_db["names"][0]["name"].strip(),
                                        "types": affiliations_db["types"]})
                                    author_found = True
                                    break

                        if not author_found:
                            colav_reg["authors"].append(
                                {"id": author_db["_id"], "full_name": author_db["full_name"], "affiliations": [
                                    {
                                        "id": affiliations_db["_id"],
                                        "name": affiliations_db["names"][0]["name"],
                                        "types": affiliations_db["types"]
                                    }
                                ]}
                            )

                else:
                    if verbose > 4:
                        print("No author in db with external id")
        else:
            if verbose > 4:
                print("No author data")
    # groups
    group_id = openadata_reg["cod_grupo_gr"]
    rgroup = db["affiliations"].find_one({"external_ids.id": group_id})
    if rgroup:
        found = False
        for group in colav_reg["groups"]:
            if group["id"] == rgroup["_id"]:
                found = True
                break
        if not found:
            colav_reg["groups"].append(
                {"id": rgroup["_id"], "name": rgroup["names"][0]["name"]})

        # Adding group relation affiliation to the author affiliations
        if author_db and rgroup["relations"]:
            for author in colav_reg["authors"]:
                if author["id"] == author_db["_id"]:
                    affs = [aff["id"] for aff in author["affiliations"]]
                    for relation in rgroup["relations"]:
                        types = []
                        if "types" in relation.keys() and relation["types"]:
                            types = [rel["type"].lower()
                                     for rel in relation["types"]]
                            if "education" in types:
                                if relation["id"] not in affs:
                                    author["affiliations"].append(relation)
                    aff_units = get_units_affiations(
                        db, author_db, author["affiliations"])
                    for aff_unit in aff_units:
                        if aff_unit not in author["affiliations"]:
                            author["affiliations"].append(aff_unit)

                    break

    collection.update_one(
        {"_id": colav_reg["_id"]},
        {"$set": {
            "updated": colav_reg["updated"],
            "titles": colav_reg["titles"],
            "external_ids": colav_reg["external_ids"],
            "types": colav_reg["types"],
            "authors": colav_reg["authors"],
            "groups": colav_reg["groups"]
        }}
    )


def process_one_insert(openadata_reg, db, collection, empty_work, es_handler, verbose=0):
    """
    Function to insert a new register in the database if it is not found in the colav(kahi works) database.
    This means that the register is not on the database and it is being inserted.

    For similarity purposes, the register is also inserted in the elasticsearch index,
    all the elastic search fields are filled with the information from the register and it is
    handled by Mohan's Similarity class.

    The register is also linked to the source of the register, and the authors and affiliations are searched in the database.

    Parameters
    ----------
    openadata_reg : dict
        Register from the minciencias opendata database
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
    # parse
    entry = parse_minciencias_opendata(openadata_reg, empty_work.copy())
    # authors
    minciencias_author = ""
    if "authors" in entry.keys():
        if entry["authors"]:
            minciencias_author = entry["authors"][0]
    if minciencias_author:
        # and minciencias_author["affiliations"]:
        if "external_ids" in minciencias_author.keys():
            for ext in minciencias_author["external_ids"]:
                author_db = db["person"].find_one(
                    {"external_ids.id": ext["id"]})
                if author_db:
                    entry["authors"][0]["id"] = author_db["_id"]
                    entry["authors"][0]["full_name"] = author_db["full_name"]
                    if minciencias_author["affiliations"]:
                        group_id = minciencias_author["affiliations"][0]['external_ids'][0]['id']
                        affiliations_db = db["affiliations"].find_one(
                            {"external_ids.id": group_id})

                        if affiliations_db:
                            if entry['authors'][0]['external_ids'][0]['id'] == ext['id']:
                                entry['authors'][0]["affiliations"].append(
                                    {
                                        "id": affiliations_db["_id"] if affiliations_db else None,
                                        "name": affiliations_db["names"][0]["name"].strip() if affiliations_db else None,
                                        "types": affiliations_db["types"] if affiliations_db else None
                                    }
                                )
                                if len(entry['authors'][0]["affiliations"]) > 1:
                                    entry['authors'][0]["affiliations"].pop(0)
                                if verbose > 4:
                                    print("group added to author: {}".format(
                                        affiliations_db["names"][0]["name"] if affiliations_db else None))
                                break
                        else:
                            print(
                                f"WARNING: group {group_id} not found in affiliation db")
            del entry["authors"][0]["external_ids"]

        else:
            if verbose > 4:
                print("No author data")
    if entry["authors"][0]["full_name"] == "":
        del entry["authors"][0]  # this is an empty author, so it is removed

    entry["author_count"] = len(entry["authors"])

    # group
    group_id = openadata_reg["cod_grupo_gr"]
    rgroup = db["affiliations"].find_one({"external_ids.id": group_id})
    if rgroup:
        found = False
        for group in entry["groups"]:
            if group["id"] == rgroup["_id"]:
                found = True
                break
        if not found:
            entry["groups"].append(
                {"id": rgroup["_id"], "name": rgroup["names"][0]["name"]})

        # Adding group relation affiliation to the author affiliations
        if author_db and rgroup["relations"]:
            for author in entry["authors"]:
                if author["id"] == author_db["_id"]:
                    affs = [aff["id"] for aff in author["affiliations"]]
                    for relation in rgroup["relations"]:
                        types = []
                        if "types" in relation.keys() and relation["types"]:
                            types = [rel["type"].lower()
                                     for rel in relation["types"]]
                            if "education" in types:
                                if relation["id"] not in affs:
                                    author["affiliations"].append(relation)
                    aff_units = get_units_affiations(
                        db, author_db, author["affiliations"])
                    for aff_unit in aff_units:
                        if aff_unit not in author["affiliations"]:
                            author["affiliations"].append(aff_unit)
                    break
    # insert in mongo
    response = collection.insert_one(entry)
    # insert in elasticsearch
    authors = []
    if es_handler:
        work = {}
        work["title"] = entry["titles"][0]["title"]
        work["source"] = ""
        work["year"] = "0"
        work["volume"] = ""
        work["issue"] = ""
        work["first_page"] = ""
        work["last_page"] = ""
        for author in entry['authors']:
            if "full_name" in author.keys():
                if author["full_name"]:
                    authors.append(author["full_name"])
        work["authors"] = authors
        if work["title"]:
            es_handler.insert_work(_id=str(response.inserted_id), work=work)
        else:
            if verbose > 4:
                print("Not enough data for insert in elasticsearch index")
    else:
        if verbose > 4:
            print("No elasticsearch index provided")


def str_normilize(word):
    return unidecode(word).lower().strip().replace(".", "")


def check_work(title_work, authors, response, thresholds):
    author_found = False
    if authors:
        if authors[0] != "":
            _authors = []
            for author in response["_source"]["authors"]:
                _authors.append(str_normilize(author))
            scores = process.extract(str_normilize(
                authors[0]), _authors, scorer=fuzz.partial_ratio)
            for score in scores:
                if score[1] >= thresholds["author_thd"]:
                    author_found = True
                    break
    es_title = response["_source"]["title"]
    if es_title:
        score = fuzz.ratio(str_normilize(title_work),
                           str_normilize(es_title))
        if author_found:
            if score >= thresholds["paper_thd_low"]:
                return True
        else:
            if score >= thresholds["paper_thd_high"]:
                return True
    return False


def process_one(openadata_reg, db, collection, empty_work, es_handler, insert_all, thresholds, verbose=0):
    """
    Function to process a single register from the minciencias opendata database.
    This function is used to insert or update a register in the colav(kahi works) database.

    Parameters
    ----------
    openadata_reg : dict
        Register from the minciencias opendata database
    db : pymongo.database.Database
        Database where the colav collections are stored, used to search for authors and affiliations.
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    es_handler : Similarity
        Elasticsearch handler to insert the register in the elasticsearch index, Mohan's Similarity class.
    insert_all : bool
        Flag to insert all the registers in the minciencias opendata database.
    thresholds : list
        List with the thresholds for the similarity functions.
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    # type id verification
    if "id_producto_pd" in openadata_reg.keys():
        if openadata_reg["id_producto_pd"]:
            COD_RH = ""
            COD_PROD = ""
            product_id = openadata_reg["id_producto_pd"]
            match = search(r'(\d{9,11})-(\d{1,7})$', product_id)
            if match:
                COD_RH = match.group(1)
                COD_PROD = match.group(2)

                if COD_RH and COD_PROD:
                    colav_reg = collection.find_one(
                        {"external_ids.id": {"COD_RH": COD_RH, "COD_PRODUCTO": COD_PROD}})
                    if colav_reg:
                        process_one_update(
                            openadata_reg, colav_reg, db, collection, empty_work, verbose)
                        return

    # elasticsearch section
    if es_handler:
        # Search in elasticsearch
        if thresholds and len(thresholds) == 3:
            thresholds = {"author_thd": thresholds[0],
                          "paper_thd_low": thresholds[1], "paper_thd_high": thresholds[2]}
        else:
            if verbose > 4:
                print("Invalid thresholds values provided, using default values")
            thresholds = {"author_thd": 65,
                          "paper_thd_low": 90, "paper_thd_high": 95}

        authors = []
        title_work = ""
        if 'nme_producto_pd' in openadata_reg.keys():
            if openadata_reg["nme_producto_pd"]:
                title_work = openadata_reg["nme_producto_pd"]

        if 'id_persona_pd' in openadata_reg.keys():
            if openadata_reg["id_persona_pd"]:
                author_db = db["person"].find_one(
                    {"external_ids.id.COD_RH": openadata_reg["id_persona_pd"]}, {"_id": 1, "full_name": 1})
                if author_db:
                    authors.append(author_db["full_name"])

        if authors and title_work != "":
            responses = es_handler.search_work(
                title=title_work,
                source="",
                year="0",
                authors=authors,
                volume="",
                issue="",
                page_start="",
                page_end="",
                use_es_thold=True,
                es_thold=0,
                hits=20
            )
            if responses:
                for response in responses:
                    out = check_work(title_work, authors, response, thresholds)
                    if out:
                        colav_reg = collection.find_one(
                            {"_id": ObjectId(response["_id"])})
                        if colav_reg:
                            process_one_update(
                                openadata_reg, colav_reg, db, collection, empty_work, verbose)
                            return
                        else:
                            if verbose > 4:
                                print("Register with {} not found in mongodb".format(
                                    response["_id"]))
                            return
                # Work not found
                if insert_all:
                    process_one_insert(
                        openadata_reg, db, collection, empty_work, es_handler, verbose)

        elif title_work:
            # No authors
            title = sub('[_|,\\\\]', '', title_work).lower()
            es_results = es_handler.search_work(
                title=title,
                source="",
                year="0",
                authors=[],
                volume="",
                issue="",
                page_start="",
                page_end="",
                use_es_thold=True,
                es_thold=0,
                hits=20
            )
            if es_results:
                for es_work in es_results:
                    colav_reg = collection.find_one(
                        {"_id": ObjectId(es_work["_id"])})
                    if colav_reg:
                        titles = [titles.get('title')
                                  for titles in colav_reg["titles"]]
                        display_name, score = process.extractOne(
                            title_work, titles)
                        if score > thresholds["paper_thd_high"]:
                            process_one_update(
                                openadata_reg, colav_reg, db, collection, empty_work, verbose)
                            return
                    else:
                        if verbose > 4:
                            print("Register with {} not found in mongodb".format(
                                response["_id"]))
                        return

            if insert_all:
                process_one_insert(
                    openadata_reg, db, collection, empty_work, es_handler, verbose)
    else:
        process_one_insert(
            openadata_reg, db, collection, empty_work, es_handler, verbose)
        if verbose > 4:
            print("No elasticsearch index provided")
