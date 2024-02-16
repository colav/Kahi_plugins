from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from kahi_impactu_utils.Utils import lang_poll, doi_processor, check_date_format


def parse_scienti(reg, empty_work, verbose=0):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "scienti", "time": int(time())}]
    lang = lang_poll(reg["TXT_NME_PROD"], verbose=verbose)
    entry["titles"].append(
        {"title": reg["TXT_NME_PROD"], "lang": lang, "source": "scienti"})
    entry["external_ids"].append({"source": "COD_RH", "id": reg["COD_RH"]})
    entry["external_ids"].append(
        {"source": "COD_PRODUCTO", "id": reg["COD_PRODUCTO"]})
    if "TXT_DOI" in reg.keys():
        entry["external_ids"].append(
            {"source": "doi", "id": doi_processor(reg["TXT_DOI"])})
    if "TXT_WEB_PRODUCTO" in reg.keys():
        entry["external_urls"].append(
            {"source": "scienti", "url": reg["TXT_WEB_PRODUCTO"]})
    if "NRO_ANO_PRESENTA" in reg.keys():
        year = reg["NRO_ANO_PRESENTA"]
    if "NRO_MES_PRESENTA" in reg.keys():
        month = reg["NRO_MES_PRESENTA"]
        if len(str(month)) == 1:
            month = f'0{month}'
    if year and month:
        entry["date_published"] = check_date_format(
            f'{month}-{year}')
        entry["year_published"] = int(year)
    if "SGL_CATEGORIA" in reg.keys():
        entry["ranking"].append(
            {"date": "", "rank": reg["SGL_CATEGORIA"], "source": "scienti"})
    entry["types"].append(
        {"source": "scienti", "type": reg["product_type"][0]["TXT_NME_TIPO_PRODUCTO"]})
    if "product_type" in reg["product_type"][0].keys():
        typ = reg["product_type"][0]["product_type"][0]["TXT_NME_TIPO_PRODUCTO"]
        entry["types"].append({"source": "scienti", "type": typ})

    # details only for articles
    if "details" in reg.keys() and len(reg["details"]) > 0 and "article" in reg["details"][0].keys():
        details = reg["details"][0]["article"][0]
        try:
            if "TXT_PAGINA_INICIAL" in details.keys():
                entry["bibliographic_info"]["start_page"] = details["TXT_PAGINA_INICIAL"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing start page on RH:{reg["COD_RH"]} and COD_PROD:{reg["COD_PRODUCTO"]}')
                print(e)
        try:
            if "TXT_PAGINA_FINAL" in details.keys():
                entry["bibliographic_info"]["end_page"] = details["TXT_PAGINA_FINAL"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing end page on RH:{reg["COD_RH"]} and COD_PROD:{reg["COD_PRODUCTO"]}')
                print(e)
        try:
            if "TXT_VOLUMEN_REVISTA" in details.keys():
                entry["bibliographic_info"]["volume"] = details["TXT_VOLUMEN_REVISTA"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing volume on RH:{reg["COD_RH"]} and COD_PROD:{reg["COD_PRODUCTO"]}')
                print(e)
        try:
            if "TXT_FASCICULO_REVISTA" in details.keys():
                entry["bibliographic_info"]["issue"] = details["TXT_FASCICULO_REVISTA"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing issue on RH:{reg["COD_RH"]} and COD_PROD:{reg["COD_PRODUCTO"]}')
                print(e)

        # source section
        source = {"external_ids": [], "title": ""}
        if "journal" in details.keys():
            journal = details["journal"][0]
            source["title"] = journal["TXT_NME_REVISTA"]
            if "TXT_ISSN_REF_SEP" in journal.keys():
                source["external_ids"].append(
                    {"source": "issn", "id": journal["TXT_ISSN_REF_SEP"]})
            if "COD_REVISTA" in journal.keys():
                source["external_ids"].append(
                    {"source": "scienti", "id": journal["COD_REVISTA"]})
        elif "journal_others" in details.keys():
            journal = details["journal_others"][0]
            source["title"] = journal["TXT_NME_REVISTA"]
            if "TXT_ISSN_REF_SEP" in journal.keys():
                source["external_ids"].append(
                    {"source": "issn", "id": journal["TXT_ISSN_REF_SEP"]})
            if "COD_REVISTA" in journal.keys():
                source["external_ids"].append(
                    {"source": "scienti", "id": journal["COD_REVISTA"]})

        entry["source"] = source

    # authors section
    affiliations = []
    if "group" in reg.keys():
        group = reg["group"][0]
        affiliations.append({
            "external_ids": [{"source": "scienti", "id": group["COD_ID_GRUPO"]}],
            "name": group["NME_GRUPO"]
        })
        if "institution" in group.keys():
            inst = group["institution"][0]
            affiliations.append({
                "external_ids": [{"source": "scienti", "id": inst["COD_INST"]}],
                "name": inst["NME_INST"]
            })
    author = reg["author"][0]
    author_entry = {
        "full_name": author["TXT_TOTAL_NAMES"],
        "types": [],
        "affiliations": affiliations,
        "external_ids": [{"source": "scienti", "id": author["COD_RH"]}]
    }
    if author["TPO_DOCUMENTO_IDENT"] == "P":
        author_entry["external_ids"].append(
            {"source": "Passport", "id": author["NRO_DOCUMENTO_IDENT"]})
    if author["TPO_DOCUMENTO_IDENT"] == "C":
        author_entry["external_ids"].append(
            {"source": "Cédula de Ciudadanía", "id": author["NRO_DOCUMENTO_IDENT"]})
    if author["TPO_DOCUMENTO_IDENT"] == "E":
        author_entry["external_ids"].append(
            {"source": "Cédula de Extranjería", "id": author["NRO_DOCUMENTO_IDENT"]})
    entry["authors"] = [author_entry]
    return entry


def process_one(scienti_reg, client, url, db_name, empty_work, verbose=0, multiprocessing=False):
    if multiprocessing:
        # TODO: fix multiprocessing support if possible
        client = MongoClient(url)
    db = client[db_name]
    collection = db["works"]
    doi = None
    # register has doi
    if "TXT_DOI" in scienti_reg.keys():
        if scienti_reg["TXT_DOI"]:
            doi = doi_processor(scienti_reg["TXT_DOI"])
    if doi:
        # is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id": doi})
        if colav_reg:  # update the register
            entry = parse_scienti(
                scienti_reg, empty_work.copy(), verbose=verbose)
            # updated
            for upd in colav_reg["updated"]:
                if upd["source"] == "scienti":
                    # adding new author and affiliations to the register
                    if "openalex" in [upd["source"] for upd in colav_reg["updated"]]:
                        if multiprocessing:
                            client.close()
                        return None
                    for i, author in enumerate(entry["authors"]):
                        author_db = None
                        for ext in author["external_ids"]:
                            author_db = db["person"].find_one(
                                {"external_ids.id": ext["id"]})
                            if author_db:
                                break
                        if author_db:
                            sources = [ext["source"] for ext in author_db["external_ids"]]
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
                                sources = [ext["source"] for ext in author_db["external_ids"]]
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

                    collection.update_one(
                        {"_id": colav_reg["_id"]},
                        {"$push": {"authors": entry['authors'][0]}, "$inc": {"author_count": 1}}
                    )

                    if multiprocessing:
                        client.close()
                    return None  # Register already on db
                    # Could be updated with new information when scienti database changes
            colav_reg["updated"].append(
                {"source": "scienti", "time": int(time())})
            # titles
            if 'scienti' not in [title['source'] for title in colav_reg["titles"]]:
                lang = lang_poll(entry["titles"][0]["title"])
                colav_reg["titles"].append(
                    {"title": entry["titles"][0]["title"], "lang": lang, "source": "scienti"})
            # external_ids
            ext_ids = [ext["id"] for ext in colav_reg["external_ids"]]
            for ext in entry["external_ids"]:
                if ext["id"] not in ext_ids:
                    colav_reg["external_ids"].append(ext)
                    ext_ids.append(ext["id"])
            # types
            types = [ext["source"] for ext in colav_reg["types"]]
            for typ in entry["types"]:
                if typ["source"] not in types:
                    colav_reg["types"].append(typ)

            # external urls
            url_sources = [url["source"]
                           for url in colav_reg["external_urls"]]
            for ext in entry["external_urls"]:
                if ext["source"] not in url_sources:
                    colav_reg["external_urls"].append(ext)
                    url_sources.append(ext["source"])

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
                }}
            )
        else:  # insert a new register
            print(f'Inserting new work with DOI: {doi}')
            # parse
            entry = parse_scienti(scienti_reg, empty_work.copy())
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
                if "external_ids" in entry["source"].keys():
                    if len(entry["source"]["external_ids"]) == 0:
                        if verbose > 4:
                            if "title" in entry["source"].keys():
                                print(
                                    f'Register with RH: {scienti_reg["COD_RH"]} and COD_PROD: {scienti_reg["COD_PRODUCTO"]} could not be linked to a source with name: {entry["source"]["title"]}')
                            else:
                                print(
                                    f'Register with RH: {scienti_reg["COD_RH"]} and COD_PROD: {scienti_reg["COD_PRODUCTO"]} does not provide a source')
                    else:
                        if verbose > 4:
                            print(
                                f'Register with RH: {scienti_reg["COD_RH"]} and COD_PROD: {scienti_reg["COD_PRODUCTO"]} could not be linked to a source with {entry["source"]["external_ids"][0]["source"]}: {entry["source"]["external_ids"][0]["id"]}')  # noqa: E501
                else:
                    if "title" in entry["source"].keys():
                        if entry["source"]["title"] == "":
                            if verbose > 4:
                                print(
                                    f'Register with RH: {scienti_reg["COD_RH"]} and COD_PROD: {scienti_reg["COD_PRODUCTO"]} does not provide a source')
                        else:
                            if verbose > 4:
                                print(
                                    f'Register with RH: {scienti_reg["COD_RH"]} and COD_PROD: {scienti_reg["COD_PRODUCTO"]} could not be linked to a source with name: {entry["source"]["title"]}')
                    else:
                        if verbose > 4:
                            print(
                                f'Register with RH: {scienti_reg["COD_RH"]} and COD_PROD: {scienti_reg["COD_PRODUCTO"]} could not be linked to a source (no ids and no name)')

                entry["source"] = {
                    "id": "",
                    "name": entry["source"]["title"] if "title" in entry["source"].keys() else ""
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
            collection.insert_one(entry)
            # insert in elasticsearch
    else:  # does not have a doi identifier
        # elasticsearch section
        pass
    if multiprocessing:
        client.close()


class Kahi_scienti_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        self.collection.create_index("year_published")
        self.collection.create_index("authors.affiliations.id")
        self.collection.create_index("authors.id")
        self.collection.create_index([("titles.title", TEXT)])
        self.collection.create_index("external_ids.id")

        self.n_jobs = config["scienti_works"]["num_jobs"] if "num_jobs" in config["scienti_works"].keys(
        ) else 1
        self.verbose = config["scienti_works"]["verbose"] if "verbose" in config["scienti_works"].keys(
        ) else 0

        self.pipeline = [
            {"$match": {"TXT_DOI": {"$ne": None}}},
            {"$project": {"doi": {"$trim": {"input": "$TXT_DOI"}}}},
            {"$project": {"doi": {"$toLower": "$doi"}}},
            {"$group": {"_id": "$doi", "ids": {"$push": "$_id"}}}
        ]

    def process_group(self, group, client, mongodb_url, db_name, collection, empty_work, verbose=0):
        for i in group["ids"]:
            reg = collection.find_one({"_id": i})
            process_one(reg, client, mongodb_url, db_name, empty_work, verbose=verbose)

    def process_scienti(self, config):
        client = MongoClient(config["database_url"])
        db = client[config["database_name"]]
        scienti = db[config["collection_name"]]
        paper_groups = list(scienti.aggregate(self.pipeline))
        if self.verbose > 0:
            print("Processing {} groups of papers".format(len(paper_groups)))
        Parallel(
            n_jobs=self.n_jobs,
            verbose=self.verbose,
            backend="threading")(
            delayed(self.process_group)(
                group,
                self.client,
                self.mongodb_url,
                self.config["database_name"],
                scienti,
                self.empty_work(),
                verbose=self.verbose
            ) for group in paper_groups
        )

    def run(self):
        for config in self.config["scienti_works"]["databases"]:
            if self.verbose > 0:
                print("Processing {} database".format(config["database_name"]))
            if self.verbose > 4:
                print("Updating already inserted entries")
            print(config)
            print(type(config))
            self.process_scienti(config)
        self.client.close()
        return 0
