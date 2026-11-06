from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from math import isnan
from re import split, UNICODE
from kahi_impactu_utils.Utils import doi_processor, lang_poll, normalize_publication_year


def parse_scopus(reg, empty_work, verbose=0):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "scopus", "time": int(time())}]
    lang = lang_poll(reg["Title"])
    entry["titles"].append(
        {"title": reg["Title"], "lang": lang, "source": "scopus"})
    if "Abstract" in reg.keys():
        if reg["Abstract"] and reg["Abstract"] == reg["Abstract"]:
            entry["abstract"] = reg["Abstract"]
    entry["year_published"] = normalize_publication_year(reg.get("Year"))

    if "Document Type" in reg.keys():
        entry["types"].append(
            {"source": "scopus", "type": reg["Document Type"]})
    if "Index Keywords" in reg.keys():
        if reg["Index Keywords"] and reg["Index Keywords"] == reg["Index Keywords"]:
            entry["keywords"].extend(reg["Index Keywords"].lower().split("; "))
    if "Author Keywords" in reg.keys():
        if reg["Author Keywords"] and reg["Author Keywords"] == reg["Author Keywords"]:
            entry["keywords"].extend(
                reg["Author Keywords"].lower().split("; "))

    if "DOI" in reg.keys():
        entry["external_ids"].append(
            {"source": "doi", "id": doi_processor(reg["DOI"])})
    if "EID" in reg.keys():
        entry["external_ids"].append({"source": "scopus", "id": reg["EID"]})
    if "Pubmed ID" in reg.keys():
        entry["external_ids"].append(
            {"source": "pubmed", "id": reg["Pubmed ID"]})
    if reg["ISBN"] and reg["ISBN"] == reg["ISBN"] and isinstance(reg["ISBN"], str):
        entry["external_ids"].append(
            {"source": "isbn", "id": reg["ISBN"]})

    if "Link" in reg.keys():
        entry["external_urls"].append({"source": "scopus", "url": reg["Link"]})

    if "Volume" in reg.keys():
        if reg["Volume"] and reg["Volume"] == reg["Volume"]:
            entry["bibliographic_info"]["volume"] = reg["Volume"]
    if "Issue" in reg.keys():
        if reg["Issue"] and reg["Issue"] == reg["Issue"]:
            entry["bibliographic_info"]["issue"] = reg["Issue"]
    if "Page start" in reg.keys():
        # checking for NaN in the second criteria
        if reg["Page start"] and reg["Page start"] == reg["Page start"]:
            entry["bibliographic_info"]["start_page"] = reg["Page start"]
    if "Page end" in reg.keys():
        if reg["Page end"] and reg["Page end"] == reg["Page end"]:
            entry["bibliographic_info"]["end_page"] = reg["Page end"]

    if "Cited by" in reg.keys():
        if isinstance(reg["Cited by"], str):
            if verbose > 4:
                print("Error parsing citations count in doi ", reg["DOI"])
                print("Cited by is a string instead of a integer")
        elif not isnan(reg["Cited by"]):
            try:
                entry["citations_count"].append(
                    {"source": "scopus", "count": int(reg["Cited by"])})
            except Exception as e:
                if verbose > 4:
                    print("Error parsing citations count in doi ", reg["DOI"])
                    print(e)

    source = {"external_ids": []}
    if "Source title" in reg.keys():
        if reg["Source title"] and reg["Source title"] == reg["Source title"]:
            source["name"] = reg["Source title"]
    if "ISSN" in reg.keys():
        if reg["ISSN"] and reg["ISSN"] == reg["ISSN"] and isinstance(reg["ISSN"], str):
            source["external_ids"].append(
                {"source": "issn", "id": reg["ISSN"][:4] + "-" + reg["ISSN"][4:]})
        if reg["CODEN"] and reg["CODEN"] == reg["CODEN"] and isinstance(reg["CODEN"], str):
            source["external_ids"].append(
                {"source": "coden", "id": reg["CODEN"]})
    entry["source"] = source

    # authors section
    ids = None

    if "Authors with affiliations" in reg.keys():
        if reg["Authors with affiliations"] and reg["Authors with affiliations"] == reg["Authors with affiliations"]:
            if "Author(s) ID" in reg.keys():
                ids = reg["Author(s) ID"].split(";")
            auwaf_list = reg["Authors with affiliations"].split("; ")
            for i in range(len(auwaf_list)):
                auaf = split('(^[\w\-\s\.]+,\s+[\w\s\.\-]+,\s)',  # noqa: W605
                             auwaf_list[i], UNICODE)
                if len(auaf) == 1:
                    author = auaf[0]
                    aff_name = ""
                else:
                    author = auaf[1]
                    affiliations = auaf[-1]
                    aff_name = affiliations.split(
                        ",")[-3].strip() if len(affiliations.split(",")) > 2 else affiliations.strip()
                author_entry = {
                    "full_name": author.replace("-", " ").strip(),
                    "types": [],
                    "affiliations": [{"name": aff_name}],
                    "external_ids": []
                }
                if ids:
                    try:
                        if i < len(ids):
                            author_entry["external_ids"] = [
                                {"source": "scopus", "id": ids[i]}] if ids[i] else []
                        else:
                            print("Not all authors have ids in doi ",
                                  reg["DOI"])
                    except Exception as e:
                        if verbose > 4:
                            print("Error parsing author ids in doi ",
                                  reg["DOI"])
                            print(e)
                entry["authors"].append(author_entry)

    return entry


def process_one(scopus_reg, db, collection, empty_work, verbose=0):
    doi = None
    # register has doi
    if scopus_reg["DOI"]:
        if isinstance(scopus_reg["DOI"], str):
            doi = doi_processor(scopus_reg["DOI"])
    if doi:
        # is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id": doi})
        if colav_reg:  # update the register
            # updated
            for upd in colav_reg["updated"]:
                if upd["source"] == "scopus":
                    # client.close()
                    return None  # Register already on db
                    # Could be updated with new information when scopus database changes
            entry = parse_scopus(
                scopus_reg, empty_work.copy(), verbose=verbose)
            colav_reg["updated"].append(
                {"source": "scopus", "time": int(time())})
            # titles
            colav_reg["titles"].extend(entry["titles"])
            # external_ids
            ext_ids = [ext["id"] for ext in colav_reg["external_ids"]]
            for ext in entry["external_ids"]:
                if ext["id"] not in ext_ids:
                    colav_reg["external_ids"].append(ext)
                    ext_ids.append(ext["id"])
            # types
            colav_reg["types"].append(
                {"source": "scopus", "type": entry["types"][0]["type"]})
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
                    "year_published": (normalize_publication_year(colav_reg.get("year_published"))
                                       or normalize_publication_year(entry.get("year_published"))),
                    "updated": colav_reg["updated"],
                    "titles": colav_reg["titles"],
                    "external_ids": colav_reg["external_ids"],
                    "types": colav_reg["types"],
                    "bibliographic_info": colav_reg["bibliographic_info"],
                    "external_urls": colav_reg["external_urls"],
                    "citations_count": colav_reg["citations_count"],
                    "citations_by_year": colav_reg["citations_by_year"]
                }}
            )
        else:  # insert a new register
            # parse
            entry = parse_scopus(
                scopus_reg, empty_work.copy(), verbose=verbose)
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
                            f'Register with doi: {scopus_reg["DOI"]} does not provide a source')
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
            collection.insert_one(entry)
            # insert in elasticsearch
    else:  # does not have a doi identifier
        # elasticsearch section
        pass
    # client.close()


class Kahi_scopus_works(KahiBase):

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

        self.scopus_client = MongoClient(
            config["scopus_works"]["database_url"])
        if config["scopus_works"]["database_name"] not in self.scopus_client.list_database_names():
            raise Exception(
                f"Database {config['scopus_works']['database_name']} not found")
        self.scopus_db = self.scopus_client[config["scopus_works"]
                                            ["database_name"]]
        if config["scopus_works"]["collection_name"] not in self.scopus_db.list_collection_names():
            raise Exception(
                f"Collection {config['scopus_works']['database_name']}.{config['scopus_works']['collection_name']} not found")
        self.scopus_collection = self.scopus_db[config["scopus_works"]
                                                ["collection_name"]]

        self.n_jobs = config["scopus_works"]["num_jobs"] if "num_jobs" in config["scopus_works"].keys(
        ) else 1
        self.verbose = config["scopus_works"]["verbose"] if "verbose" in config["scopus_works"].keys(
        ) else 0

        self.client.close()

    def process_scopus(self):
        paper_list = list(self.scopus_collection.find())
        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            collection = db["works"]

            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(process_one)(
                    paper,
                    db,
                    collection,
                    self.empty_work(),
                    verbose=self.verbose
                ) for paper in paper_list
            )

    def run(self):
        self.process_scopus()
        return 0
