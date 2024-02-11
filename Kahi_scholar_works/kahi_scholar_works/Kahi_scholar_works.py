from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from thefuzz import fuzz

from kahi_impactu_utils.Utils import lang_poll, doi_processor


def parse_scholar(reg, empty_work, verbose=0):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "scholar", "time": int(time())}]
    lang = lang_poll(reg["title"])
    entry["titles"].append(
        {"title": reg["title"], "lang": lang, "source": "scholar"})
    if "year" in reg.keys():
        year = ""
        try:
            if reg["year"][-1] == "\n":
                reg["year"] = reg["year"][:-1]
            year = int(reg["year"])
        except Exception as e:
            if verbose > 4:
                print(f"""Could not convert year to int in {reg["doi"]}""")
                print(e)
        entry["year_published"] = year
    if "doi" in reg.keys():
        entry["external_ids"].append(
            {"source": "doi", "id": doi_processor(reg["doi"])})
    if "cid" in reg.keys():
        entry["external_ids"] = [{"source": "scholar", "id": reg["cid"]}]
    if "abstract" in reg.keys():
        entry["abstract"] = reg["abstract"]
    if "volume" in reg.keys():
        if reg["volume"]:
            if reg["volume"][-1] == "\n":
                reg["volume"] = reg["volume"][:-1]
            entry["bibliographic_info"]["volume"] = reg["volume"]
    if "issue" in reg.keys():
        if reg["issue"]:
            if reg["issue"][-1] == "\n":
                reg["issue"] = reg["issue"][:-1]
            entry["bibliographic_info"]["issue"] = reg["issue"]
    if "pages" in reg.keys():
        pages = ""
        if reg["pages"]:
            if reg["pages"][-1] == "\n":
                reg["pages"] = reg["pages"][:-1]
            if "--" in reg["pages"]:
                reg["pages"].replace("\n", "")
                pages = reg["pages"].split("--")
                entry["bibliographic_info"]["start_page"] = pages[0]
                entry["bibliographic_info"]["end_page"] = pages[1]
                if entry["bibliographic_info"]["start_page"].isdigit() and entry["bibliographic_info"]["end_page"].isdigit():
                    try:
                        entry["bibliographic_info"]["pages"] = str(int(
                            entry["bibliographic_info"]["end_page"]) - int(entry["bibliographic_info"]["start_page"]))
                    except Exception as e:
                        if verbose > 4:
                            print(
                                f"""Could not cast pages to substract in {reg["doi"]}""")
                            print(e)
                else:
                    if verbose > 4:
                        print(f"Malformed start_page or end_page in source database for {reg['doi']}. Setting 'pages' to the original value.")
                    entry["bibliographic_info"]["pages"] = reg["pages"]
            else:
                if verbose > 4:
                    print(
                        f"""Malformed pages in source database for {reg["doi"]}. Inserting anyway""")
                entry["bibliographic_info"]["pages"] = reg["pages"]
                entry["bibliographic_info"]["start_page"] = reg["pages"]
    if "bibtex" in reg.keys():
        entry["bibliographic_info"]["bibtex"] = reg["bibtex"]
        typ = reg["bibtex"].split("{")[0].replace("@", "")
        entry["types"].append({"source": "scholar", "type": typ})
    if "cites" in reg.keys():
        entry["citations_count"].append(
            {"source": "scholar", "count": int(reg["cites"])})
    if "cites_link" in reg.keys():
        entry["external_urls"].append(
            {"source": "scholar citations", "url": reg["cites_link"]})
    if "pdf" in reg.keys():
        entry["external_urls"].append({"source": "pdf", "url": reg["pdf"]})

    if "journal" in reg.keys():
        entry["source"] = {"name": reg["journal"], "external_ids": []}

    # authors section
    full_name_list = []
    if "author" in reg.keys():
        for author in reg["author"].strip().split(" and "):
            if "others" in author:
                continue
            author_entry = {}
            names_list = author.split(", ")
            last_names = ""
            first_names = ""
            if len(names_list) > 0:
                last_names = names_list[0].strip()
            if len(names_list) > 1:
                first_names = names_list[1].strip()
            full_name = first_names + " " + last_names
            author_entry["full_name"] = full_name
            author_entry["affiliations"] = []
            author_entry["external_ids"] = []
            entry["authors"].append(author_entry)
            full_name_list.append(full_name)
    if "profiles" in reg.keys():
        if reg["profiles"]:
            for name in reg["profiles"].keys():
                for i, author in enumerate(full_name_list):
                    score = fuzz.ratio(name, author)
                    if score >= 80:
                        entry["authors"][i]["external_ids"] = [
                            {"source": "scholar", "id": reg["profiles"][name]}]
                        break
                    elif score > 70:
                        score = fuzz.partial_ratio(name, author)
                        if score >= 90:
                            entry["authors"][i]["external_ids"] = [
                                {"source": "scholar", "id": reg["profiles"][name]}]
                            break

    return entry


def process_one(scholar_reg, db, collection, empty_work, verbose=0):
    # client = MongoClient(url)
    # db = client[db_name]
    # collection = db["works"]
    doi = None
    # register has doi
    if scholar_reg["doi"]:
        if isinstance(scholar_reg["doi"], str):
            doi = doi_processor(scholar_reg["doi"])
    if doi:
        # is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id": doi})
        if colav_reg:  # update the register
            # updated
            for upd in colav_reg["updated"]:
                if upd["source"] == "scholar":
                    # client.close()
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
        else:  # insert a new register
            # parse
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
            collection.insert_one(entry)
            # insert in elasticsearch
    else:  # does not have a doi identifier
        # elasticsearch section
        pass
    # client.close()


class Kahi_scholar_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("year_published")
        self.collection.create_index("authors.affiliations.id")
        self.collection.create_index("authors.id")
        self.collection.create_index([("titles.title", TEXT)])

        self.scholar_client = MongoClient(
            config["scholar_works"]["database_url"])
        self.scholar_db = self.scholar_client[config["scholar_works"]
                                              ["database_name"]]
        self.scholar_collection = self.scholar_db[config["scholar_works"]
                                                  ["collection_name"]]

        self.n_jobs = config["scholar_works"]["num_jobs"] if "num_jobs" in config["scholar_works"].keys(
        ) else 1
        self.verbose = config["scholar_works"]["verbose"] if "verbose" in config["scholar_works"].keys(
        ) else 0

    def process_scholar(self):
        paper_list = list(self.scholar_collection.find())

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
        self.process_scholar()
        return 0
