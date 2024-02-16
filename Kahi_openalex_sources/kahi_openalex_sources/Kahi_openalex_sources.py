from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from time import time
from joblib import Parallel, delayed


def process_one(source, client, db_name, empty_source):
    db = client[db_name]
    collection = db["sources"]

    source_db = None
    if "issn" in source.keys():
        if source["issn"]:
            if isinstance(source["issn"], list) and len(source["issn"]) > 1:
                source_db = collection.find_one(
                    {"external_ids.id": source["issn"][0]})
                if not source_db:
                    source_db = collection.find_one(
                        {"external_ids.id": source["issn"][1]})
            else:
                source_db = collection.find_one(
                    {"external_ids.id": source["issn"]})
    if not source_db:
        if "issn_l" in source.keys():
            if source["issn_l"]:
                source_db = collection.find_one(
                    {"external_ids.id": source["issn_l"]})
    if source_db:
        oa_found = False
        for ext in source_db["external_ids"]:
            if ext["id"] == source["id"]:
                oa_found = True
                break
        if oa_found:
            return

        for upd in source_db["updated"]:
            if upd["source"] == "openalex":
                upd["time"] = int(time())
                oa_found = True
        if not oa_found:
            source_db["updated"].append(
                {"source": "openalex", "time": int(time())})

        ext_found = False
        for ext in source_db["external_ids"]:
            if ext["id"] == source["id"]:
                ext_found = True
                break
        if not ext_found:
            source_db["external_ids"].append(
                {"source": "openalex", "id": source["id"]})

        if "type" in source.keys():
            if source["type"]:
                type_found = False
                for typ in source_db["types"]:
                    if typ["source"] == "openalex":
                        type_found = True
                        break
                if not type_found:
                    source_db["types"].append(
                        {"source": "openalex", "type": source["type"]})
        name_found = False
        for name in source_db["names"]:
            if name["name"] == source["display_name"]:
                name_found = True
                break
        if not name_found:
            source_db["names"].append(
                {"name": source["display_name"], "lang": "en", "source": "openalex"})

        collection.update_one({"_id": source_db["_id"]}, {"$set": {
            "updated": source_db["updated"],
            "names": source_db["names"],
            "external_ids": source_db["external_ids"],
            "types": source_db["types"],
            "subjects": source_db["subjects"]
        }})
    else:
        entry = empty_source.copy()
        entry["updated"] = [
            {"source": "openalex", "time": int(time())}]
        entry["names"].append(
            {"name": source["display_name"], "lang": "en", "source": "openalex"})
        entry["external_ids"].append(
            {"source": "openalex", "id": source["id"]})
        if "issn" in source.keys():
            if source["issn"]:
                entry["external_ids"].append(
                    {"source": "issn", "id": source["issn"]})
        if "issn_l" in source.keys():
            if source["issn_l"]:
                entry["external_ids"].append(
                    {"source": "issn_l", "id": source["issn_l"]})
        if "type" in source.keys():
            if source["type"]:
                entry["types"].append(
                    {"source": "openalex", "type": source["type"]})
        if "publisher" in source.keys():
            if source["publisher"]:
                entry["publisher"] = {
                    "name": source["publisher"], "country_code": source["country_code"] if "country_code" in source.keys() else ""}
        if "apc_usd" in source.keys():
            if source["apc_usd"]:
                entry["apc"] = {"currency": "USD",
                                "charges": source["apc_usd"]}
        if "abbreviated_title" in source.keys():
            if source["abbreviated_title"]:
                entry["abbreviations"].append(
                    source["abbreviated_title"])
        for name in source["alternate_titles"]:
            entry["abbreviations"].append(name)
        if source["homepage_url"]:
            entry["external_urls"].append(
                {"source": "site", "url": source["homepage_url"]})
        if source["societies"]:
            for soc in source["societies"]:
                entry["external_urls"].append(
                    {"source": soc["organization"], "url": soc["url"]})

        collection.insert_one(entry)


class Kahi_openalex_sources(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["sources"]

        self.collection.create_index("external_ids.id")

        self.openalex_client = MongoClient(
            config["openalex_sources"]["database_url"])

        if config["openalex_sources"]["database_name"] not in self.openalex_client.list_database_names():
            raise RuntimeError(
                f'''Database {config["openalex_sources"]["database_name"]} was not found''')

        self.openalex_db = self.openalex_client[config["openalex_sources"]
                                                ["database_name"]]

        if config["openalex_sources"]["collection_name"] not in self.openalex_db.list_collection_names():
            raise RuntimeError(
                f'''Collection {config["openalex_sources"]["collection_name"]} was not found on database {config["openalex_sources"]["database_name"]}''')

        self.openalex_collection = self.openalex_db[config["openalex_sources"]
                                                    ["collection_name"]]

        self.n_jobs = config["openalex_sources"]["num_jobs"]
        self.client.close()

    def process_openalex(self):
        source_list = list(self.openalex_collection.find())
        client = MongoClient(self.mongodb_url)
        Parallel(
            n_jobs=self.n_jobs,
            verbose=10,
            backend="threading")(
            delayed(process_one)(
                source,
                client,
                self.config["database_name"],
                self.empty_source()
            ) for source in source_list
        )
        client.close()

    def run(self):
        self.process_openalex()
        return 0
