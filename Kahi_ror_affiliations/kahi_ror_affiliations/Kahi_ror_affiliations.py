from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed


def process_one(inst, collection, empty_affiliations):
    found_entry = collection.find_one({"external_ids.id": inst["id"]})
    if found_entry:
        return
        # may be updatable, check accordingly
    else:
        entry = empty_affiliations.copy()
        entry["updated"].append({"time": int(time()), "source": "ror"})
        entry["names"].append(
            {"source": "ror", "name": inst["name"], "lang": "en"})
        entry["aliases"].extend(inst["aliases"])
        entry["abbreviations"].extend(inst["acronyms"])
        entry["year_established"] = int(
            inst["established"]) if inst["established"] else -1
        entry["status"] = [inst["status"]]

        # types
        for typ in inst["types"]:
            entry["types"].append({"source": "ror", "type": typ})

        # addresses
        for add in inst["addresses"]:
            add_entry = {
                "lat": add["lat"],
                "lng": add["lng"],
                "postcode": add["postcode"] if add["postcode"] else "",
                "state": add["state"],
                "city": add["city"],
                "country": "",
                "country_code": "",
            }
            entry["addresses"].append(add_entry)
        entry["addresses"][0]["country"] = inst["country"]["country_name"]
        entry["addresses"][0]["country_code"] = inst["country"]["country_code"]

        # external_urls
        if inst["links"]:
            for link in inst["links"]:
                url_entry = {"source": "site", "url": inst["links"][0]}
                if url_entry not in entry["external_urls"]:
                    entry["external_urls"].append(url_entry)
        if inst["wikipedia_url"]:
            entry["external_urls"].append(
                {"source": "wikipedia", "url": inst["wikipedia_url"]})

        # external_ids
        if inst["external_ids"]:
            for key, ext in inst["external_ids"].items():
                if isinstance(ext["all"], list):
                    alll = ext["all"][0] if len(
                        ext["all"]) > 0 else ext["all"]
                    ext_entry = {"source": key.lower(), "id": alll}
                    if ext_entry not in entry["external_ids"]:
                        entry["external_ids"].append(ext_entry)
        entry["external_ids"].append(
            {"source": "ror", "id": inst["id"]})
        entry["_id"] = inst["id"].split("/")[-1]
        collection.insert_one(entry)


class Kahi_ror_affiliations(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config
        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.ror_client = MongoClient(
            config["ror_affiliations"]["database_url"])
        if config["ror_affiliations"]["database_name"] not in self.ror_client.list_database_names():
            raise Exception("Database {} not found in {}".format(
                config["ror_affiliations"]['database_name'], config["ror_affiliations"]["database_url"]))
        self.ror_db = self.ror_client[config["ror_affiliations"]
                                      ["database_name"]]
        if config["ror_affiliations"]["collection_name"] not in self.ror_db.list_collection_names():
            raise Exception("Collection {}.{} not found in {}".format(config["ror_affiliations"]['database_name'],
                                                                      config["ror_affiliations"]['collection_name'], config["ror_affiliations"]["database_url"]))

        self.ror_collection = self.ror_db[config["ror_affiliations"]
                                          ["collection_name"]]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("types.type")
        self.collection.create_index([("names.name", TEXT)])

        self.n_jobs = config["ror_affiliations"]["num_jobs"]
        self.client.close()
        self.config = config

    def process_ror(self):
        inst_cursor = self.ror_collection.find(no_cursor_timeout=True)
        print(
            f"Processing {self.config['ror_affiliations']['database_name']}.{self.config['ror_affiliations']['collection_name']}...")
        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            collection = db["affiliations"]

            Parallel(
                n_jobs=self.n_jobs,
                verbose=5,
                backend="threading")(
                delayed(process_one)(
                    inst,
                    collection,
                    self.empty_affiliation()
                ) for inst in inst_cursor
            )
            client.close()

    def run(self):
        self.process_ror()
        return 0
