from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed


def append_unique(items, item):
    if item not in items:
        items.append(item)


def add_ror_names(inst, entry):
    for item in inst.get("names", []):
        name = item.get("value")
        name_types = item.get("types", [])
        lang = item.get("lang") or ""
        if not name:
            continue
        if "acronym" in name_types:
            append_unique(entry["abbreviations"], name)
        elif "alias" in name_types:
            append_unique(entry["aliases"], name)
        elif "label" in name_types or "ror_display" in name_types:
            append_unique(entry["names"], {"source": "ror", "name": name, "lang": lang})


def add_ror_addresses(inst, entry):
    for location in inst.get("locations", []):
        details = location.get("geonames_details", {})
        entry["addresses"].append({
            "lat": details.get("lat", None),
            "lng": details.get("lng", None),
            "postcode": "",
            "state": details.get("country_subdivision_name", ""),
            "city": details.get("name", ""),
            "country": details.get("country_name", ""),
            "country_code": details.get("country_code", ""),
        })


def add_ror_external_urls(inst, entry):
    for link in inst.get("links", []):
        source = "wikipedia" if link.get("type") == "wikipedia" else "site"
        url = link.get("value", "")
        if url:
            append_unique(entry["external_urls"], {"source": source, "url": url})


def add_ror_external_ids(inst, entry):
    for ext in inst.get("external_ids", []):
        values = ext.get("all", [])
        ext_id = ext.get("preferred") or (values[0] if values else "")
        if ext_id:
            append_unique(entry["external_ids"], {"provenance": "ror", "source": ext.get("type", "").lower(), "id": ext_id})


def process_one(inst, collection, empty_affiliations):
    found_entry = collection.find_one({"external_ids.id": inst["id"]})
    if found_entry:
        return
        # may be updatable, check accordingly
    else:
        entry = empty_affiliations.copy()
        entry["updated"].append({"time": int(time()), "source": "ror"})
        add_ror_names(inst, entry)
        entry["year_established"] = int(
            inst["established"]) if inst["established"] else -1
        entry["status"] = [inst["status"]]

        # types
        for typ in inst["types"]:
            entry["types"].append({"source": "ror", "type": typ})

        # addresses
        add_ror_addresses(inst, entry)

        # external_urls
        add_ror_external_urls(inst, entry)

        # external_ids
        add_ror_external_ids(inst, entry)
        append_unique(entry["external_ids"], {"provenance": "ror", "id": inst["id"], "source": "ror"})
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
