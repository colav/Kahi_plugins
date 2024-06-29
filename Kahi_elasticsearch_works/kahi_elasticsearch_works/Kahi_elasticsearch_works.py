from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from math import isnan

from mohan.Similarity import Similarity


class Kahi_elasticsearch_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        if "es_index" not in config["elasticsearch_works"].keys():
            raise Exception(
                "[Kahi_elasticsearch_works] ERROR: Please specify an es_index")

        self.index = config["elasticsearch_works"]["es_index"]

        self.es_url = config["elasticsearch_works"]["es_url"] if "es_url" in config["elasticsearch_works"].keys(
        ) else "http://localhost:9200"

        self.es_client = Similarity(
            es_index=self.index,
            es_uri=self.es_url,
            es_auth=(
                config["elasticsearch_works"]["es_user"],
                config["elasticsearch_works"]["es_password"]
            ),
        )

        self.task = config["elasticsearch_works"]["task"] if "task" in config["elasticsearch_works"].keys(
        ) else None

        self.verbose = config["elasticsearch_works"]["verbose"] if "verbose" in config["elasticsearch_works"].keys(
        ) else 0
        self.bulk_size = config["elasticsearch_works"]["bulk_size"] if "bulk_size" in config["elasticsearch_works"].keys(
        ) else 100

        self.inserted_ids = []

    def bulk_insert(self):
        es_entries = []
        count = 0
        paper_list = self.collection.find(
            {}, {"titles": 1, "source": 1, "year_published": 1, "bibliographic_info": 1, "authors.full_name": 1})
        for reg in paper_list:
            work = {
                "title": "",
                "source": "",
                "year": "",
                "volume": "",
                "issue": "",
                "start_page": "",
                "end_page": "",
                "authors": [],
                "provenance": "elasticsearch",

            }
            if "titles" not in reg.keys():
                continue
            if len(reg["titles"]) < 1:
                continue
            work["title"] = reg["titles"][0]["title"]
            if "name" in reg["source"].keys():
                work["source"] = reg["source"]["name"] if reg["source"]["name"] else ""
            if "year_published" in reg.keys():
                work["year"] = reg["year_published"] if reg["year_published"] else ""
            if "volume" in reg["bibliographic_info"].keys():
                work["volume"] = reg["bibliographic_info"]["volume"] if reg["bibliographic_info"]["volume"] else ""
            if "issue" in reg["bibliographic_info"].keys():
                work["issue"] = reg["bibliographic_info"]["issue"] if reg["bibliographic_info"]["issue"] else ""
            if "start_page" in reg["bibliographic_info"].keys():
                work["start_page"] = reg["bibliographic_info"]["start_page"] if reg["bibliographic_info"]["start_page"] else ""
            if "end_page" in reg["bibliographic_info"].keys():
                work["end_page"] = reg["bibliographic_info"]["end_page"] if reg["bibliographic_info"]["end_page"] else ""
            authors = []
            for author in reg["authors"]:
                authors.append(author["full_name"])
                if len(authors) == 5:
                    break
            work["authors"] = authors
            # double checking for nan
            for key, val in work.items():
                if isinstance(val, float) and isnan(val):
                    work[key] = ""
            entry = {
                "_index": self.index,
                "_id": str(reg["_id"]),
                "_source": work
            }
            es_entries.append(entry)
            if len(es_entries) == self.bulk_size:
                try:
                    self.es_client.insert_bulk(es_entries)
                except Exception as e:
                    print(e)
                    print(es_entries)
                    raise
                count += self.bulk_size
                es_entries = []
                if self.verbose > 4:
                    print(f"""{count} entries inserted""")

    def delete(self):
        self.es_client.delete_index(self.index)

    def run(self):
        if self.task == "bulk_insert":
            if self.verbose > 0:
                print(f"""Bulk inserting index {self.index}""")
            self.bulk_insert()
        elif self.task == "delete":
            if self.verbose > 0:
                print(f"""Deleting index {self.index}""")
            self.delete()
        else:
            raise Exception("Please specify a task to execute")
        return 0
