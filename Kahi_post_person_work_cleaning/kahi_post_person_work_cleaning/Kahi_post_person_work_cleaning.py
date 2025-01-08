from kahi.KahiBase import KahiBase
from pymongo import MongoClient


class Kahi_post_person_work_cleaning(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config
        self.config = config
        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.works = self.db["works"]
        self.person = self.db["person"]

    def process_one(self, author):
        works = works = list(self.works.find(
            {"authors.id": author["_id"]}, {"authors": 1, "external_ids": 1}))
        # Get the cod_rh from the author
        cod_rh = next((x["id"]["COD_RH"] for x in author["external_ids"] if x["source"] in ("scienti", "minciencias")),
                      None)
        for work in works:
            # Check if the author has the cod_rh in the work
            cod_rh_work = [
                x["id"]["COD_RH"]
                for x in work["external_ids"]
                if x.get("source") == "scienti" and "COD_RH" in x.get("id", {})
            ]
            if cod_rh in cod_rh_work:
                # The author has the cod_rh in the work
                continue

            # Check if the author has the affiliation in the work
            for j, work_author in enumerate(work["authors"]):
                # Only analyze the author we are looking for in the work
                if work_author["id"] == author["_id"]:
                    found = False
                    if not work_author["affiliations"]:
                        # if not affiliation we assume it is right
                        continue
                    for aff in work_author["affiliations"]:
                        found = self.person.count_documents(
                            {"$and": [{"_id": author["_id"]}, {"affiliations.id": aff["id"]}]})
                        if found:
                            break
                    if not found:
                        work["authors"][j]["id"] = ""
                        self.works.update_one({"_id": work['_id']}, {
                                              "$set": {"authors": work["authors"]}})

    def run(self):
        # https://github.com/colav/impactu/issues/141
        # only authors from scienti, staff or ciarp
        authors = list(self.person.find({"$or": [{"updated.source": "scienti"}, {
                       "updated.source": "staff"}, {"updated.source": "ciarp"}]}, {"_id": 1, "external_ids": 1, "affilations": 1}))
        for author in authors:
            self.process_one(author)
        return 0
