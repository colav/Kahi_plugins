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
        works = works = list(self.works.find({"authors.id":author["_id"]},{"authors":1}))
        for work in works:
            for j, work_author in enumerate(work["authors"]):
                if work_author["id"] == author["_id"]:
                    found = False
                    if not work_author["affiliations"]:
                        #if not affiliation we assume it is right
                        continue
                    for aff in work_author["affiliations"]:
                        found = self.person.count_documents({"$and":[{"_id":author["_id"]},{"affiliations.id":aff["id"]}]})
                        if found:
                            break
                    if not found:
                        work["authors"][j]["id"] = ""
                        self.works.update_one({"_id":work['_id']},{"$set":{"authors":work["authors"]}})
    def run(self):
        #https://github.com/colav/impactu/issues/141
        #only authors from scienti, staff o ranking 
        authors = list(self.person.find({"$or":[{"updated.source":"scienti"},{"updated.source":"staff"},{"updated.source":"ranking"}]},{"_id":1,"affilations":1}))
        for author in authors:
            self.process_one(author)
        return 0
