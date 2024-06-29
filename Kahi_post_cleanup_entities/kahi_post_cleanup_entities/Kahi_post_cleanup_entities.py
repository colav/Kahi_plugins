from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from joblib import Parallel, delayed


class Kahi_post_cleanup_entities(KahiBase):

    config = {}

    def __init__(self, config):
        """
        Constructor for the class, database connection is established here
        and collections are initialized.

        Parameters:
        ----------
        config : dict
            config:
            database_url: localhost:27017
            database_name: kahi
            log_database: kahi
            log_collection: log
            workflow:
            ##Works plugins here
            post_cleanup_entities: # run this after all works plugins are done
                num_jobs: 20
                verbose: 4
        """
        self.config = config
        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.works = self.db["works"]
        self.person = self.db["person"]
        self.affiliations = self.db["affiliations"]

        self.n_jobs = config["post_cleanup_entities"]["num_jobs"] if "num_jobs" in config["post_cleanup_entities"].keys(
        ) else 1
        self.verbose = config["post_cleanup_entities"]["verbose"] if "verbose" in config["post_cleanup_entities"].keys(
        ) else 0

    def cleanup_author(self, author):
        """
        remove author if no works are associated with the author

        Parameters:
        ----------
        author : dict
            author object from the person collection with _id field
        """
        works = self.works.count_documents({"authors.id": author["_id"]})
        if works == 0:
            self.person.delete_one({"_id": author["_id"]})
            return 1
        return 0

    def cleanup_affiliation(self, affiliation):
        """
        remove affiliation if no authors are associated with the affiliation

        Parameters:
        ----------
        affiliation : dict
            affiliation object from the affiliations collection with _id field
        """
        count = self.person.count_documents(
            {"affiliations.id": affiliation["_id"]})
        if count == 0:
            self.affiliations.delete_one({"_id": affiliation["_id"]})
            return 1
        return 0

    def run(self):
        """
        Run the post cleanup process for authors and affiliations
        """
        authors = self.person.find({}, {"_id": 1})
        out = Parallel(
            n_jobs=self.n_jobs,
            verbose=self.verbose,
            backend="threading")(
            delayed(self.cleanup_author)(
                author
            ) for author in authors
        )
        print("INFO: Removed {} authors".format(sum(out)))

        affiliations = self.affiliations.find({}, {"_id": 1})
        out = Parallel(
            n_jobs=self.n_jobs,
            verbose=self.verbose,
            backend="threading")(
            delayed(self.cleanup_affiliation)(
                affiliation
            ) for affiliation in affiliations
        )
        print("INFO: Removed {} affiliations".format(sum(out)))
        return 0
