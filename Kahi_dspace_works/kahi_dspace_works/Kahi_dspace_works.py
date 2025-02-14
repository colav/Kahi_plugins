from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from mohan.Similarity import Similarity
from kahi_dspace_works.utils import get_doi, process_affiliation, filter_work
from kahi_dspace_works.process_one import process_one
from joblib import Parallel, delayed


class Kahi_dspace_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]

        self.collection = self.db["works"]

        self.verbose = config["verbose"] if "verbose" in config else 0

        if (
            "es_index" in config["dspace_works"].keys() and "es_url" in config["dspace_works"].keys(
            ) and "es_user" in config["dspace_works"].keys() and "es_password" in config["dspace_works"].keys()
        ):  # noqa: E501
            es_index = config["dspace_works"]["es_index"]
            es_url = config["dspace_works"]["es_url"]
            if (
                config["dspace_works"]["es_user"] and config["dspace_works"]["es_password"]
            ):
                es_auth = (
                    config["dspace_works"]["es_user"],
                    config["dspace_works"]["es_password"],
                )
            else:
                es_auth = None
            self.es_handler = Similarity(
                es_index, es_uri=es_url, es_auth=es_auth)
            print("INFO: ES handler created successfully")
        else:
            self.es_handler = None
            print("WARNING: No elasticsearch configuration provided")

        self.task = (
            config["dspace_works"]["task"]
            if "task" in config["dspace_works"].keys()
            else None
        )
        self.n_jobs = (
            config["dspace_works"]["num_jobs"]
            if "num_jobs" in config["dspace_works"].keys()
            else 1
        )
        self.verbose = (
            config["dspace_works"]["verbose"]
            if "verbose" in config["dspace_works"].keys()
            else 0
        )

        thresholds = config["dspace_works"]["thresholds"] if "thresholds" in config["dspace_works"].keys(
        ) else None

        if thresholds and len(thresholds) == 3:
            self.thresholds = {"author_thd": thresholds[0],
                               "paper_thd_low": thresholds[1], "paper_thd_high": thresholds[2]}
        else:
            if self.verbose > 4:
                print("Invalid thresholds values provided, using default values")
            self.thresholds = {"author_thd": 65,
                               "paper_thd_low": 90, "paper_thd_high": 95}

    def process_repository(self, affiliation, base_url, dspace_collection):
        if self.task == "doi":
            work_cursor = dspace_collection.find(
                {
                    "$and": [
                        {
                            "OAI-PMH.GetRecord.record.metadata.dim:dim.dim:field.@element": "identifier"
                        },
                        {
                            "OAI-PMH.GetRecord.record.metadata.dim:dim.dim:field.@qualifier": "doi"
                        },
                        {
                            "OAI-PMH.GetRecord.record.metadata.dim:dim.dim:field.@element": "title"
                        },
                    ]
                }
            )

            Parallel(n_jobs=self.n_jobs, verbose=10, backend="threading")(
                delayed(process_one)(
                    dspace_reg=work,
                    affiliation=affiliation,
                    base_url=base_url,
                    db=self.db,
                    collection=self.collection,
                    empty_work=self.empty_work(),
                    es_handler=self.es_handler,
                    similarity=False,
                    thresholds=self.thresholds,
                    verbose=self.verbose,
                )
                for work in work_cursor if get_doi(work) and filter_work(work)
            )

        else:
            work_cursor = dspace_collection.find(
                {
                    "OAI-PMH.GetRecord.record.metadata.dim:dim.dim:field.@element": "title"
                }
            )
            Parallel(n_jobs=self.n_jobs, verbose=10, backend="threading")(
                delayed(process_one)(
                    dspace_reg=work,
                    affiliation=affiliation,
                    base_url=base_url,
                    db=self.db,
                    collection=self.collection,
                    empty_work=self.empty_work(),
                    es_handler=self.es_handler,
                    similarity=True,
                    thresholds=self.thresholds,
                    verbose=self.verbose,
                )
                for work in work_cursor if not get_doi(work) and filter_work(work))

    def run(self):
        print(
            f"INFO: Running dspace works with num_jobs = {self.n_jobs} task = {self.task}")
        dsapce_db_client = MongoClient(
            self.config["dspace_works"]["database_url"])
        dsapce_db = dsapce_db_client[self.config["dspace_works"]
                                     ["database_name"]]
        for repository in self.config["dspace_works"]["repositories"]:
            print(
                f"INFO: Processing repository {repository['institution_id']} collection {repository['collection_name']} with url {repository['repository_url']}")
            affiliation = process_affiliation(
                repository["institution_id"], self.db)
            base_url = repository["repository_url"]
            dspace_collection = dsapce_db[repository["collection_name"]]
            self.process_repository(affiliation, base_url, dspace_collection)

        return -1
