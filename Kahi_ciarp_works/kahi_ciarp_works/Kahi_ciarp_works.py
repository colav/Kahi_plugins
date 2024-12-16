from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from pandas import read_excel, isna
from kahi_ciarp_works.process_one import process_one
from kahi_impactu_utils.Utils import doi_processor
from kahi_impactu_utils.Mapping import ciarp_mapping
from mohan.Similarity import Similarity


class Kahi_ciarp_works(KahiBase):

    config = {}

    def __init__(self, config):
        """
        Constructor for the Kahi_ciarp_works class.

        Several indices are created in the MongoDB collection to speed up the queries.

        Parameters
        ----------
        config : dict
            The configuration dictionary. It should contain the following keys:
            - ciarp_works(/doi or empty): a dictionary with the following keys:
                - task: the task to be performed. It can be "doi" or "all"
                - num_jobs: the number of jobs to be used in parallel processing
                - verbose: the verbosity level
                - databases: a list of dictionaries with the following keys:
                    - database_url: the URL for the MongoDB database
                    - database_name: the name of the database
                    - collection_name: the name of the collection
                    - es_index: the name of the Elasticsearch index
                    - es_url: the URL for the Elasticsearch server
                    - es_user: the username for the Elasticsearch server
                    - es_password: the password for the Elasticsearch server
        """
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

        if all(key in config["ciarp_works"] for key in ["es_index", "es_url", "es_user", "es_password"]):
            es_index = config["ciarp_works"]["es_index"]
            es_url = config["ciarp_works"]["es_url"]
            if config["ciarp_works"]["es_user"] and config["ciarp_works"]["es_password"]:
                es_auth = (config["ciarp_works"]["es_user"],
                           config["ciarp_works"]["es_password"])
            else:
                es_auth = None
            self.es_handler = Similarity(
                es_index, es_uri=es_url, es_auth=es_auth)
            print("INFO: ES handler created successfully")
        else:
            self.es_handler = None
            print("WARNING: No elasticsearch configuration provided")

        self.ciarp_databases = config["ciarp_works"]["databases"]

        self.required_columns = [
            "código_unidad_académica", "código_subunidad_académica", "tipo_documento", "identificación",
            "año", "título", "idioma", "revista", "editorial", "doi", "issn", "isbn", "volumen", "issue",
            "primera_página", "pais_producto", "última_página", "entidad_premiadora", "ranking"
        ]

        self.task = config["ciarp_works"]["task"]
        self.n_jobs = config["ciarp_works"].get("num_jobs", 1)
        self.verbose = config["ciarp_works"].get("verbose", 0)

    def process_ciarp(self):
        """
        Method to process the CIARP database.
        Checks if the task is "doi" or not and processes the documents accordingly.

        Parameters:
        -----------
        db : Database
            The MongoDB database to be used. (colav database genrated by the kahi)
        collection : Collection
            The MongoDB collection to be used. (works collection genrated by the kahi)
        config : dict
            A dictionary with the configuration for the scienti database. It should have the following keys:
            - database_url: the URL for the MongoDB database
            - database_name: the name of the database
            - collection_name: the name of the collection
            - es_index: the name of the Elasticsearch index
            - es_url: the URL for the Elasticsearch server
            - es_user: the username for the Elasticsearch server
            - es_password: the password for the Elasticsearch server
        """
        for database in self.ciarp_databases:
            self.aff_reg = self.db["affiliations"].find_one(
                {"external_ids.id": database["institution_id"]})
            if not self.aff_reg:
                print(
                    f"WARNING: Affiliation {database['institution_id']} not found")
                continue

            # Load Excel file into DataFrame
            dtype_mapping = {col: str for col in self.required_columns}
            self.ciarp = read_excel(
                database["file_path"],
                dtype=dtype_mapping
            ).fillna("")

            # Validate required columns
            for col in self.required_columns:
                if col not in self.ciarp.columns:
                    print(
                        f"Column {col} not found in file {database['file_path']}, and it is required.")
                    return

            # Get allowed categories for the current entity
            allowed_categories = ciarp_mapping(database['institution_id'], "works")

            # Filter DataFrame by `ranking` field
            self.filtered_ciarp = self.ciarp[self.ciarp["ranking"].isin(allowed_categories)].copy()
            if self.verbose > 0:
                print("Filtering by {} categories of works".format(len(allowed_categories)))

            # Add index for unique identification
            self.filtered_ciarp["index"] = [
                f"{i}-{rec}-{int(time())}" for i, rec in enumerate(self.filtered_ciarp["identificación"])
            ]
            index = []
            for i, rec in enumerate(self.filtered_ciarp["identificación"]):
                # row index - cedula - timestamp
                index.append(f"{str(i)}-{rec}-{int(time())}")
            self.filtered_ciarp['index'] = index
            self.filtered_ciarp = self.filtered_ciarp.to_dict(orient="records")

            # selects papers with doi according to task variable
            if self.task == "doi":
                papers = []
                for par in self.filtered_ciarp:
                    if not isna(par["doi"]):
                        if doi_processor(par["doi"]):
                            papers.append(par)
                self.filtered_ciarp = papers
            else:
                # TODO: implement similarity task and a default task that runs all
                papers = []
                for par in self.filtered_ciarp:
                    if isna(par["doi"]):
                        papers.append(par)
                    elif not doi_processor(par["doi"]):
                        papers.append(par)
                self.filtered_ciarp = papers

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
                        self.aff_reg,
                        self.empty_work(),
                        True if self.task != "doi" else False,
                        self.es_handler,
                        verbose=self.verbose
                    ) for paper in self.filtered_ciarp
                )

    def run(self):
        """
        Method to run the process_ciarp method.
        Entrypoint for the Kahi_ciarp_works class to be excute by kahi.
        """
        self.process_ciarp()
        return 0
