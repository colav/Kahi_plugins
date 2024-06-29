from kahi_impactu_utils.Utils import compare_author
from pymongo import MongoClient, TEXT
from joblib import Parallel, delayed
from kahi.KahiBase import KahiBase
from bson import ObjectId
from time import time
import copy


class Kahi_authors_unicity(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config
        self.merge_suffix = "_merged"
        self.mongodb_url = config["database_url"]
        self.client = MongoClient(config["database_url"])
        self.db = self.client[config["database_name"]]

        if config["authors_unicity"]["collection_name"] not in self.db.list_collection_names():
            raise Exception("Collection {} not found in {}".format(
                config["authors_unicity"]['collection_name'], config["authors_unicity"]["database_url"]))
        self.collection = self.db[config["authors_unicity"]["collection_name"]]
        self.collection_merged = self.db[config["authors_unicity"]
                                         ["collection_name"] + self.merge_suffix]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        self.authors_threshold = config["authors_unicity"]["max_authors_threshold"] if "max_authors_threshold" in config["authors_unicity"].keys(
        ) else 0

        self.task = config["authors_unicity"]["task"] if "task" in config["authors_unicity"].keys(
        ) else None

        self.n_jobs = config["authors_unicity"]["num_jobs"] if "num_jobs" in config["authors_unicity"].keys(
        ) else 1

        self.verbose = config["authors_unicity"][
            "verbose"] if "verbose" in config["authors_unicity"].keys() else 0

    # Function to merge affiliations

    def merge_affiliations(self, target_doc, doc):
        """
        Merges affiliations from one document into another.

        If 'affiliations' exist in the source document ('doc'), they are merged into the target document ('target_doc').

        Parameters:
        ----------
        self : object
            The object instance.
        target_doc : dict
            The target document where affiliations will be merged.
        doc : dict
            The source document containing affiliations to be merged.
        """
        if 'affiliations' in doc:
            if 'affiliations' not in target_doc:
                target_doc['affiliations'] = []
            target_affiliation_ids = {aff['id']
                                      for aff in target_doc['affiliations']}
            for aff in doc['affiliations']:
                if aff['id'] not in target_affiliation_ids:
                    target_doc['affiliations'].append(copy.deepcopy(aff))

    # Function to merge list fields

    def merge_lists(self, target_ids, source_ids):
        """
        Merges lists by appending unique items from the source list to the target list.

        Parameters:
        ----------
        self : object
            The object instance.
        target_ids : list
            The target list where unique items will be appended.
        source_ids : list
            The source list containing items to be merged.
        """
        for item in source_ids:
            if item not in target_ids:
                target_ids.append(item)

    # Function to merge other fields

    def merge_fields(self, target, source, fields):
        """
        Merges specified fields from a source dictionary into a target dictionary.

        Parameters:
        ----------
        self : object
            The object instance.
        target : dict
            The target dictionary where fields will be merged.
        source : dict
            The source dictionary containing fields to be merged.
        fields : list
            A list of field names to be merged from the source dictionary into the target dictionary.
        """
        for field in fields:
            if not target[field]:
                target[field] = source[field]
            if field == "first_names" or field == "last_names":
                continue
    # Function to merge, store and delete documents

    def merge_documents(self, authors_docs, target_doc):
        """
        Merges information from multiple author documents into a target document, updates the target document in the collection, and deletes other documents.

        Parameters:
        ----------
        self : object
            The object instance.
        authors_docs : list
            A list of author documents containing information to be merged into the target document.
        target_doc : dict
            The target document where information will be merged.
        collection : Collection
            The MongoDB collection to be used.
        """
        target_id = target_doc["_id"]
        other_docs = []
        for doc in authors_docs:
            if doc['_id'] != target_id:
                if not compare_author(target_doc, doc):
                    continue
                # updated
                target_update_sources = {profile["source"]
                                         for profile in target_doc["updated"]}
                for source in doc["updated"]:
                    if source["source"] not in target_update_sources:
                        target_doc["updated"].append(
                            {"source": source["source"], "time": int(time())})

                # full_name
                if len(doc["full_name"]) > len(target_doc["full_name"]):
                    target_doc["full_name"] = doc["full_name"]

                # first_names, last_names, initials, sex, marital_status, birthplace, birthdate
                self.merge_fields(target_doc, doc, [
                    "first_names", "last_names", "initials", "keywords", "sex", "marital_status", "birthplace", "birthdate"])

                # aliases, external_ids, ranking, degrees, subjects, related_works
                fields = ["aliases", "external_ids", "ranking",
                          "degrees", "subjects", "related_works"]
                for field in fields:
                    self.merge_lists(target_doc[field], doc[field])
                # affiliations
                self.merge_affiliations(target_doc, doc)
                other_docs.append(doc)
        # Update the target document with new external ids
        for other_doc in other_docs:
            self.collection_merged.update_one({"_id": other_doc["_id"]}, {
                                              "$set": other_doc}, upsert=True)
            self.collection.delete_one({"_id": other_doc["_id"]})
        self.collection.update_one({"_id": target_id}, {"$set": target_doc})

    # Find the target document based on the 'provenance' of 'external_ids'
    def find_target_doc(self, author_docs, _id):
        """
        Finds the target document among a list of author documents based on specified criteria.

        Parameters:
        ----------
        self : object
            The object instance.
        author_docs : list
            A list of author documents to search through.
        _id : str
            The identifier ('orcid' or 'doi') indicating which type of document to prioritize.

        Returns:
        ----------
        dict or None
            The target document found based on the specified criteria, or None if no target document is found.
        """
        target_doc = None

        # Define the priority order of provenance to search for
        priority_order = ['staff', 'scienti', 'minciencias']
        for provenance in priority_order:
            for doc in author_docs:
                for ext_id in doc.get('external_ids', []):
                    if ext_id['provenance'] == provenance:
                        target_doc = doc
                        break
                if target_doc:
                    break
            if target_doc:
                break

        # Handle the case where _id is 'orcid'
        if not target_doc and _id == 'orcid':
            target_doc = author_docs[0]
        # Handle the case where _id is 'doi'
        elif not target_doc and _id == 'doi':
            target_doc = max(author_docs, key=lambda doc: len(
                doc.get('full_name', '')))

        return target_doc

    # Function to process authors unicity based on ORCID

    def orcid_unicity(self, reg, collection, verbose=0):
        """
        Checks unicity by ORCID id among a group of author documents.

        Parameters:
        ----------
        self : object
            The object instance.
        reg : dict
            A dictionary containing a registry of agggrupated author documents by ORCID id.
        collection : Collection
            The MongoDB collection to be used.
        """
        # Fetch all author documents by given IDs
        author_ids = reg["document_ids"]
        author_docs = list(collection.find(
            {"_id": {"$in": [ObjectId(aid) for aid in author_ids]}}))
        if not author_docs:
            print("No authors found with the provided IDs.")
            return

        target_doc = self.find_target_doc(author_docs, "orcid")
        if target_doc:
            self.merge_documents(author_docs, target_doc)

    # Function to compare authors based on DOI

    def doi_unicity(self, reg, verbose=0):
        """
        Checks unicity by DOI among a group of author documents.

        Parameters:
        ----------
        self : object
            The object instance.
        reg : dict
            A dictionary containing a registry of agggrupated author documents by DOI.
        collection : Collection
            The MongoDB collection to be used.
        """
        # Fetch author documents from the database
        author_ids = reg["authors"]
        author_docs = list(self.collection.find({"_id": {"$in": author_ids}}))

        if not author_docs:
            return

        # Set the authors_filter flag to True
        authors_filter = True

        found = []  # we will create a list of sets of authors, every set in the list have to be merge
        for author in author_docs:
            # Filter authors based on the source of the related_works
            if authors_filter:
                source_match = any(
                    source in ['staff', 'scienti', 'minciencias', "scholar"] for source in [updt["source"] for updt in author["updated"]]
                )
                if not source_match:
                    # Skip the author if the source of the related_works is not in ['staff', 'scienti', 'minciencias']
                    continue

            for other_author in author_docs:
                if author["_id"] == other_author["_id"]:
                    continue
                # Perform the author comparison
                if compare_author(author, other_author):
                    if not found:
                        found.append(set([author["_id"], other_author["_id"]]))
                    else:
                        for i, author_set in enumerate(found):
                            # if the author is in the current set then add it to the set
                            if author_set.intersection([author["_id"], other_author["_id"]]):
                                found[i] = author_set.union(
                                    [author["_id"], other_author["_id"]])
                            else:
                                found.append(
                                    set([author["_id"], other_author["_id"]]))
        for author_found in found:
            author_docs_ = list(self.collection.find(
                {"_id": {"$in": list(author_found)}}))

            if not author_docs_:
                continue

            target_doc = self.find_target_doc(author_docs_, "doi")
            if target_doc:
                self.merge_documents(author_docs_, target_doc)

    def process_authors(self):
        """
        Processes authors' information including checking unicity by ORCID id and DOI among author documents.

        Parameters:
        ----------
        self : object
            The object instance.
        """
        # ORCID unicity
        if isinstance(self.task, list) and "orcid" in self.task:
            pipeline = [
                {"$unwind": "$external_ids"},
                {"$match": {"external_ids.source": "orcid"}},
                {"$group": {"_id": "$external_ids.id", "document_ids": {
                    "$addToSet": "$_id"}, "count": {"$sum": 1}}},
                {"$match": {"count": {"$gt": 1}}}
            ]
            authors_cursor = list(self.collection.aggregate(
                pipeline, allowDiskUse=True))
            print("INFO: ORCID unicity for groups of authors is started!")
            with MongoClient(self.mongodb_url) as client:
                Parallel(
                    n_jobs=self.n_jobs,
                    verbose=self.verbose,
                    backend="threading")(
                    delayed(self.orcid_unicity)(
                        reg,
                        self.collection,
                        self.verbose
                    ) for reg in authors_cursor
                )
                client.close()
            if self.verbose > 1:
                print("ORCID unicity for {} groups of authors is done!".format(
                    len(authors_cursor)))

        # DOI unicity
        if isinstance(self.task, list) and "doi" in self.task:
            if self.authors_threshold == 0:
                pepeline_count = {"$gt": 1}
            else:
                pepeline_count = {"$gt": 1, "$lte": self.authors_threshold}
            pipeline = [
                {"$unwind": "$related_works"},
                {"$match": {"related_works.source": "doi"}},
                {"$group": {"_id": "$related_works.id", "authors": {
                    "$addToSet": "$_id"}, "count": {"$sum": 1}}},
                {"$match": {"count": pepeline_count}}
            ]
            authors_cursor = list(self.collection.aggregate(
                pipeline, allowDiskUse=True))

            print("INFO: DOI unicity for groups of authors is started!")
            with MongoClient(self.mongodb_url) as client:
                Parallel(
                    n_jobs=self.n_jobs,
                    verbose=self.verbose,
                    backend="threading")(
                    delayed(self.doi_unicity)(
                        reg,
                        self.verbose
                    ) for reg in authors_cursor
                )
                client.close()
            if self.verbose > 1:
                print("DOI unicity for {} groups of authors is done!".format(
                    len(authors_cursor)))

        else:
            if self.verbose > 1:
                print("Invalid task! Please provide a valid task.")

    def run(self):
        self.process_authors()
        return 0
