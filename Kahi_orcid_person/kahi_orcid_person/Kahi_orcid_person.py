from kahi.KahiBase import KahiBase
from kahi.progress import ProgressLogger
from kahi_impactu_utils.Utils import split_name_part, parse_orcid_id_from_url, normalize_name
from kahi_impactu_utils.String import has_dash, normalize_dashes
from pymongo import MongoClient, TEXT
from joblib import Parallel, delayed
import copy
from threading import Lock
from time import time


def orcid_name_compatible(person_full_name, registry_full_name):
    """
    Decide whether a person's name is compatible with the name of an ORCID record.

    Names are compared as sets of normalized tokens (no accents, lowercase,
    initials ignored). They are compatible when they share at least two tokens,
    or every token of the shorter name when one of them has a single token.
    "Ana Perez" and "Juan Perez" share one of two tokens and are not compatible.
    """
    person_tokens = {t for t in normalize_name(person_full_name or "").split() if len(t) > 1}
    registry_tokens = {t for t in normalize_name(registry_full_name or "").split() if len(t) > 1}
    if not person_tokens or not registry_tokens:
        return False
    shared = len(person_tokens & registry_tokens)
    return shared >= min(2, len(person_tokens), len(registry_tokens))


class Kahi_orcid_person(KahiBase):
    """
    A class to process ORCID records and update the 'person' collection in the database.

    This class connects to both the main database and the ORCID database.
    It extracts the ORCID, given names, and family names from ORCID records,
    constructs the full name and initials, and then updates or inserts the corresponding
    document in the 'person' collection using the ORCID as a key.
    """

    def __init__(self, config):
        self.config = config
        self.mongodb_url = config["database_url"]

        # Initialize connection to the main database and set up the 'person' collection
        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        # Create indexes on the 'person' collection for optimized queries
        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        # Initialize connection to the ORCID database and validate its existence
        self.orcid_client = MongoClient(config["orcid_person"]["database_url"])
        if config["orcid_person"]["database_name"] not in self.orcid_client.list_database_names():
            raise Exception("Database {} not found in {}".format(
                config["orcid_person"]['database_name'],
                config["orcid_person"]["database_url"]))
        self.orcid_db = self.orcid_client[config["orcid_person"]
                                          ["database_name"]]
        if config["orcid_person"]["collection_name"] not in self.orcid_db.list_collection_names():
            raise Exception("Collection {}.{} not found in {}".format(
                config["orcid_person"]['database_name'],
                config["orcid_person"]['collection_name'],
                config["orcid_person"]["database_url"]))
        self.orcid_collection = self.orcid_db[config["orcid_person"]
                                              ["collection_name"]]
        # self.orcid_collection_works = self.orcid_db[config["orcid_person"]["collection_name_works"]]
        # self.orcid_collection_works.create_index("")

        # Configure parallel processing parameters
        self.n_jobs = config["orcid_person"]["num_jobs"] if "num_jobs" in config["orcid_person"] else 1
        self.verbose = config["orcid_person"]["verbose"] if "verbose" in config["orcid_person"] else 0

        # Close the initial connection to the main database; it will be reopened in process_orcid
        self.client.close()

    def process_one_update(self, person_reg, query, update_fields, person_collection, orcid):
        """
        Process update for a single ORCID record that already exists in the 'person' collection.

        Parameters
        ----------
        person_reg : dict
            The existing document in the 'person' collection.
        query : dict
            Query to identify the document.
        update_fields : dict
            Fields to update in the document.
        person_collection : Collection
            The collection where the document is stored.
        orcid : str
            The ORCID of the author.

        Returns
        -------
        str
            "actualizados", the outcome reported to the progress logger.
        """
        update_operation = {
            "$addToSet": {
                "updated": {"source": "orcid", "time": int(time())},
                "external_ids": {"provenance": "orcid", "source": "orcid", "id": orcid},
            }
        }
        set_fields = {
            field: value
            for field, value in update_fields.items()
            if field != "aliases" and value and not person_reg.get(field)
        }
        if set_fields:
            update_operation["$set"] = set_fields

        aliases = list(update_fields.get("aliases", []))
        incoming_name = update_fields.get("full_name", "")
        if incoming_name and incoming_name != person_reg.get("full_name"):
            aliases.append(incoming_name)
        aliases = list(dict.fromkeys(alias for alias in aliases if alias))
        if aliases:
            update_operation["$addToSet"]["aliases"] = {"$each": aliases}

        result = person_collection.update_one(query, update_operation)
        if self.verbose > 4:
            print(
                f"ORCID: {orcid} - Updated: Matched documents: {result.matched_count}, Modified documents: {result.modified_count}")
        return "actualizados"

    def resolve_shared_orcid(self, candidates, orcid, registry_full_name, registry_aliases, person_collection):
        """
        Keep an ORCID only on the people whose name is compatible with the ORCID record.

        ScienTI and staff sometimes assign the same ORCID to different people. When
        several documents carry `orcid`, the record's name decides: the identifier,
        the aliases copied from the record and the orcid `updated` mark are removed
        from the incompatible documents. Nothing changes when every candidate (or
        none) is compatible. Every decision is written to `orcid_person_shared_orcids`.

        Returns the candidates that keep the ORCID.
        """
        owners = [c for c in candidates if orcid_name_compatible(c.get("full_name"), registry_full_name)]
        others = [c for c in candidates if c not in owners]
        audit = {
            "orcid": orcid, "registry_name": registry_full_name, "time": int(time()),
            "candidates": [{"_id": c["_id"], "full_name": c.get("full_name", "")} for c in candidates],
            "kept": [c["_id"] for c in owners],
        }
        if not owners or not others:
            audit["decision"] = "ambiguous" if owners else "no_match"
            audit["removed"] = []
            person_collection.database["orcid_person_shared_orcids"].replace_one(
                {"orcid": orcid}, audit, upsert=True)
            return candidates
        alias_values = [a for a in ([registry_full_name] + list(registry_aliases or [])) if a]
        person_collection.update_many(
            {"_id": {"$in": [c["_id"] for c in others]}},
            {"$pull": {
                "external_ids": {"id": orcid},
                "aliases": {"$in": alias_values},
                "updated": {"source": "orcid"},
            }},
        )
        audit["decision"] = "reassigned"
        audit["removed"] = [c["_id"] for c in others]
        person_collection.database["orcid_person_shared_orcids"].replace_one(
            {"orcid": orcid}, audit, upsert=True)
        with self._stats_lock:
            self.shared_orcids_removed += len(others)
        if self.verbose > 2:
            print(f"ORCID: {orcid} retirado de {len(others)} persona(s) incompatible(s) con {registry_full_name!r}")
        return owners

    def process_one_insert(self, person_reg, query, update_fields, person_collection, empty_person, orcid):
        """
        Process insertion for a single ORCID record that does not exist in the 'person' collection.

        Parameters
        ----------
        person_reg : dict
            The existing document in the 'person' collection (for consistency).
        query : dict
            Query to identify the document (for consistency).
        update_fields : dict
            Fields to insert in the document.
        person_collection : Collection
            The collection where the document will be inserted.
        empty_person : dict
            Empty template for a new author record.
        orcid : str
            The ORCID of the author.

        Returns
        -------
        str
            "nuevos", the outcome reported to the progress logger.
        """
        # Create a new document using the empty person template
        entry = copy.deepcopy(empty_person)
        # Update the template with the extracted fields
        entry.update(update_fields)
        # set the updated field with the current date
        entry["updated"].append({"source": "orcid", "time": int(time())})
        # Set the external_ids field with the required format for ORCID
        entry["external_ids"] = [{
            'provenance': 'orcid',
            'source': 'orcid',
            'id': orcid
        }]
        result = person_collection.insert_one(entry)
        if self.verbose > 4:
            print(
                f"ORCID: {orcid} - Inserted document with id: {result.inserted_id}")
        return "nuevos"

    def process_one(self, orcid_author, person_collection, empty_person, related_works):
        """
        Process a single ORCID record to extract personal details and either update or insert the corresponding document
        in the 'person' collection based on whether the record exists.

        Parameters
        ----------
        orcid_author : dict
            An ORCID record.
        person_collection : Collection
            Authors collection in the provided database.
        empty_person :
            Empty template for an author record.
        related_works : list
            Placeholder for related works (pending implementation).

        Returns
        -------
        str or None:
            "actualizados" or "nuevos", or None if the record was skipped (no ORCID or no name).
        """
        # Extract the record and retrieve the ORCID URI
        record = orcid_author.get("record:record", {})
        orcid = record.get("common:orcid-identifier", {}).get("common:uri", "")
        if orcid:
            orcid = parse_orcid_id_from_url(orcid)
        # If ORCID is not found, log a message if verbose is enabled and return None
        if not orcid:
            if self.verbose > 4:
                print("No ORCID found in the provided record.")
            return None

        # Extract personal information including given names and family name
        person = record.get("person:person", {})
        name_data = person.get("person:name", {})
        given_names = name_data.get("personal-details:given-names", "")
        family_name = name_data.get("personal-details:family-name", "")

        # Extract other names if available
        alias = ""
        other_name = person.get("other-name:other-names", {})
        other_names = other_name.get("other-name:other-name", [])
        if isinstance(other_names, list):
            other_name_val = other_names[0] if other_names else ""
            if other_name_val:
                alias = other_name_val.get("other-name:content", "")
        elif isinstance(other_names, dict):
            alias = other_names.get("other-name:content", "")

        # Process the names: split into lists, build full_name, and calculate initials
        raw_full_name = " ".join(
            part.strip() for part in (given_names or "", family_name or "") if part and part.strip())
        given_names = normalize_dashes(given_names.replace(".", "")) if given_names else ""
        family_name = normalize_dashes(family_name.replace(".", "")) if family_name else ""
        first_names_list = split_name_part(given_names) if given_names else []
        last_names_list = split_name_part(family_name) if family_name else []
        full_name = f"{given_names} {family_name}".strip()
        initials = ''.join(name[0].upper()
                           for name in first_names_list) if first_names_list else ""
        aliases = [alias] if alias else []
        if has_dash(raw_full_name) and raw_full_name.lower() not in aliases:
            aliases.append(raw_full_name.lower())

        query = {"external_ids.id": orcid}
        update_fields = {
            "full_name": full_name,
            "first_names": first_names_list,
            "last_names": last_names_list,
            "initials": initials,
            "aliases": aliases,
        }

        # Check if the record exists in the 'person' collection
        candidates = list(person_collection.find(query))
        if len(candidates) > 1:
            candidates = self.resolve_shared_orcid(
                candidates, orcid, full_name, update_fields["aliases"], person_collection)
        person_reg = candidates[0] if candidates else None
        if person_reg:
            return self.process_one_update(person_reg, query, update_fields, person_collection, orcid)
        elif full_name:
            return self.process_one_insert(person_reg, query, update_fields, person_collection, empty_person, orcid)
        return None

    def process_orcid(self):
        """
        Retrieve ORCID records from the ORCID collection and update or insert the corresponding documents
        in the main 'person' collection in parallel.
        """
        # Retrieve only necessary fields from ORCID records with the no_cursor_timeout option enabled
        author_cursor = self.orcid_collection.find(
            {},
            {
                "record:record.common:orcid-identifier.common:uri": 1,
                "record:record.person:person.person:name.personal-details:given-names": 1,
                "record:record.person:person.person:name.personal-details:family-name": 1,
            },
            no_cursor_timeout=True
        )

        # Open a new connection to the main database to perform updates/inserts
        client = MongoClient(self.mongodb_url)
        kahi_db = client[self.config["database_name"]]
        person_collection = kahi_db["person"]

        # Placeholder for related works; might be used in future versions
        related_works = []
        self.shared_orcids_removed = 0
        self._stats_lock = Lock()

        progress = ProgressLogger(
            "orcid_person",
            total=self.orcid_collection.estimated_document_count(),
        ).start(f"num_jobs={self.n_jobs}")

        def tracked(*args):
            progress.step(self.process_one(*args) or "omitidos")

        # Process each ORCID record in parallel using a threading backend
        Parallel(
            n_jobs=self.n_jobs,
            verbose=0,
            backend="threading"
        )(
            delayed(tracked)(
                author,
                person_collection,
                self.empty_person(),
                related_works,
            ) for author in author_cursor
        )
        # Close the main database connection after processing
        client.close()
        progress.done()
        if self.shared_orcids_removed:
            print(f"[orcid_person] ORCID compartidos retirados de {self.shared_orcids_removed} persona(s); "
                  f"detalle en orcid_person_shared_orcids")

    def run(self):
        """
        Main method to process ORCID records and update/insert the 'person' collection.

        Returns
        -------
        int
            0 upon successful execution.
        """
        self.process_orcid()
        return 0
