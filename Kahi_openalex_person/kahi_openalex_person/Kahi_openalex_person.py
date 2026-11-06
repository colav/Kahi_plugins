from kahi.KahiBase import KahiBase
from kahi.progress import ProgressLogger
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from kahi_impactu_utils.Utils import get_id_from_url, split_names
from re import search, sub
from copy import deepcopy
from itertools import islice

from .affiliation_years import (
    InstitutionResolver, canonical_openalex_id, canonical_orcid,
    collect_affiliations, resolve_person, update_person,
)


def author_external_ids(author):
    records = []
    ids = dict(author.get("ids") or {})
    ids.setdefault("openalex", author.get("id"))
    if author.get("orcid") and not ids.get("orcid"):
        ids["orcid"] = author["orcid"]
    for source, value in ids.items():
        if not value:
            continue
        if source == "openalex":
            openalex_id = canonical_openalex_id(value)
            identifier = f"https://openalex.org/{openalex_id}" if openalex_id else None
        elif source == "orcid":
            orcid = canonical_orcid(value)
            identifier = f"https://orcid.org/{orcid}" if orcid else None
        else:
            identifier = get_id_from_url(value)
        if identifier:
            records.append({"provenance": "openalex", "source": source, "id": identifier})
    return records


def author_affiliations(db, author, resolver=None):
    if not (author.get("affiliations") or author.get("last_known_institutions")):
        return []
    return collect_affiliations(author, resolver or InstitutionResolver(db["affiliations"]))


def freeze_identity(value):
    if isinstance(value, dict):
        return tuple(sorted((key, freeze_identity(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(freeze_identity(item) for item in value)
    return value


def related_work_key(work):
    return work.get("source"), freeze_identity(work.get("id"))


def external_id_key(record):
    source, value = record.get("source"), record.get("id")
    if source == "openalex":
        value = canonical_openalex_id(value) or value
    elif source == "orcid":
        value = canonical_orcid(value) or value
    return source, freeze_identity(value)


def is_plausible_person_name(name):
    """Reject empty labels and obvious non-person placeholders."""
    return bool(
        name
        and name.strip()
        and any(character.isalpha() for character in name)
        and not search(r"[-_]{2,}", name)
    )


def was_openalex_author_processed(person, openalex_id):
    """Return whether this exact OpenAlex author completed a previous run."""
    if not person or not openalex_id:
        return False
    was_updated = any(
        update.get("source") == "openalex"
        for update in person.get("updated", [])
    )
    has_exact_id = any(
        external_id.get("source") == "openalex"
        and canonical_openalex_id(external_id.get("id")) == openalex_id
        for external_id in person.get("external_ids", [])
    )
    return was_updated and has_exact_id


def get_related_works(openalex_works, author_id):
    """Load DOI evidence only when an author still needs processing."""
    return list(openalex_works.aggregate(
        [
            {"$project": {
                "ids": 1,
                "publication_year": 1,
                "authorships": 1,
            }},
            {"$match": {"authorships.author.id": author_id}},
            {"$unwind": "$authorships"},
            {"$match": {"authorships.author.id": author_id}},
            {"$project": {
                "ids": 1,
                "publication_year": 1,
                "authorships.institutions": 1,
            }},
        ]
    ))


def process_one_insert(person_db, oa_author, client, db_name, empty_person, related_works,
                       institution_resolver=None):
    """"
    Insert a new OpenAlex author record into the database.

    Parameters:
    person_db (dict):
        The existing OpenAlex author record in the database.
    oa_author (dict):
        The new OpenAlex author record to insert into the database.
    client (MongoClient):
        The MongoDB client to use for database operations.
    db_name (str):
        The name of the database to use.
    empty_person (dict):
        A template for an empty person document.
    related_works (list):
        A list of related works to associate with the person.
    max_tries (int):
        The maximum number of attempts to insert the record into the database.
    """
    db = client[db_name]
    collection = db["person"]

    entry = deepcopy(empty_person)
    entry["updated"].append({"source": "openalex", "time": int(time())})

    entry["full_name"] = sub(
        r'\s+', ' ', oa_author["display_name"].replace(".", " ")).strip()
    try:
        author_data = split_names(entry["full_name"])
    except Exception as e:
        raise RuntimeError(
            f"Unable to split OpenAlex author name {entry['full_name']!r}"
        ) from e
    entry["full_name"] = entry["full_name"].replace("-", " ")
    entry["last_names"] = author_data["last_names"]
    entry["first_names"] = author_data["first_names"]
    entry["initials"] = author_data["initials"]
    for name in oa_author.get("display_name_alternatives") or []:
        if not name.lower() in entry["aliases"]:
            entry["aliases"].append(name.lower())
    entry["external_ids"].extend(author_external_ids(oa_author))
    entry["affiliations"].extend(author_affiliations(db, oa_author, institution_resolver))

    related_work_keys = set()
    for rwork in related_works:
        for key in rwork["ids"].keys():
            if key == "doi":
                rec = {"provenance": "openalex",
                       "source": key, "id": rwork["ids"][key], "year": rwork["publication_year"], "institutions": rwork["authorships"]["institutions"]}
                key = related_work_key(rec)
                if key not in related_work_keys:
                    entry["related_works"].append(rec)
                    related_work_keys.add(key)
                break
    collection.insert_one(entry)
    return "nuevos"


def process_one_update(person_db, oa_author, client, db_name, empty_person, related_works,
                       institution_resolver=None):
    """"
    Update an existing OpenAlex author record in the database with new information.

    Parameters:
    person_db (dict):
        The existing OpenAlex author record in the database.
    oa_author (dict):
        The new OpenAlex author record to update the existing record with.
    client (MongoClient):
        The MongoDB client to use for database operations.
    db_name (str):
        The name of the database to use.
    empty_person (dict):
        A template for an empty person document.
    related_works (list):
        A list of related works to associate with the person.
    """
    db = client[db_name]
    collection = db["person"]

    entry = deepcopy(empty_person)
    entry["updated"].append({
        "source": "openalex",
        "time": int(time())
    })
    # Update the aliases
    for name in oa_author.get("display_name_alternatives") or []:
        if name.lower() not in person_db.get("aliases", []) and name.lower() not in entry["aliases"]:
            entry["aliases"].append(name.lower())

    # Iterate over OpenAlex IDs and add any that are not already in the document
    new_external_ids = []
    external_id_values = {
        external_id_key(record)
        for record in person_db.get("external_ids", [])
    }
    for record in author_external_ids(oa_author):
        identity = external_id_key(record)
        if identity not in external_id_values:
            new_external_ids.append(record)
            external_id_values.add(identity)
    entry["external_ids"] = new_external_ids
    affiliations = author_affiliations(db, oa_author, institution_resolver)

    related_work_keys = {
        related_work_key(work)
        for work in person_db.get("related_works", [])
    }
    for rwork in related_works:
        for key in rwork["ids"].keys():
            if key == "doi":
                rec = {"provenance": "openalex",
                       "source": key, "id": rwork["ids"][key], "year": rwork["publication_year"], "institutions": rwork["authorships"]["institutions"]}
                key = related_work_key(rec)
                if key not in related_work_keys:
                    entry["related_works"].append(rec)
                    related_work_keys.add(key)
                break

    # Update the person document in the database
    update_person(collection, person_db, oa_author, affiliations, {
        field: entry[field] for field in ("updated", "aliases", "external_ids", "related_works")
    })
    return "actualizados"


def process_one(
        oa_author, client, db_name, empty_person, openalex_works,
        skip_previously_processed=False, institution_resolver=None):
    """"
    Process a single OpenAlex record to extract personal details and either update or insert the corresponding document
    in the 'person' collection based on whether the record exists.

    Parameters:
    oa_author (dict):
        The OpenAlex author record to process.
    client (MongoClient):
        The MongoDB client to use for database operations.
    db_name (str):
        The name of the database to use.
    empty_person (dict):
        A template for an empty person document.
    related_works (list):
        A list of related works to associate with the person.
    """
    if not is_plausible_person_name(oa_author.get("display_name")):
        return None

    db = client[db_name]
    collection = db["person"]

    openalex_id = canonical_openalex_id(
        oa_author.get("ids", {}).get("openalex") or oa_author.get("id")
    )
    person_db, identity_ok = resolve_person(collection, oa_author)
    if not identity_ok:
        return None
    if (
        skip_previously_processed
        and was_openalex_author_processed(person_db, openalex_id)
    ):
        update_person(collection, person_db, oa_author,
                      author_affiliations(db, oa_author, institution_resolver))
        return None

    related_works = get_related_works(openalex_works, oa_author["id"])
    if person_db:
        return process_one_update(
            person_db, oa_author, client, db_name, empty_person, related_works,
            institution_resolver)
    else:
        return process_one_insert(
            person_db, oa_author, client, db_name, empty_person, related_works,
            institution_resolver)


class Kahi_openalex_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])
        self.db["affiliations"].create_index("external_ids.id")

        self.openalex_client = MongoClient(
            config["openalex_person"]["database_url"])
        if config["openalex_person"]["database_name"] not in self.openalex_client.list_database_names():
            raise Exception("Database {} not found in {}".format(
                config["openalex_person"]['database_name'], config["openalex_person"]["database_url"]))
        self.openalex_db = self.openalex_client[config["openalex_person"]
                                                ["database_name"]]
        if config["openalex_person"]["collection_name"] not in self.openalex_db.list_collection_names():
            raise Exception("Collection {}.{} not found in {}".format(config["openalex_person"]['database_name'],
                                                                      config["openalex_person"]['collection_name'], config["openalex_person"]["database_url"]))
        self.openalex_collection = self.openalex_db[config["openalex_person"]
                                                    ["collection_name"]]
        self.openalex_collection_works = self.openalex_db[config["openalex_person"]
                                                          ["collection_name_works"]]
        self.openalex_collection_works.create_index("authorships.author.id")

        self.n_jobs = config["openalex_person"]["num_jobs"] if "num_jobs" in config["openalex_person"].keys(
        ) else 1
        self.verbose = config["openalex_person"]["verbose"] if "verbose" in config["openalex_person"].keys(
        ) else 0
        self.skip_previously_processed = config["openalex_person"].get(
            "skip_previously_processed", False
        )
        self.batch_size = int(config["openalex_person"].get("batch_size", 500))
        if self.batch_size < 1:
            raise ValueError("openalex_person.batch_size must be positive")

        self.client.close()

    def process_openalex(self):
        progress = ProgressLogger(
            "openalex_person",
            total=self.openalex_collection.estimated_document_count(),
        ).start(f"num_jobs={self.n_jobs} batch_size={self.batch_size}")

        def tracked(*args):
            # None: nombre no plausible, identidad ambigua o autor ya procesado (reanudacion).
            progress.step(process_one(*args) or "omitidos")

        author_cursor = self.openalex_collection.find(no_cursor_timeout=True)
        client = MongoClient(self.mongodb_url)
        institution_resolver = InstitutionResolver(client[self.config["database_name"]]["affiliations"])
        try:
            with Parallel(n_jobs=self.n_jobs, verbose=0, backend="threading") as parallel:
                while batch := list(islice(author_cursor, self.batch_size)):
                    institution_resolver.prefetch(batch)
                    parallel(delayed(tracked)(
                        author, client, self.config["database_name"], self.empty_person(),
                        self.openalex_collection_works, self.skip_previously_processed,
                        institution_resolver,
                    ) for author in batch)
        finally:
            author_cursor.close()
            client.close()
        progress.done()

    def run(self):
        self.process_openalex()
        return 0
