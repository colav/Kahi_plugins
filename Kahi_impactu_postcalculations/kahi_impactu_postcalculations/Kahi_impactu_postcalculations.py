from joblib import Parallel, delayed
from kahi.KahiBase import KahiBase
from pymongo import MongoClient
import subprocess
from spacy import cli, load
from kahi_impactu_postcalculations.process_one import (
    network_creation_process_one,
    top_words_process_one,
    top_words_from_works_scan,
    count_works_one,
    load_nlp_models,
)
from kahi_impactu_postcalculations.indexes import create_indexes
from kahi_impactu_postcalculations.denormalization import denormalize
from kahi_impactu_postcalculations.typing import (
    build_type_lookup,
    clear_impactu_types,
    process_type,
)
from kahi_impactu_postcalculations.topics import (
    _topic_payload,
    load_openalex_topics,
    process_topic_batch,
    process_topic_batches_pool,
    request_topic_inference_batch,
)
from kahi_impactu_postcalculations.person_persistent_ids import (
    process_person_id,
    recover_person_id_migration, finish_person_id_migration,
)
from kahi_impactu_postcalculations.geographic_normalization import (
    SpanishGeographicNormalizer,
    normalize_affiliation_addresses,
)
from kahi_impactu_postcalculations.openalex_enrichment import (
    enrich_works_from_openalex,
)
from kahi_impactu_postcalculations.author_reconciliation import (
    reconcile_collection_authors,
)
from kahi_impactu_postcalculations.abstract_enrichment import (
    enrich_missing_abstracts,
)
from kahi_impactu_postcalculations.person_topics import (
    aggregate_person_topics,
)
from kahi_impactu_type_catalog import get_impactu_catalog
import requests
from itertools import islice


def batched(iterable, size):
    """Yield bounded lists from a cursor without materializing it completely."""
    iterator = iter(iterable)
    while batch := list(islice(iterator, size)):
        yield batch


class Kahi_impactu_postcalculations(KahiBase):
    """
    Plugin for performing post calculations for Impactu.

    This class extends KahiBase and implements functions for creating co-authorship networks and
    extracting top words
    """

    config = {}

    def __init__(self, config):
        """
        Initialize the Kahi_impactu_postcalculations plugin.
        """
        self.config = config

        self.mongodb_url = config["database_url"]
        self.database_name = config["database_name"]

        self.impactu_database_url = config["impactu_postcalculations"]["database_url"]
        self.impactu_database_name = config["impactu_postcalculations"]["database_name"]
        self.backend = self.config["impactu_postcalculations"]["backend"]
        self.verbose = self.config["impactu_postcalculations"]["verbose"]
        self.n_jobs = self.config["impactu_postcalculations"]["n_jobs"]
        self.openalex_database_url = self.config["impactu_postcalculations"]["openalex_database_url"]
        self.openalex_database_name = self.config["impactu_postcalculations"]["openalex_database_name"]
        topic_config = self.config["impactu_postcalculations"]
        self.topic_inference_mode = topic_config.get(
            "topic_inference_mode", "local"
        )
        if self.topic_inference_mode not in {"local", "remote"}:
            raise ValueError("topic_inference_mode must be 'local' or 'remote'")
        self.inference_endpoint = topic_config["inference_endpoint"]
        configured_endpoints = topic_config.get("inference_endpoints")
        self.inference_endpoints = (
            list(configured_endpoints)
            if configured_endpoints
            else [self.inference_endpoint]
        )
        if not all(
            isinstance(endpoint, str) and endpoint.strip()
            for endpoint in self.inference_endpoints
        ):
            raise ValueError("inference_endpoints must contain non-empty URLs")
        self.inference_endpoints = list(dict.fromkeys(self.inference_endpoints))
        self.force_recalculate = self.config["impactu_postcalculations"].get(
            "force_recalculate", False
        )
        self.networks_enabled = self.config["impactu_postcalculations"].get(
            "networks_enabled", True
        )
        self.topics_enabled = self.config["impactu_postcalculations"].get(
            "topics_enabled", True
        )
        self.topic_jobs = self.config["impactu_postcalculations"].get(
            "topic_jobs", min(self.n_jobs, 4)
        )
        self.topic_batch_size = self.config["impactu_postcalculations"].get(
            "topic_batch_size", 1
        )
        if self.topic_batch_size < 1:
            raise ValueError("topic_batch_size must be greater than zero")
        self.topic_request_timeout = self.config["impactu_postcalculations"].get(
            "topic_request_timeout", 300
        )
        self.topic_request_retries = self.config["impactu_postcalculations"].get(
            "topic_request_retries", 3
        )
        if self.topic_request_retries < 1:
            raise ValueError("topic_request_retries must be greater than zero")
        self.topic_progress_interval = topic_config.get(
            "topic_progress_interval", 100
        )
        self.person_topics_enabled = topic_config.get(
            "person_topics_enabled", False
        )
        self.person_topics_dry_run = topic_config.get(
            "person_topics_dry_run", True
        )
        self.person_topics_jobs = topic_config.get(
            "person_topics_jobs", self.n_jobs
        )
        self.person_topics_batch_size = topic_config.get(
            "person_topics_batch_size", 1000
        )
        self.person_topics_min_works = topic_config.get(
            "person_topics_min_works", 2
        )
        self.person_topics_limit = topic_config.get(
            "person_topics_limit", 5
        )
        self.person_topics_singleton_min_score = topic_config.get(
            "person_topics_singleton_min_score", 0.9
        )
        self.person_topics_progress_interval = topic_config.get(
            "person_topics_progress_interval", 10000
        )
        self.person_topics_staging_collection = topic_config.get(
            "person_topics_staging_collection", "person_topics_tmp"
        )
        self.person_topics_runs_collection = topic_config.get(
            "person_topics_runs_collection", "person_topics_runs"
        )
        for name, value in (
            ("person_topics_jobs", self.person_topics_jobs),
            ("person_topics_batch_size", self.person_topics_batch_size),
            ("person_topics_min_works", self.person_topics_min_works),
            ("person_topics_limit", self.person_topics_limit),
            (
                "person_topics_progress_interval",
                self.person_topics_progress_interval,
            ),
        ):
            if value < 1:
                raise ValueError(f"{name} must be greater than zero")
        if not 0 <= self.person_topics_singleton_min_score <= 1:
            raise ValueError(
                "person_topics_singleton_min_score must be between zero and one"
            )
        self.denormalization_enabled = self.config["impactu_postcalculations"].get(
            "denormalization_enabled", True
        )
        self.persistent_ids_enabled = self.config["impactu_postcalculations"].get(
            "persistent_ids_enabled", True
        )
        self.author_reconciliation_enabled = self.config[
            "impactu_postcalculations"
        ].get("author_reconciliation_enabled", False)
        self.author_reconciliation_dry_run = self.config[
            "impactu_postcalculations"
        ].get("author_reconciliation_dry_run", True)
        self.author_reconciliation_collections = self.config[
            "impactu_postcalculations"
        ].get("author_reconciliation_collections", ["works", "events"])
        self.author_reconciliation_batch_size = self.config[
            "impactu_postcalculations"
        ].get("author_reconciliation_batch_size", 1000)
        self.author_reconciliation_progress_interval = self.config[
            "impactu_postcalculations"
        ].get("author_reconciliation_progress_interval", 10000)
        self.author_reconciliation_skip_affiliation_conflicts = self.config[
            "impactu_postcalculations"
        ].get("author_reconciliation_skip_affiliation_conflicts", True)
        if self.author_reconciliation_batch_size < 1:
            raise ValueError(
                "author_reconciliation_batch_size must be greater than zero"
            )
        self.types_enabled = self.config["impactu_postcalculations"].get(
            "types_enabled", True
        )
        self.geographic_normalization_enabled = self.config[
            "impactu_postcalculations"
        ].get("geographic_normalization_enabled", True)
        self.geographic_normalization_dry_run = self.config[
            "impactu_postcalculations"
        ].get("geographic_normalization_dry_run", False)
        self.geographic_batch_size = self.config["impactu_postcalculations"].get(
            "geographic_batch_size", 1000
        )
        self.openalex_enrichment_enabled = self.config[
            "impactu_postcalculations"
        ].get("openalex_enrichment_enabled", True)
        self.openalex_enrichment_dry_run = self.config[
            "impactu_postcalculations"
        ].get("openalex_enrichment_dry_run", False)
        self.openalex_enrichment_batch_size = self.config[
            "impactu_postcalculations"
        ].get("openalex_enrichment_batch_size", 1000)
        if self.openalex_enrichment_batch_size < 1:
            raise ValueError(
                "openalex_enrichment_batch_size must be greater than zero"
            )
        self.abstracts_enabled = self.config["impactu_postcalculations"].get(
            "abstracts_enrichment_enabled",
            self.config["impactu_postcalculations"].get(
                "abstracts_enabled", False
            ),
        )
        self.abstracts_dry_run = self.config["impactu_postcalculations"].get(
            "abstracts_dry_run", True
        )
        self.abstracts_database_url = self.config["impactu_postcalculations"].get(
            "abstracts_database_url", self.mongodb_url
        )
        self.abstracts_database_name = self.config["impactu_postcalculations"].get(
            "abstracts_database_name", "impactu_abstract_resolver"
        )
        self.abstracts_collection_name = self.config[
            "impactu_postcalculations"
        ].get("abstracts_collection_name", "resolved")
        self.abstracts_runs_collection_name = self.config[
            "impactu_postcalculations"
        ].get("abstracts_runs_collection_name", "kahi_enrichment_runs")
        self.abstracts_batch_size = self.config["impactu_postcalculations"].get(
            "abstracts_batch_size", 1000
        )
        self.abstracts_title_threshold = self.config[
            "impactu_postcalculations"
        ].get("abstracts_title_threshold", 90.0)
        if self.abstracts_batch_size < 1:
            raise ValueError("abstracts_batch_size must be greater than zero")
        if not 0 <= self.abstracts_title_threshold <= 100:
            raise ValueError(
                "abstracts_title_threshold must be between 0 and 100"
            )
        self.top_words_backend = self.config["impactu_postcalculations"].get(
            "top_words_backend", "threading"
        )
        self.top_words_enabled = self.config["impactu_postcalculations"].get(
            "top_words_enabled", True
        )
        if self.top_words_backend not in ["threading", "multiprocessing"]:
            raise ValueError("top_words_backend must be 'threading' or 'multiprocessing'")
        self.top_words_jobs = self.config["impactu_postcalculations"].get(
            "top_words_jobs", min(self.n_jobs, 4)
        )
        if self.top_words_jobs < 1:
            raise ValueError("top_words_jobs must be greater than zero")
        self.top_words_strategy = self.config["impactu_postcalculations"].get(
            "top_words_strategy", "legacy"
        )
        if self.top_words_strategy not in ["legacy", "works_scan"]:
            raise ValueError("top_words_strategy must be 'legacy' or 'works_scan'")
        self.top_words_batch_size = self.config["impactu_postcalculations"].get(
            "top_words_batch_size", 5000
        )
        if self.top_words_batch_size < 1:
            raise ValueError("top_words_batch_size must be greater than zero")
        self.parallel_collections = True
        self.collection_jobs = 3

        self.author_count = self.config["impactu_postcalculations"][
            "author_count"] if "author_count" in self.config["impactu_postcalculations"] else 6
        self._check_and_install_spacy_models()
        self.types_priority = ["minciencias", "scienti", "ciarp", "cvlac", "gruplac",
                               "coar", "redcol", "eu-repo", "openalex", "scholar", "crossref"]
        self.person_priority = ["scienti", "orcid",
                                "scholar", "openalex", "mongodb_id"]
        self.type_catalog = get_impactu_catalog()
        self.types = self.type_catalog

    def _check_and_install_spacy_models(self):
        """
        Check if the spaCy models are installed and install them if needed.
        """
        print("INFO: Checking spaCy models")
        if not self.are_spacy_models_installed():
            print("INFO: Installing spaCy models")
            self.install_spacy_models()
        self.en_model = load('en_core_web_sm')
        self.es_model = load('es_core_news_sm')
        self.stopwords = self.en_model.Defaults.stop_words.union(
            self.es_model.Defaults.stop_words)
        # Load the spaCy models in process one for parallel computing
        load_nlp_models()

    def are_spacy_models_installed(self):
        """
        Check if the required spaCy models are installed.

        Returns:
            bool: True if models are installed, False otherwise.
        """
        return "en_core_web_sm" in cli.info()["pipelines"].keys() and "es_core_news_sm" in cli.info()["pipelines"].keys()

    def install_spacy_models(self):
        """
        Install the required spaCy models.
        """
        subprocess.run(["python3", "-m", "spacy",
                       "download", "en_core_web_sm"])
        subprocess.run(["python3", "-m", "spacy",
                       "download", "es_core_news_sm"])

    def check_topic_inference_service(self):
        """Fail fast when any configured topic endpoint is unavailable."""
        for endpoint in self.inference_endpoints:
            ping_endpoint = endpoint.rsplit("/", 1)[0] + "/ping"
            try:
                response = requests.get(ping_endpoint, timeout=10)
                response.raise_for_status()
            except requests.RequestException as error:
                raise RuntimeError(
                    f"Topic inference service is unavailable at {ping_endpoint}"
                ) from error

    def preflight(self, db, impactu_db, openalex_db, abstracts_db=None):
        """Validate every enabled phase before the first database mutation."""
        for name, database in (
            (self.database_name, db),
            (self.impactu_database_name, impactu_db),
            (self.openalex_database_name, openalex_db),
        ):
            database.client.admin.command("ping")
            print(f"INFO: Preflight MongoDB OK: {name}")

        required = {"works", "person", "affiliations"}
        missing = required.difference(db.list_collection_names())
        if missing:
            raise RuntimeError(
                f"Preflight missing required collections: {sorted(missing)}"
            )
        if not self.type_catalog.payload.get("mappings"):
            raise RuntimeError("Preflight ImpactU type catalog is empty")
        SpanishGeographicNormalizer()
        if (
            self.openalex_enrichment_enabled
            and "works" not in openalex_db.list_collection_names()
        ):
            raise RuntimeError("Preflight OpenAlex works collection is missing")
        if getattr(self, "abstracts_enabled", False):
            if abstracts_db is None:
                raise RuntimeError("Preflight abstract resolver database is missing")
            abstracts_db.client.admin.command("ping")
            print(
                "INFO: Preflight MongoDB OK: "
                f"{self.abstracts_database_name}"
            )
            if self.abstracts_collection_name not in abstracts_db.list_collection_names():
                raise RuntimeError(
                    "Preflight abstract resolver collection is missing: "
                    f"{self.abstracts_collection_name}"
                )

        # Validate the MongoDB operator used by the type phase.
        list(db["works"].aggregate([
            {"$limit": 1},
            {"$project": {
                "preflight_sort": {
                    "$sortArray": {"input": [2, 1], "sortBy": 1}
                }
            }},
        ]))

        # Exercise both language models used by works-scan top words.
        if self.top_words_enabled and self.top_words_strategy == "works_scan":
            if not self.es_model("prueba") or not self.en_model("test"):
                raise RuntimeError("Preflight spaCy models returned no tokens")

        if self.topics_enabled:
            self.check_topic_inference_service()
            if openalex_db["topics"].estimated_document_count() == 0:
                raise RuntimeError("Preflight OpenAlex topic catalog is empty")
            sample = None
            for work in db["works"].find(
                {
                    "primary_topic": {"$in": [{}, None]},
                    "abstracts.0": {"$exists": True},
                    "titles.0.title": {"$type": "string"},
                },
                {"titles": 1, "abstracts": 1, "source": 1},
            ).limit(20):
                sample = _topic_payload(work)
                if sample is not None:
                    break
            if sample is not None:
                for endpoint in self.inference_endpoints:
                    response = request_topic_inference_batch(
                        [sample], endpoint,
                        min(self.topic_request_timeout, 120),
                    )
                    response.raise_for_status()
                    predictions = response.json()
                    if (
                        not isinstance(predictions, list)
                        or len(predictions) != 1
                        or not isinstance(predictions[0], list)
                    ):
                        raise RuntimeError(
                            "Preflight topic inference returned an invalid response shape"
                        )
                    valid = [
                        prediction for prediction in predictions[0]
                        if isinstance(prediction, dict)
                        and prediction.get("topic_id") not in (None, -1)
                        and "topic_score" in prediction
                    ]
                    if not valid:
                        print(
                            "INFO: Preflight topic inference OK: model returned "
                            f"the valid no-classification marker (-1) at {endpoint}"
                        )
                    else:
                        topic_id = "https://openalex.org/T" + str(
                            valid[0]["topic_id"]
                        )
                        if openalex_db["topics"].count_documents(
                            {"id": topic_id}, limit=1
                        ) == 0:
                            raise RuntimeError(
                                "Preflight predicted topic is absent from OpenAlex: "
                                f"{topic_id}"
                            )
                        print(
                            "INFO: Preflight topic inference OK: "
                            f"{topic_id} at {endpoint}"
                        )
            else:
                print("INFO: Preflight topics OK: no pending eligible works")
        print("INFO: Preflight completed successfully")

    def process_types(self, db):
        warnings_enabled = self.verbose >= 4
        result = clear_impactu_types(db)
        print(
            "INFO: removed previous ImpactU types from "
            f"{result.modified_count} works"
        )
        for source in self.types_priority:
            print(f"INFO: processing types for {source}")
            type_lookup = build_type_lookup(self.types, source)
            pipe = [{"$match": {"types.source": {"$ne": "impactu"}}},
                    {"$match": {"types.source": source}},
                    {"$unwind": "$types"},
                    {"$match": {"types.source": source}},
                    {"$group": {"_id": "$_id", "types": {"$push": "$types"}}},
                    {"$set": {
                        "types": {
                            "$sortArray": {"input": "$types", "sortBy": {"level": 1}}
                        }
                    }
            },
                {"$project": {"types": 1}},
            ]
            data = db["works"].aggregate(pipe)
            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading",
            )(
                delayed(process_type)(
                    db,
                    work,
                    source,
                    type_lookup,
                    verbose=warnings_enabled,
                )
                for work in data
            )

    def process_person_ids(self, client):
        db = client[self.database_name]
        existing_collections = set(db.list_collection_names())
        product_cols = [
            db[collection_name]
            for collection_name in ("works", "patents", "events", "projects")
            if collection_name in existing_collections
        ]

        # These indexes must exist before reference migration. Creating them
        # afterwards would make every update scan the complete collection.
        for product_col in product_cols:
            product_col.create_index("authors.id")
        db["person"].create_index("external_ids.source")
        db["person"].create_index("_id_migration_complete")

        print("INFO: Recovering interrupted persistent id migrations")
        recovery_cursor = db["person"].find(
            {
                "_id_old": {"$exists": True},
                "_id_migration_complete": {"$ne": True},
            }
        )
        Parallel(n_jobs=self.n_jobs, backend="threading", verbose=10)(
            delayed(recover_person_id_migration)(
                db["person"], product_cols, person
            )
            for person in recovery_cursor
        )

        for source in self.person_priority:
            print("INFO: PERSISTENT ID SOURCE  ", source)
            # Paso 1: Buscar todos los documentos 'person' (con o sin COD_RH)
            if source == "mongodb_id":
                # Si el source es 'mongodb_id', buscar por _id
                cursor = db["person"].find(
                    {"_id_old": {"$exists": False}}
                )
            else:
                # Si el source no es 'mongodb_id', buscar por external_ids.source
                cursor = db["person"].find(
                    {"_id_old": {"$exists": False}, "external_ids.source": source})

            Parallel(n_jobs=self.n_jobs, backend="threading", verbose=10)(
                delayed(process_person_id)(client, db["person"], product_cols, person, source) for person in cursor
            )

        finish_person_id_migration(db["person"])

    def process_networks(self, db, client, impactu_client):
        """Build co-authorship networks for affiliations and people."""
        print("INFO: Getting authors and affiliations ids")
        institutions_ids = []
        for aff in db["affiliations"].find(
            {"types.type": {"$nin": ["faculty", "department", "group"]}},
            {"_id": 1},
        ):
            count = db["works"].count_documents(
                {"authors.affiliations.id": aff["_id"]}
            )
            if count != 0:
                institutions_ids.append(aff["_id"])

        print("INFO: Creating affiliations networks")
        if institutions_ids:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=10,
                backend=self.backend,
            )(
                delayed(network_creation_process_one)(
                    self.config,
                    client if self.backend == "threading" else None,
                    impactu_client if self.backend == "threading" else None,
                    idx,
                    self.author_count,
                    "affiliations",
                    self.backend,
                    self.force_recalculate,
                )
                for idx in institutions_ids
            )

        print("INFO: Checking authors with works")
        authors_ids = [x["_id"] for x in db["person"].find({}, {"_id": 1})]
        authors_ids = Parallel(
            n_jobs=self.n_jobs,
            backend="threading",
            verbose=1,
        )(
            delayed(count_works_one)(db, author)
            for author in authors_ids
        )
        authors_ids = [x for x in authors_ids if x is not None]

        print(f"INFO: total authors {len(authors_ids)}")
        print("INFO: Creating authors networks")
        if authors_ids:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=10,
                backend=self.backend,
            )(
                delayed(network_creation_process_one)(
                    self.config,
                    client if self.backend == "threading" else None,
                    impactu_client if self.backend == "threading" else None,
                    idx,
                    self.author_count,
                    "person",
                    self.backend,
                    self.force_recalculate,
                )
                for idx in authors_ids
            )

    def maybe_process_networks(self, db, client, impactu_client):
        """Run network construction only when enabled by the workflow."""
        if not self.networks_enabled:
            print("INFO: Co-authorship network construction disabled")
            return False
        self.process_networks(db, client, impactu_client)
        return True

    def run(self):
        """
        Execute the plugin to create co-authorship networks and extract top words.
        """

        client = MongoClient(self.mongodb_url)
        db = client[self.database_name]

        impactu_client = MongoClient(self.impactu_database_url)
        impactu_db = impactu_client[self.impactu_database_name]

        openalex_client = MongoClient(self.openalex_database_url)
        openalex_db = openalex_client[self.openalex_database_name]

        abstracts_client = None
        abstracts_db = None
        if self.abstracts_enabled:
            abstracts_client = MongoClient(self.abstracts_database_url)
            abstracts_db = abstracts_client[self.abstracts_database_name]

        print("INFO: Running post-calculations preflight")
        self.preflight(db, impactu_db, openalex_db, abstracts_db)

        if self.geographic_normalization_enabled:
            print("INFO: Normalizing affiliation geography to Spanish")
            geographic_report = normalize_affiliation_addresses(
                db["affiliations"],
                dry_run=self.geographic_normalization_dry_run,
                batch_size=self.geographic_batch_size,
                verbose=self.verbose,
            )
            action = "Would change" if self.geographic_normalization_dry_run else "Changed"
            print(
                f"INFO: {action} {geographic_report['documents_changed']} affiliations "
                f"and {geographic_report['addresses_changed']} addresses; "
                f"unresolved countries={geographic_report['unresolved_countries']}; "
                "unrecognized Colombian states="
                f"{geographic_report['unrecognized_colombian_states']}"
            )
            if self.verbose >= 2 and geographic_report["issue_samples"]:
                print(
                    "WARNING: Geographic normalization unresolved samples: "
                    f"{geographic_report['issue_samples']}"
                )
        else:
            print("INFO: Geographic normalization disabled by workflow")

        if self.openalex_enrichment_enabled:
            print("INFO: Recovering missing OpenAlex abstracts and topics")
            enrichment_report = enrich_works_from_openalex(
                db["works"],
                openalex_db["works"],
                batch_size=self.openalex_enrichment_batch_size,
                dry_run=self.openalex_enrichment_dry_run,
                verbose=self.verbose,
            )
            action = (
                "Would enrich" if self.openalex_enrichment_dry_run else "Enriched"
            )
            print(
                f"INFO: {action} {enrichment_report['works_changed']} works; "
                f"abstracts={enrichment_report['abstracts_recovered']}; "
                f"primary_topics={enrichment_report['primary_topics_recovered']}"
            )
        else:
            print("INFO: OpenAlex work enrichment disabled by workflow")

        if self.persistent_ids_enabled:
            print("INFO: Setting up persistent ids for authors")
            self.process_person_ids(client)
        else:
            print("INFO: Persistent person IDs disabled by workflow")

        if self.author_reconciliation_enabled:
            for collection_name in self.author_reconciliation_collections:
                if collection_name not in db.list_collection_names():
                    print(
                        "WARNING: Author reconciliation collection missing: "
                        f"{collection_name}"
                    )
                    continue
                report = reconcile_collection_authors(
                    db[collection_name],
                    dry_run=self.author_reconciliation_dry_run,
                    batch_size=self.author_reconciliation_batch_size,
                    progress_interval=self.author_reconciliation_progress_interval,
                    skip_affiliation_conflicts=(
                        self.author_reconciliation_skip_affiliation_conflicts
                    ),
                )
                action = (
                    "Would reconcile"
                    if self.author_reconciliation_dry_run
                    else "Reconciled"
                )
                print(
                    f"INFO: {action} {report['authors_resolved']} authors in "
                    f"{report['documents_changed']} {collection_name} documents; "
                    f"ambiguous={report['ambiguous_authors']}; "
                    f"affiliation_conflicts={report['affiliation_conflicts']}"
                )
        else:
            print("INFO: Author reconciliation disabled by workflow")

        if self.types_enabled:
            print("INFO: Setting up impactu types for works")
            self.process_types(db)
        else:
            print("INFO: ImpactU type processing disabled by workflow")
        print(f"INFO: Creating indexes in db {self.database_name} for backend")
        db["works"].create_index("authors.id")
        db["patents"].create_index("authors.id")
        db["events"].create_index("authors.id")
        db["projects"].create_index("authors.id")
        create_indexes(db)

        if self.abstracts_enabled:
            print("INFO: Recovering DOI abstracts from impactu_abstract_resolver")
            abstract_report = enrich_missing_abstracts(
                db["works"],
                abstracts_db[self.abstracts_collection_name],
                abstracts_db[self.abstracts_runs_collection_name],
                dry_run=self.abstracts_dry_run,
                batch_size=self.abstracts_batch_size,
                title_threshold=self.abstracts_title_threshold,
                verbose=self.verbose,
            )
            action = "Would enrich" if self.abstracts_dry_run else "Enriched"
            print(
                f"INFO: {action} {abstract_report['works_changed']} works with "
                "resolver abstracts; "
                f"scanned={abstract_report['works_scanned']}; "
                f"doi_matches={abstract_report['doi_matches']}; "
                f"title_rejections={abstract_report['title_rejections']}; "
                f"run_id={abstract_report['run_id']}"
            )
        else:
            print("INFO: Abstract resolver enrichment disabled by workflow")

        if self.topics_enabled:
            print("INFO: Setting up topics for works")
            topic_cache = load_openalex_topics(openalex_db["topics"])
            print(f"INFO: Cached {len(topic_cache)} OpenAlex topics")
            works_cursor = db["works"].find(
                {
                    "primary_topic": {"$in": [{}, None]},
                    "abstracts.0": {"$exists": True},
                },
                {
                    "titles": 1,
                    "abstracts": 1,
                    "source": 1,
                    "primary_topic": 1,
                    "topics": 1,
                },
            )
            topic_batches = batched(works_cursor, self.topic_batch_size)
            print(
                f"INFO: Topic inference mode: {self.topic_inference_mode}; "
                "endpoint pool: "
                f"{len(self.inference_endpoints)} isolated workers"
            )
            progress = process_topic_batches_pool(
                db["works"], topic_cache, topic_batches,
                self.inference_endpoints,
                self.topic_request_timeout,
                self.topic_request_retries,
                self.topic_progress_interval,
            )
            print(
                "INFO: Topic inference completed: "
                f"{progress['works']} works; {progress['modified']} updated; "
                f"{progress['failed']} failed"
            )
        else:
            print("INFO: Topic inference disabled by workflow")

        if self.person_topics_enabled:
            print("INFO: Aggregating work topics for identified persons")
            person_topic_report = aggregate_person_topics(
                db["person"],
                db["works"],
                impactu_db[self.person_topics_staging_collection],
                impactu_db[self.person_topics_runs_collection],
                dry_run=self.person_topics_dry_run,
                jobs=self.person_topics_jobs,
                batch_size=self.person_topics_batch_size,
                min_works=self.person_topics_min_works,
                limit=self.person_topics_limit,
                singleton_min_score=self.person_topics_singleton_min_score,
                progress_interval=self.person_topics_progress_interval,
                verbose=self.verbose,
            )
            action = (
                "Would update"
                if self.person_topics_dry_run
                else "Updated"
            )
            print(
                f"INFO: {action} "
                f"{person_topic_report['persons_with_topics']} persons with "
                "aggregated topics; "
                f"eligible={person_topic_report['eligible_persons']}; "
                f"without_topics={person_topic_report['persons_without_topics']}; "
                f"modified={person_topic_report['persons_modified']}; "
                f"run_id={person_topic_report['run_id']}"
            )
        else:
            print("INFO: Person topic aggregation disabled by workflow")

        if self.denormalization_enabled:
            print(f"INFO: Denormalizing data in {self.database_name}")
            denormalize(
                db,
                parallel_collections=self.parallel_collections,
                max_parallel_jobs=self.collection_jobs,
            )
        else:
            print("INFO: Denormalization disabled by workflow")

        self.maybe_process_networks(db, client, impactu_client)

        if not self.top_words_enabled:
            print("INFO: Top words disabled by workflow")
            return 0

        if self.top_words_strategy == "works_scan":
            print("INFO: Creating top words with works-scan strategy")
            top_words_from_works_scan(
                db,
                impactu_db,
                self.es_model,
                self.en_model,
                self.stopwords,
                self.top_words_batch_size,
                self.force_recalculate,
            )
            return 0

        print("INFO: Creating top words for institutions")
        affiliations_cursor = list(db["affiliations"].find({}, {"_id": 1}))
        Parallel(
            n_jobs=self.top_words_jobs,
            verbose=10,
            backend=self.top_words_backend,
        )(
            delayed(top_words_process_one)(
                self.config,
                client if self.top_words_backend == "threading" else None,
                impactu_client if self.top_words_backend == "threading" else None,
                aff,
                self.stopwords,
                "affiliations",
                self.top_words_backend,
                self.force_recalculate,
            )
            for aff in affiliations_cursor
        )

        print(
            "INFO: Creating top words for others affiliations such as faculty, "
            "department, group"
        )
        affiliations_cursor = list(db["affiliations"].find(
            {"types.type": {"$in": ["faculty", "department", "group"]}},
            {"_id": 1},
        ))
        Parallel(
            n_jobs=self.top_words_jobs,
            verbose=10,
            backend=self.top_words_backend,
        )(
            delayed(top_words_process_one)(
                self.config,
                client if self.top_words_backend == "threading" else None,
                impactu_client if self.top_words_backend == "threading" else None,
                aff,
                self.stopwords,
                "affiliations_others",
                self.top_words_backend,
                self.force_recalculate,
            )
            for aff in affiliations_cursor
        )

        print("INFO: Creating top words for person")
        authors_cursor = list(db["person"].find({}, {"_id": 1}))

        Parallel(
            n_jobs=self.top_words_jobs,
            verbose=10,
            backend=self.top_words_backend,
        )(
            delayed(top_words_process_one)(
                self.config,
                client if self.top_words_backend == "threading" else None,
                impactu_client if self.top_words_backend == "threading" else None,
                author,
                self.stopwords,
                "person",
                self.top_words_backend,
                self.force_recalculate,
            )
            for author in authors_cursor
        )
