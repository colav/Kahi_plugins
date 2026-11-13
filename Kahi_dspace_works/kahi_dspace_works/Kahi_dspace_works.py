import json
from urllib.parse import urlsplit
from joblib import Parallel, delayed
from kahi.KahiBase import KahiBase
from kahi.progress import ProgressLogger
from mohan.Similarity import Similarity
from pymongo import MongoClient
from threading import BoundedSemaphore
from elasticsearch import Elasticsearch
from kahi_impactu_utils.Work import ensure_work_doi_indexes

from kahi_dspace_works.process_one import process_one
from kahi_dspace_works.utils import (
    ensure_repository_source,
    get_doi,
    is_similarity_reg,
    is_thesis_work,
    process_affiliation,
)


DIM_FIELDS = "OAI-PMH.GetRecord.record.metadata.dim:dim.dim:field"


def normalize_repository_url(url):
    """Convert a DSpace OAI endpoint into the repository public base URL."""
    if not url:
        return ""
    url = url.rstrip("/")
    for suffix in (
        "/server/oai/request",
        "/oai/request",
        "/oai-pmh/request",
        "/oai",
    ):
        if url.lower().endswith(suffix):
            return url[: -len(suffix)]
    return url


def doi_query():
    """Return records containing a non-empty DOI field."""
    return _dim_query(
        {
            "@element": "identifier",
            "@qualifier": "doi",
            "#text": {"$exists": True, "$nin": [None, ""]},
        }
    )


def title_query():
    """Return records containing a non-empty title field."""
    return _dim_query(
        {
            "@element": "title",
            "#text": {"$exists": True, "$nin": [None, ""]},
        }
    )


def counted_records(cursor, accept, progress, foreign_hosts=()):
    """Yield accepted records; count the rest as skipped and the records
    harvested from another configured repository as foreign."""
    for work in cursor:
        if foreign_hosts and request_host(work) in foreign_hosts:
            progress.step("foraneos")
        elif accept(work):
            yield work
        else:
            progress.step("omitidos")


def url_host(url):
    """Host of a URL in lower case ('' when absent)."""
    try:
        return (urlsplit(str(url or "")).hostname or "").lower()
    except ValueError:
        return ""


def request_host(work):
    """Host of the OAI server that answered the record (OAI-PMH.request)."""
    request = (work.get("OAI-PMH") or {}).get("request") or {}
    return url_host(request.get("#text") if isinstance(request, dict) else request)


def _dim_query(condition):
    """Match one DIM field whether xmltodict produced a list or a singleton."""
    singleton = [
        {"$expr": {"$eq": [{"$type": f"${DIM_FIELDS}"}, "object"]}}
    ]
    singleton.extend(
        {f"{DIM_FIELDS}.{key}": value} for key, value in condition.items()
    )
    return {
        "$or": [
            {DIM_FIELDS: {"$elemMatch": condition}},
            {"$and": singleton},
        ]
    }


class Kahi_dspace_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config
        plugin_config = config["dspace_works"]

        self.mongodb_url = config["database_url"]
        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        self.task = plugin_config.get("task")
        self.n_jobs = plugin_config.get("num_jobs", 1)
        self.verbose = plugin_config.get("verbose", 0)
        self.batch_size = plugin_config.get("batch_size", 500)
        self.es_max_concurrency = plugin_config.get("es_max_concurrency", 8)
        if self.es_max_concurrency < 1:
            raise ValueError(
                "dspace_works.es_max_concurrency must be greater than zero"
            )

        thresholds = plugin_config.get("thresholds")
        if thresholds and len(thresholds) == 3:
            self.thresholds = {
                "author_thd": thresholds[0],
                "paper_thd_low": thresholds[1],
                "paper_thd_high": thresholds[2],
            }
        else:
            self.thresholds = {
                "author_thd": 65,
                "paper_thd_low": 90,
                "paper_thd_high": 95,
            }

        es_keys = ("es_index", "es_url", "es_user", "es_password")
        if all(key in plugin_config for key in es_keys):
            es_auth = None
            if plugin_config["es_user"] and plugin_config["es_password"]:
                es_auth = (
                    plugin_config["es_user"],
                    plugin_config["es_password"])
            self.es_handler = Similarity(
                plugin_config["es_index"],
                es_uri=plugin_config["es_url"],
                es_auth=es_auth,
            )
            # Similarity creates an Elasticsearch client with a pool of 10
            # connections. Match the pool to the configured semaphore so
            # workers never fail with EmptyPoolError under higher concurrency.
            previous_client = self.es_handler.es
            self.es_handler.es = Elasticsearch(
                plugin_config["es_url"],
                basic_auth=es_auth,
                request_timeout=120,
                retry_on_timeout=True,
                max_retries=5,
                connections_per_node=max(10, self.es_max_concurrency),
            )
            previous_client.close()
            print("INFO: ES handler created successfully")
        else:
            self.es_handler = None
            print("WARNING: No elasticsearch configuration provided")

        self.es_semaphore = None
        if self.es_handler is not None:
            self.es_semaphore = BoundedSemaphore(self.es_max_concurrency)

        print("INFO: Creating index for full_name in person collection")
        self.db["person"].create_index("full_name")
        self.db["person"].create_index(
            [("full_name", 1), ("affiliations.id", 1)],
            name="full_name_affiliation_es",
            collation={"locale": "es", "strength": 1},
        )
        self.collection.create_index("external_ids.id")
        ensure_work_doi_indexes(self.collection)

    def _repositories(self, source_db):
        """Resolve explicit repositories or discover every ``*_records`` collection."""
        plugin_config = self.config["dspace_works"]
        explicit = plugin_config.get("repositories")
        if explicit:
            for repository in explicit:
                yield repository
            return

        affiliations = plugin_config.get("repository_affiliations", {})
        include = set(plugin_config.get("collections", []))
        exclude = set(plugin_config.get("exclude_collections", []))
        collection_names = sorted(
            name
            for name in source_db.list_collection_names()
            if name.startswith("dspace_")
            and name.endswith("_records")
            and (not include or name in include)
            and name not in exclude
        )
        for collection_name in collection_names:
            identity_name = collection_name[: -len("records")] + "identity"
            identity = source_db[identity_name].find_one(
                {}, {"base_url": 1, "ror": 1, "repository_name": 1, "openalex_source": 1}) or {}
            institution_id = (
                affiliations[collection_name]
                if collection_name in affiliations
                else identity.get("ror")
            )
            yield {
                "collection_name": collection_name,
                "institution_id": institution_id,
                "repository_url": normalize_repository_url(identity.get("base_url")),
                "repository_name": identity.get("repository_name") or "",
                "oai_url": identity.get("base_url") or "",
                "ror": identity.get("ror") or "",
                "openalex_source": identity.get("openalex_source") or "",
            }

    def repository_metadata(self, repository, affiliation, base_url):
        """Register the repository in sources and return the metadata that
        describes it in works.external_urls[].source_ref."""
        plugin_config = self.config["dspace_works"]
        key = repository["collection_name"]
        if key.endswith("_records"):
            key = key[: -len("_records")]
        name = repository.get("repository_name") or key
        metadata = {
            "key": key,
            "name": name,
            "url": base_url or "",
            "oai_url": repository.get("oai_url") or "",
            "ror": repository.get("ror") or "",
            "affiliation": ({"id": affiliation["id"], "name": affiliation["name"]}
                            if affiliation else {}),
        }
        # The OpenAlex source of the repository lives in its identity document
        # (persisted by the capture DAG); `repository_sources` only overrides it.
        openalex_id = ((plugin_config.get("repository_sources") or {}).get(key)
                       or repository.get("openalex_source") or None)
        metadata["source_id"] = ensure_repository_source(
            self.db, metadata, affiliation, self.empty_source(), openalex_id=openalex_id)
        return metadata

    def process_repository(self, affiliation, base_url, dspace_collection,
                           repository=None, foreign_hosts=()):
        query = doi_query() if self.task == "doi" else title_query()
        progress = ProgressLogger(
            "dspace_works:{}:{}".format(
                "doi" if self.task == "doi" else "nodoi", dspace_collection.name),
            total=dspace_collection.count_documents(query),
        ).start(f"num_jobs={self.n_jobs}")
        if self.task == "doi":
            cursor = dspace_collection.find(
                query).batch_size(
                self.batch_size)
            records = counted_records(
                cursor,
                lambda work: get_doi(work) and not is_thesis_work(work),
                progress,
                foreign_hosts,
            )
            similarity = False
        else:
            cursor = dspace_collection.find(
                query).batch_size(
                self.batch_size)
            records = counted_records(cursor, is_similarity_reg, progress, foreign_hosts)
            similarity = True

        try:
            results = Parallel(
                n_jobs=self.n_jobs,
                verbose=0,
                backend="threading",
                pre_dispatch="2*n_jobs",
                return_as="generator_unordered",
            )(
                delayed(process_one)(
                    dspace_reg=work,
                    affiliation=affiliation,
                    base_url=base_url,
                    db=self.db,
                    collection=self.collection,
                    empty_work=self.empty_work(),
                    es_handler=self.es_handler,
                    similarity=similarity,
                    thresholds=self.thresholds,
                    verbose=self.verbose,
                    es_semaphore=self.es_semaphore,
                    repository=repository,
                )
                for work in records
            )
            for outcome in results:
                progress.step(outcome or "omitidos")
        finally:
            cursor.close()
        progress.done()

    def run(self):
        print(
            f"INFO: Running dspace works with num_jobs = {self.n_jobs} "
            f"task = {self.task}"
        )
        plugin_config = self.config["dspace_works"]
        mapping_file = plugin_config.get("repository_sources_file")
        if mapping_file:
            with open(mapping_file, encoding="utf-8") as handle:
                mapping = json.load(handle)
            plugin_config["repository_sources"] = {
                **mapping, **(plugin_config.get("repository_sources") or {})}
        source_client = MongoClient(plugin_config["database_url"])
        try:
            source_db = source_client[plugin_config["database_name"]]
            repositories = list(self._repositories(source_db))
            if not repositories:
                raise ValueError("No DSpace *_records collections were found")

            hosts = {
                repository["collection_name"]: url_host(repository.get("oai_url"))
                for repository in repositories
            }
            for repository in repositories:
                collection_name = repository["collection_name"]
                institution_id = repository.get("institution_id")
                # Records answered by the OAI server of another configured
                # repository are copies (e.g. dspace_uac_records holds the
                # whole uniatlantico repository); that repository loads them.
                foreign_hosts = {
                    host for name, host in hosts.items()
                    if host and name != collection_name and host != hosts[collection_name]
                }
                base_url = normalize_repository_url(
                    repository.get("repository_url"))
                print(
                    f"INFO: Processing collection {collection_name} "
                    f"institution {institution_id or 'unmapped'}"
                )
                affiliation = (
                    process_affiliation(institution_id, self.db)
                    if institution_id
                    else None
                )
                if institution_id and affiliation is None and self.verbose:
                    print(
                        "WARNING: Affiliation not found for institution "
                        f"{institution_id} - processing without affiliation"
                    )
                metadata = self.repository_metadata(repository, affiliation, base_url)
                self.process_repository(
                    affiliation, base_url, source_db[collection_name],
                    repository=metadata, foreign_hosts=foreign_hosts,
                )
        finally:
            source_client.close()

        return 0
