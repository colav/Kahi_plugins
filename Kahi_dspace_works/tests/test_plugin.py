from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import BoundedSemaphore, Lock
from time import sleep
from unittest.mock import MagicMock, patch

import mongomock
import pytest
import os
from uuid import uuid4
from pymongo import MongoClient
from importlib import import_module
from kahi.KahiBase import KahiBase

from kahi_dspace_works.Kahi_dspace_works import (
    DIM_FIELDS,
    Kahi_dspace_works,
    doi_query,
    normalize_repository_url,
    title_query,
)
from kahi_dspace_works.utils import get_doi
import kahi_dspace_works.Kahi_dspace_works as plugin_module
from kahi_dspace_works.process_one import (
    get_units_affiliations,
    process_one,
    process_one_insert,
    process_one_update,
    run_es_operation,
)
from kahi_impactu_utils.Work import ensure_work_doi_indexes


@pytest.fixture
def mongo_db():
    url = os.environ.get("KAHI_TEST_MONGODB_URL")
    if not url:
        pytest.skip("Set KAHI_TEST_MONGODB_URL for isolated MongoDB tests")
    client = MongoClient(url, serverSelectionTimeoutMS=5000)
    name = "kahi_dspace_doi_test_" + uuid4().hex
    try:
        db = client[name]
        ensure_work_doi_indexes(db.works)
        yield db
    finally:
        client.drop_database(name)
        client.close()


@pytest.fixture(autouse=True)
def mock_insert_only_storage(monkeypatch):
    # mongomock cannot evaluate $type in an update pipeline. Exercise the
    # same insert-only semantics here; Mongo.py has isolated real-server tests.
    monkeypatch.setattr(
        import_module("kahi_dspace_works.process_one"), "insert_only_update",
        lambda document: {"$setOnInsert": document},
    )


class FakeCollection:
    def __init__(self, document=None):
        self.document = document

    def find_one(self, *_args, **_kwargs):
        return self.document


class FakeDatabase:
    def __init__(self):
        self.collections = {
            "dspace_demo_records": FakeCollection(),
            "dspace_demo_identity": FakeCollection(
                {"base_url": "https://example.edu/server/oai/request"}
            ),
            "dspace_demo_errors": FakeCollection(),
        }

    def list_collection_names(self):
        return list(self.collections)

    def __getitem__(self, name):
        return self.collections.get(name, FakeCollection())


def test_queries_bind_conditions_to_one_dim_field():
    assert "$elemMatch" in doi_query()["$or"][0][DIM_FIELDS]
    assert "$elemMatch" in title_query()["$or"][0][DIM_FIELDS]


def test_repository_discovery_uses_identity_and_affiliation_map():
    plugin = Kahi_dspace_works.__new__(Kahi_dspace_works)
    plugin.config = {
        "dspace_works": {
            "repository_affiliations": {
                "dspace_demo_records": "https://ror.org/012345678"
            }
        }
    }

    repositories = list(plugin._repositories(FakeDatabase()))

    assert repositories == [
        {
            "collection_name": "dspace_demo_records",
            "institution_id": "https://ror.org/012345678",
            "repository_url": "https://example.edu",
            "repository_name": "",
            "oai_url": "https://example.edu/server/oai/request",
            "ror": "",
        }
    ]


def test_repository_discovery_uses_ror_from_identity():
    plugin = Kahi_dspace_works.__new__(Kahi_dspace_works)
    plugin.config = {"dspace_works": {}}
    source_db = FakeDatabase()
    source_db.collections["dspace_demo_identity"].document["ror"] = (
        "https://ror.org/02yr3f298"
    )

    repositories = list(plugin._repositories(source_db))

    assert repositories[0]["institution_id"] == "https://ror.org/02yr3f298"


def test_explicit_affiliation_overrides_identity_ror():
    plugin = Kahi_dspace_works.__new__(Kahi_dspace_works)
    plugin.config = {
        "dspace_works": {
            "repository_affiliations": {
                "dspace_demo_records": "https://ror.org/explicit"
            }
        }
    }
    source_db = FakeDatabase()
    source_db.collections["dspace_demo_identity"].document["ror"] = (
        "https://ror.org/identity"
    )

    repositories = list(plugin._repositories(source_db))

    assert repositories[0]["institution_id"] == "https://ror.org/explicit"


def test_oai_endpoint_normalization():
    assert normalize_repository_url("https://example.edu/oai/request/") == "https://example.edu"


def test_initialization_creates_name_affiliation_collation_index(monkeypatch):
    client = MagicMock()
    db = MagicMock()
    person = MagicMock()
    works = MagicMock()
    client.__getitem__.return_value = db
    db.__getitem__.side_effect = lambda name: {
        "person": person,
        "works": works,
    }[name]
    monkeypatch.setattr(plugin_module, "MongoClient", lambda *_args, **_kwargs: client)

    Kahi_dspace_works({
        "database_url": "mongodb://localhost:27017",
        "database_name": "kahi",
        "dspace_works": {
            "database_url": "mongodb://localhost:27017",
            "database_name": "dspace",
        },
    })

    person.create_index.assert_any_call(
        [("full_name", 1), ("affiliations.id", 1)],
        name="full_name_affiliation_es",
        collation={"locale": "es", "strength": 1},
    )


def empty_work():
    return deepcopy(object.__new__(KahiBase).empty_work())


def dspace_record(identifier="oai:example.edu:123/456"):
    return {
        "_id": identifier,
        "OAI-PMH": {
            "request": {
                "#text": "https://example.edu/oai/request",
                "@verb": "GetRecord",
                "@metadataPrefix": "dim",
                "@identifier": identifier,
            },
            "GetRecord": {"record": {"metadata": {"dim:dim": {"dim:field": [
                {"@element": "title", "#text": "Trabajo DSpace controlado"},
                {"@element": "type", "#text": "Artículo"},
            ]}}}},
        },
    }


def dspace_doi_record(identifier, doi):
    record = dspace_record(identifier)
    fields = record["OAI-PMH"]["GetRecord"]["record"]["metadata"][
        "dim:dim"]["dim:field"]
    fields.append({
        "@element": "identifier",
        "@qualifier": "doi",
        "#text": doi,
    })
    return record


def test_get_doi_rejects_unresolved_repository_template():
    record = dspace_doi_record(
        "oai:example.edu:123/template", "10.33881/2011-7191.%x")

    assert get_doi(record) is None


def test_doi_insert_does_not_acquire_a_nested_dspace_lock(monkeypatch, mongo_db):
    class NonReentrantGuard:
        def __init__(self):
            self.active = False
            self.entries = 0

        def __enter__(self):
            if self.active:
                raise RuntimeError("nested lock acquisition")
            self.active = True
            self.entries += 1

        def __exit__(self, *_args):
            self.active = False

    db = mongo_db
    guard = NonReentrantGuard()
    process_module = import_module("kahi_dspace_works.process_one")
    monkeypatch.setattr(process_module, "_dspace_lock", lambda _value: guard)
    record = dspace_doi_record(
        "oai:example.edu:123/doi", "10.1000/valid-doi")

    process_one(
        record, None, "https://example.edu", db, db.works,
        empty_work(), None, False,
        {"author_thd": 65, "paper_thd_low": 90, "paper_thd_high": 95},
    )

    assert guard.entries == 1
    assert db.works.count_documents({}) == 1


class FakeES:
    es_index = "works"

    def __init__(self, responses=None):
        self.responses = responses
        self.documents = set()
        self.inserted = []
        self.es = self

    def search_work(self, **_kwargs):
        return deepcopy(self.responses)

    def exists(self, index, id):
        return id in self.documents

    def insert_work(self, _id, work):
        self.documents.add(_id)
        self.inserted.append((_id, deepcopy(work)))


def test_no_doi_retry_is_idempotent_and_reconciles_elasticsearch():
    db = mongomock.MongoClient().db
    es = FakeES()
    record = dspace_record()

    outcomes = [
        process_one(
            record, None, "https://example.edu", db, db.works,
            empty_work(), es, True,
            {"author_thd": 65, "paper_thd_low": 90, "paper_thd_high": 95},
        )
        for _ in range(2)
    ]

    assert outcomes == ["nuevos", "existentes"]
    assert db.works.count_documents({}) == 1
    assert len(es.inserted) == 1
    assert db.works.find_one()["external_ids"][-1]["id"] == record["_id"]


def test_concurrent_doi_records_preserve_every_dspace_identifier():
    db = mongomock.MongoClient().db
    doi = "https://doi.org/10.1000/shared"
    work = empty_work()
    work.update({
        "titles": [{
            "title": "Trabajo DSpace controlado",
            "lang": "es",
            "source": "openalex",
        }],
        "updated": [{"source": "openalex", "time": 1}],
        "external_ids": [{"source": "doi", "id": doi}],
    })
    db.works.insert_one(work)
    records = [
        dspace_doi_record(f"oai:example.edu:123/{index}", doi)
        for index in range(20)
    ]

    def load(record):
        process_one(
            record, None, "https://example.edu", db, db.works,
            empty_work(), None, False,
            {"author_thd": 65, "paper_thd_low": 90, "paper_thd_high": 95},
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(load, records))

    saved = db.works.find_one({"external_ids.id": doi})
    dspace_ids = {
        item["id"] for item in saved["external_ids"]
        if item.get("source") == "dspace"
    }
    assert dspace_ids == {record["_id"] for record in records}


def test_stale_elasticsearch_hit_does_not_drop_the_dspace_record():
    db = mongomock.MongoClient().db
    es = FakeES(responses=[{
        "_id": "not-a-mongo-object-id",
        "_source": {"title": "Trabajo DSpace controlado", "authors": []},
    }])

    process_one(
        dspace_record(), None, "https://example.edu", db, db.works,
        empty_work(), es, True,
        {"author_thd": 65, "paper_thd_low": 90, "paper_thd_high": 95},
    )

    assert db.works.count_documents({}) == 1


def test_es_semaphore_limits_concurrency():
    state = {"active": 0, "maximum": 0}
    state_lock = Lock()

    def operation():
        with state_lock:
            state["active"] += 1
            state["maximum"] = max(state["maximum"], state["active"])
        sleep(0.01)
        with state_lock:
            state["active"] -= 1

    semaphore = BoundedSemaphore(2)
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(run_es_operation, semaphore, operation)
            for _ in range(16)
        ]
        for future in futures:
            future.result()

    assert state["maximum"] == 2


def test_repository_cursor_is_streamed_and_closed():
    class Cursor:
        def __init__(self):
            self.iterations = 0
            self.closed = False
            self.batch = None

        def batch_size(self, value):
            self.batch = value
            return self

        def __iter__(self):
            self.iterations += 1
            yield dspace_record()

        def close(self):
            self.closed = True

    cursor = Cursor()
    collection = MagicMock()
    collection.name = "dspace_test_records"
    collection.find.return_value = cursor
    collection.count_documents.return_value = 1
    plugin = Kahi_dspace_works.__new__(Kahi_dspace_works)
    plugin.task = None
    plugin.batch_size = 500
    plugin.n_jobs = 1
    plugin.verbose = 0
    plugin.db = MagicMock()
    plugin.collection = MagicMock()
    plugin.es_handler = None
    plugin.es_semaphore = None
    plugin.thresholds = {"author_thd": 65, "paper_thd_low": 90, "paper_thd_high": 95}
    plugin.empty_work = empty_work

    class RecordingParallel:
        def __init__(self, **_kwargs):
            pass

        def __call__(self, tasks):
            assert cursor.iterations == 0
            list(tasks)
            return []

    with patch.object(plugin_module, "Parallel", RecordingParallel):
        plugin.process_repository(None, "https://example.edu", collection)

    assert cursor.batch == 500
    assert cursor.iterations == 1
    assert cursor.closed
    assert (
        collection.count_documents.call_args.args[0]
        == collection.find.call_args.args[0]
    )


def seed_internal_units(db):
    parent = {
        "id": "02yr3f298",
        "name": "FUCS",
        "types": [{"source": "ror", "type": "education"}],
    }
    faculty = {
        "id": "02yr3f298_U05",
        "name": "Facultad de Enfermería",
        "types": [{"source": "staff", "type": "faculty"}],
        "years": [2019, 2020, 2021],
    }
    department = {
        "id": "02yr3f298_U05_U01",
        "name": "Enfermería",
        "types": [{"source": "staff", "type": "department"}],
        "years": [2020, 2021],
    }
    group = {
        "id": "COL0001",
        "name": "Grupo de Enfermería",
        "types": [{"source": "scienti", "type": "group"}],
    }
    unrelated = {
        "id": "other_U01",
        "name": "Otra facultad",
        "types": [{"source": "staff", "type": "faculty"}],
        "years": [2019, 2020, 2021],
    }
    db.affiliations.insert_many([
        {"_id": parent["id"], "types": parent["types"]},
        {"_id": faculty["id"], "relations": [{"id": parent["id"]}]},
        {"_id": department["id"], "relations": [{"id": parent["id"]}]},
        {"_id": group["id"], "relations": [{"id": parent["id"]}]},
        {"_id": unrelated["id"], "relations": [{"id": "other"}]},
    ])
    person = {
        "_id": "person-1",
        "full_name": "adriana panader torres",
        "first_names": ["adriana"],
        "last_names": ["panader", "torres"],
        "initials": "apt",
        "updated": [{"source": "staff"}],
        "affiliations": [parent, faculty, department, group, unrelated],
    }
    db.person.insert_one(deepcopy(person))
    return parent, faculty, department, group, unrelated, person


def test_get_units_affiliations_returns_faculty_and_department_only():
    db = mongomock.MongoClient().db
    parent, faculty, department, _group, _unrelated, person = seed_internal_units(db)

    units = get_units_affiliations(db, person, [parent], 2021)

    assert [unit["id"] for unit in units] == [faculty["id"], department["id"]]


def test_get_units_affiliations_requires_parent_in_product():
    db = mongomock.MongoClient().db
    _parent, _faculty, _department, _group, _unrelated, person = seed_internal_units(db)

    assert get_units_affiliations(db, person, [], 2021) == []


def test_get_units_affiliations_requires_parent_in_person():
    db = mongomock.MongoClient().db
    parent, _faculty, _department, _group, _unrelated, person = seed_internal_units(db)
    person["affiliations"] = person["affiliations"][1:]
    db.person.replace_one({"_id": person["_id"]}, person)

    assert get_units_affiliations(db, person, [parent], 2021) == []


def test_get_units_affiliations_resolves_parent_external_id():
    db = mongomock.MongoClient().db
    parent, faculty, department, _group, _unrelated, person = seed_internal_units(db)
    db.affiliations.update_one(
        {"_id": parent["id"]},
        {"$set": {"external_ids": [{"id": "https://ror.org/02yr3f298"}]}})

    units = get_units_affiliations(db, person, [{
        "external_ids": [{"id": "https://ror.org/02yr3f298"}]
    }], 2021)

    assert [unit["id"] for unit in units] == [faculty["id"], department["id"]]


def test_get_units_affiliations_filters_by_publication_year():
    db = mongomock.MongoClient().db
    parent, faculty, department, _group, _unrelated, person = seed_internal_units(db)

    # 2019 is covered by the faculty only; the department starts in 2020
    assert [u["id"] for u in get_units_affiliations(db, person, [parent], 2019)] == [faculty["id"]]
    # 2022 is accepted through the one-year tolerance
    assert [u["id"] for u in get_units_affiliations(db, person, [parent], 2022)] == [
        faculty["id"], department["id"]]
    assert get_units_affiliations(db, person, [parent], 2017) == []
    assert get_units_affiliations(db, person, [parent], None) == []


def test_dspace_insert_propagates_units_without_duplicates():
    db = mongomock.MongoClient().db
    parent, faculty, department, _group, _unrelated, _person = seed_internal_units(db)
    entry = empty_work()
    entry.update({
        "titles": [{"title": "Trabajo de enfermería", "lang": "es"}],
        "external_ids": [{"source": "dspace", "id": "oai:test:1"}],
        "year_published": 2021,
        "authors": [{
            "id": "",
            "full_name": "Adriana Panader Torres",
            "first_names": ["Adriana"],
            "last_names": ["Panader", "Torres"],
            "initials": "APT",
            "affiliations": [deepcopy(parent)],
        }],
    })

    process_one_insert(entry, parent, db, db.works, None, 0)

    author = db.works.find_one()["authors"][0]
    assert author["id"] == "person-1"
    assert [item["id"] for item in author["affiliations"]] == [
        parent["id"], faculty["id"], department["id"]]


def test_dspace_update_propagates_units_to_matched_author():
    db = mongomock.MongoClient().db
    parent, faculty, department, _group, _unrelated, _person = seed_internal_units(db)
    existing = empty_work()
    existing.update({
        "_id": "work-1",
        "year_published": 2021,
        "authors": [{
            "id": "person-1",
            "full_name": "adriana panader torres",
            "affiliations": [deepcopy(parent)],
        }],
        "author_count": 1,
    })
    db.works.insert_one(deepcopy(existing))
    incoming = empty_work()
    incoming["authors"] = [{
        "full_name": "Adriana Panader Torres",
        "type": "author",
        "affiliations": [],
    }]

    with patch("kahi_dspace_works.process_one.compare_author", return_value=True):
        process_one_update(incoming, existing, parent, db, db.works, 0)

    author = db.works.find_one({"_id": "work-1"})["authors"][0]
    assert [item["id"] for item in author["affiliations"]] == [
        parent["id"], faculty["id"], department["id"]]


def test_dspace_update_without_repository_affiliation_does_not_crash():
    db = mongomock.MongoClient().db
    parent, faculty, department, _group, _unrelated, _person = seed_internal_units(db)
    existing = empty_work()
    existing.update({
        "_id": "work-1",
        "year_published": 2021,
        "authors": [{
            "id": "person-1",
            "full_name": "adriana panader torres",
            "affiliations": [deepcopy(parent)],
        }],
        "author_count": 1,
    })
    db.works.insert_one(deepcopy(existing))
    incoming = empty_work()
    incoming["authors"] = [{
        "full_name": "Adriana Panader Torres",
        "type": "author",
        "affiliations": [],
    }]

    with patch("kahi_dspace_works.process_one.compare_author", return_value=True):
        process_one_update(incoming, existing, None, db, db.works, 0)

    author = db.works.find_one({"_id": "work-1"})["authors"][0]
    assert [item["id"] for item in author["affiliations"]] == [
        parent["id"], faculty["id"], department["id"]]


# --- repository sources and source_ref -------------------------------------

from kahi_dspace_works.parser import parse_dspace, repository_source_ref  # noqa: E402
from kahi_dspace_works.utils import ensure_repository_source  # noqa: E402


def udea_repository(source_id=None):
    return {
        "key": "dspace_udea",
        "name": "Repositorio Institucional de la Universidad de Antioquia",
        "url": "https://bibliotecadigital.udea.edu.co",
        "oai_url": "https://bibliotecadigital.udea.edu.co/server/oai/request",
        "ror": "https://ror.org/03bp5hc83",
        "affiliation": {"id": "03bp5hc83", "name": "Universidad de Antioquia"},
        "source_id": source_id,
    }


UDEA_AFF = {"id": "03bp5hc83", "name": "Universidad de Antioquia",
            "types": [{"source": "ror", "type": "education"}]}


def test_ensure_repository_source_creates_document():
    db = mongomock.MongoClient().db
    source_id = ensure_repository_source(db, udea_repository(), UDEA_AFF, KahiBase.empty_source(None))
    doc = db.sources.find_one({"_id": source_id})
    assert doc["names"] == [{"name": udea_repository()["name"], "lang": "es", "source": "dspace"}]
    assert doc["types"] == [{"source": "dspace", "type": "repository"}]
    assert {"source": "dspace", "id": "dspace_udea"} in doc["external_ids"]
    assert {"source": "ror", "id": "https://ror.org/03bp5hc83"} in doc["external_ids"]
    assert [u["source"] for u in doc["external_urls"]] == ["repository", "oaipmh"]
    assert doc["publisher"]["id"] == "03bp5hc83" and doc["relations"][0]["id"] == "03bp5hc83"
    assert doc["products_count"] == 0
    # idempotent
    assert ensure_repository_source(db, udea_repository(), UDEA_AFF, KahiBase.empty_source(None)) == source_id
    assert db.sources.count_documents({}) == 1


def test_ensure_repository_source_reuses_openalex_document():
    db = mongomock.MongoClient().db
    oa = KahiBase.empty_source(None)
    oa.update({
        "names": [{"name": "Institutional Repository University of Antioquia", "lang": "en",
                   "source": "openalex"}],
        "types": [{"type": "repository"}],
        "external_ids": [{"source": "openalex", "id": "https://openalex.org/S4377196334"}],
        "updated": [{"source": "openalex", "time": 1}],
    })
    oa_id = db.sources.insert_one(oa).inserted_id

    source_id = ensure_repository_source(
        db, udea_repository(), UDEA_AFF, KahiBase.empty_source(None),
        openalex_id="https://openalex.org/S4377196334")

    assert source_id == oa_id
    doc = db.sources.find_one({"_id": oa_id})
    assert [n["source"] for n in doc["names"]] == ["openalex", "dspace"]
    assert {"source": "dspace", "id": "dspace_udea"} in doc["external_ids"]
    assert {"source": "openalex", "id": "https://openalex.org/S4377196334"} in doc["external_ids"]
    assert {"source": "dspace", "type": "repository"} in doc["types"]
    assert [u["source"] for u in doc["updated"]] == ["openalex", "dspace"]
    assert doc["relations"][0]["id"] == "03bp5hc83"
    assert db.sources.count_documents({}) == 1
    # second run finds it by the dspace id
    assert ensure_repository_source(db, udea_repository(), UDEA_AFF, KahiBase.empty_source(None)) == oa_id


def test_ensure_repository_source_without_affiliation_or_openalex_match():
    db = mongomock.MongoClient().db
    repo = udea_repository()
    repo["affiliation"] = {}
    repo["ror"] = ""
    source_id = ensure_repository_source(
        db, repo, None, KahiBase.empty_source(None), openalex_id="https://openalex.org/S0")
    doc = db.sources.find_one({"_id": source_id})
    assert doc["publisher"] == "" and doc["relations"] == []
    assert doc["external_ids"] == [{"source": "dspace", "id": "dspace_udea"}]


def test_repository_source_ref_shape():
    ref = repository_source_ref(udea_repository(source_id="sid"))
    assert ref == {
        "id": "sid", "provenance": "dspace",
        "name": "Repositorio Institucional de la Universidad de Antioquia",
        "types": [{"source": "dspace", "type": "repository"}],
        "url": "https://bibliotecadigital.udea.edu.co",
        "affiliation": {"id": "03bp5hc83", "name": "Universidad de Antioquia"},
    }
    assert repository_source_ref(None) is None


def dim_record():
    return {
        "_id": "oai:bibliotecadigital.udea.edu.co:10495/6956",
        "OAI-PMH": {"GetRecord": {"record": {"metadata": {"dim:dim": {"dim:field": [
            {"@element": "title", "#text": "Creatividad y cerebro"},
            {"@element": "identifier", "@qualifier": "uri", "#text": "http://hdl.handle.net/10495/6956"},
        ]}}}}},
    }


def test_parse_dspace_handle_entry_has_source_ref():
    entry = parse_dspace(dim_record(), empty_work(), "https://bibliotecadigital.udea.edu.co", 0,
                         repository=udea_repository(source_id="sid"))
    handle = [u for u in entry["external_urls"] if u["source"] == "handle"]
    assert len(handle) == 1
    assert handle[0]["provenance"] == "dspace"
    assert handle[0]["source_ref"]["id"] == "sid"
    assert handle[0]["source_ref"]["name"].startswith("Repositorio Institucional")
    assert not any(u["source"] == "dspace" for u in entry["external_urls"])


def test_parse_dspace_without_repository_keeps_plain_handle_entry():
    entry = parse_dspace(dim_record(), empty_work(), "https://bibliotecadigital.udea.edu.co", 0)
    handle = [u for u in entry["external_urls"] if u["source"] == "handle"]
    assert handle and "source_ref" not in handle[0]
