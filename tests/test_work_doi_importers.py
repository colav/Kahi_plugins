"""DOI identity regressions using real importers and isolated MongoDB databases."""
import importlib
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from copy import deepcopy
from uuid import uuid4

import pytest
from pymongo import MongoClient

from kahi.KahiBase import KahiBase
import kahi_impactu_utils.Work as identity


PLUGINS = ("ciarp", "scholar", "dspace")
DOI = "https://doi.org/10.1000/controlled"
TITLE = "Controlled publication title"
AFFILIATION = {
    "_id": "test-institution", "types": [],
    "names": [{"name": "Test University", "lang": "en", "source": "ror"}],
}


@pytest.fixture
def db():
    url = os.environ.get("KAHI_TEST_MONGODB_URL")
    if not url:
        pytest.skip("Set KAHI_TEST_MONGODB_URL for isolated MongoDB tests")
    client = MongoClient(url, serverSelectionTimeoutMS=5000)
    name = "kahi_importer_doi_test_" + uuid4().hex
    try:
        database = client[name]
        identity.ensure_work_doi_indexes(database.works)
        yield database
    finally:
        client.drop_database(name)
        client.close()


def empty_work():
    return deepcopy(KahiBase.empty_work(None))


def source_identifier(plugin, index):
    source = {"ciarp": "ciarp_record", "scholar": "scholar", "dspace": "dspace"}[plugin]
    identifier = f"oai:example.edu:123/{index}" if plugin == "dspace" else f"{plugin}:{index}"
    return source, identifier


def make_record(plugin, index=0, doi=DOI, title=TITLE, thesis=False):
    _, identifier = source_identifier(plugin, index)
    if plugin == "ciarp":
        return {
            "título": title, "idioma": "en", "doi": doi, "issn": "",
            "isbn": "", "revista": "", "año": "2024", "volumen": "",
            "issue": "", "primera_página": "", "última_página": "",
            "ranking": "Artículo", "identificación": "unknown-person",
            "código_unidad_académica": "", "código_subunidad_académica": "",
            "index": f"timestamp:{index}", "source_record_id": identifier,
        }
    if plugin == "scholar":
        return {"doi": doi, "cid": identifier, "title": title, "year": "2024"}
    fields = [
        {"@element": "title", "#text": title},
        {"@element": "type", "#text": "Tesis" if thesis else "Artículo"},
    ]
    if doi:
        fields.append({"@element": "identifier", "@qualifier": "doi", "#text": doi})
    return {"_id": identifier, "OAI-PMH": {
        "request": {"#text": "https://example.edu/oai/request", "@verb": "GetRecord",
                    "@metadataPrefix": "dim", "@identifier": identifier},
        "GetRecord": {"record": {"metadata": {"dim:dim": {"dim:field": fields}}}},
    }}


def invoke(plugin, record, db, *, similarity=False, insert=False):
    module = importlib.import_module(f"kahi_{plugin}_works.process_one")
    if plugin == "ciarp":
        if insert:
            return module.process_one_insert(record, db, db.works, deepcopy(AFFILIATION), empty_work(), None)
        return module.process_one(record, db, db.works, deepcopy(AFFILIATION), empty_work(), similarity, None)
    if plugin == "scholar":
        if insert:
            return module.process_one_insert(record, db, db.works, empty_work(), None)
        return module.process_one(record, db, db.works, empty_work(), similarity, None)
    if insert:
        entry = module.parse_dspace(record, empty_work(), "https://example.edu", 0)
        module.set_source(entry, db)
        return module.process_one_insert(entry, None, db, db.works, None, 0)
    return module.process_one(record, None, "https://example.edu", db, db.works, empty_work(), None, similarity, {})


def seed_work(db, *, doi=DOI, external_ids=None, title=TITLE):
    work = empty_work()
    work.update(doi=doi, titles=[{"title": title, "lang": "en", "source": "openalex"}],
                external_ids=external_ids or [], year_published=2024)
    return db.works.insert_one(work).inserted_id


@pytest.mark.parametrize("plugin", PLUGINS)
@pytest.mark.parametrize("legacy_location", ["doi", "external_ids"])
def test_real_importer_reuses_legacy_doi_and_repairs_identifiers(db, plugin, legacy_location):
    legacy = "http://dx.doi.org/10.1000/CONTROLLED"
    ids = [{"source": "openalex", "id": "original"}]
    if legacy_location == "external_ids":
        ids.append({"source": "doi", "id": legacy})
    work_id = seed_work(db, doi=legacy if legacy_location == "doi" else "", external_ids=ids)
    invoke(plugin, make_record(plugin, doi="10.1000/CONTROLLED"), db)
    assert db.works.count_documents({}) == 1
    work = db.works.find_one()
    assert work["_id"] == work_id and "_doi_key" not in work
    assert any(ext["source"] == "doi" and ext["id"] == DOI for ext in work["external_ids"])
    source, identifier = source_identifier(plugin, 0)
    assert any(ext["source"] == source and ext["id"] == identifier for ext in work["external_ids"])
    assert {"source": "openalex", "id": "original"} in work["external_ids"]


@pytest.mark.parametrize("plugin", PLUGINS)
def test_two_source_records_with_same_doi_keep_both_ids_on_retry(db, plugin):
    for index in (0, 1, 1):
        invoke(plugin, make_record(plugin, index), db)
    assert db.works.count_documents({}) == 1
    work = db.works.find_one()
    assert "_doi_key" not in work
    source, _ = source_identifier(plugin, 0)
    ids = [ext["id"] for ext in work["external_ids"] if ext["source"] == source]
    assert set(ids) == {source_identifier(plugin, index)[1] for index in (0, 1)} and len(ids) == 2


@pytest.mark.parametrize("plugin", PLUGINS)
def test_existing_source_record_acquires_new_doi_without_split(db, plugin):
    source, identifier = source_identifier(plugin, 0)
    work_id = seed_work(db, doi="", external_ids=[{"source": source, "id": identifier}])
    invoke(plugin, make_record(plugin), db)
    assert db.works.count_documents({}) == 1
    work = db.works.find_one()
    assert work["_id"] == work_id and work["doi"] == DOI and "_doi_key" not in work


@pytest.mark.parametrize("plugin", PLUGINS)
def test_historical_doi_matches_select_an_existing_work_without_stopping(db, plugin):
    seed_work(db)
    seed_work(db, doi="", external_ids=[{"source": "doi", "id": "10.1000/controlled"}])
    invoke(plugin, make_record(plugin), db)
    assert db.works.count_documents({}) == 2
    source, identifier = source_identifier(plugin, 0)
    assert db.works.count_documents({"external_ids": {"$elemMatch": {"source": source, "id": identifier}}}) == 1


@pytest.mark.parametrize("plugin", PLUGINS)
@pytest.mark.parametrize("insert", [False, True])
def test_source_and_doi_identifying_different_documents_stops(db, plugin, insert):
    source, identifier = source_identifier(plugin, 0)
    seed_work(db, doi="", external_ids=[{"source": source, "id": identifier}])
    seed_work(db)
    before = list(db.works.find())
    invoke(plugin, make_record(plugin), db, insert=insert)
    assert list(db.works.find()) == before


@pytest.mark.parametrize("similarity", [False, True])
def test_dspace_distinct_titles_keep_same_doi_and_retry_is_idempotent(db, similarity):
    first = seed_work(db, title="Marine biodiversity assessment in Pacific reefs")
    for _ in range(2):
        invoke("dspace", make_record("dspace"), db, similarity=similarity)
    assert db.works.count_documents({"doi": DOI}) == 2
    assert db.works.find_one({"_id": first})["titles"][0]["title"] == "Marine biodiversity assessment in Pacific reefs"
    assert db.works.count_documents({"_doi_key": {"$exists": True}}) == 0


def test_dspace_thesis_checks_doi_before_similarity(db):
    work_id = seed_work(db)
    invoke("dspace", make_record("dspace", thesis=True), db, similarity=True)
    assert db.works.count_documents({}) == 1
    assert db.works.find_one()["_id"] == work_id


def test_dspace_unresolved_doi_template_never_becomes_unique_identity(db):
    placeholder = "10.33881/2011-7191.%x"
    invoke("dspace", make_record("dspace", doi=placeholder), db)
    assert db.works.count_documents({}) == 0
    for index in (0, 1):
        invoke("dspace", make_record("dspace", index, doi=placeholder), db, similarity=True)
    assert db.works.count_documents({}) == 2
    assert db.works.count_documents({"_doi_key": {"$exists": True}}) == 0


@pytest.mark.parametrize("plugin", PLUGINS)
def test_concurrent_same_source_updates_preserve_every_identifier(db, plugin):
    seed_work(db)
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: invoke(plugin, make_record(plugin, i), db), range(20)))
    assert db.works.count_documents({}) == 1
    work = db.works.find_one()
    source, _ = source_identifier(plugin, 0)
    assert {ext["id"] for ext in work["external_ids"] if ext["source"] == source} == {
        source_identifier(plugin, index)[1] for index in range(20)}


def _initialize_race(barrier):
    global _race_barrier
    _race_barrier = barrier


def _import_in_separate_process(url, database_name, index, only_dspace=False):
    # Force every process beyond an empty DOI lookup before any can insert.
    # This exercises the unique MongoDB key rather than the local locks.
    plugin = "dspace" if only_dspace else PLUGINS[index % len(PLUGINS)]
    # Bind each plugin's preliminary lookup before patching the lookup inside
    # insert_work_by_doi; the barrier must guard the final pre-upsert check.
    importlib.import_module(f"kahi_{plugin}_works.process_one")
    _race_barrier.wait(timeout=45)
    client = MongoClient(url, serverSelectionTimeoutMS=5000)
    try:
        title = ("Marine biodiversity assessment in Pacific reefs"
                 if only_dspace and index % 2 else TITLE)
        invoke(plugin, make_record(plugin, index, title=title), client[database_name])
        return os.getpid(), source_identifier(plugin, index)
    finally:
        client.close()


def test_six_processes_across_three_plugins_insert_one_work(db, monkeypatch):
    for variable in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS"):
        monkeypatch.setenv(variable, "1")
    context = multiprocessing.get_context("spawn")
    workers = 6
    with ProcessPoolExecutor(max_workers=workers, mp_context=context,
                             initializer=_initialize_race,
                             initargs=(context.Barrier(workers),)) as pool:
        futures = [pool.submit(_import_in_separate_process, os.environ["KAHI_TEST_MONGODB_URL"],
                               db.name, index) for index in range(workers)]
        results = [future.result(timeout=90) for future in futures]
    assert len({pid for pid, _ in results}) == workers
    assert db.works.count_documents({}) == 1
    work = db.works.find_one()
    assert work["doi"] == DOI and "_doi_key" not in work
    stored = {(ext["source"], ext["id"]) for ext in work["external_ids"]}
    assert {identifier for _, identifier in results}.issubset(stored)
    assert not any(index.get("unique") for name, index in db.works.index_information().items() if name != "_id_")
    assert db[identity.COORDINATION_COLLECTION].count_documents({}) == 1


def test_six_dspace_processes_same_doi_two_titles_create_two_works(db, monkeypatch):
    for variable in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS"):
        monkeypatch.setenv(variable, "1")
    context = multiprocessing.get_context("spawn")
    workers = 6
    with ProcessPoolExecutor(max_workers=workers, mp_context=context,
                             initializer=_initialize_race,
                             initargs=(context.Barrier(workers),)) as pool:
        futures = [pool.submit(_import_in_separate_process, os.environ["KAHI_TEST_MONGODB_URL"],
                               db.name, index, True) for index in range(workers)]
        results = [future.result(timeout=90) for future in futures]
    assert len({pid for pid, _ in results}) == workers
    assert db.works.count_documents({"doi": DOI}) == 2
    assert db.works.count_documents({"_doi_key": {"$exists": True}}) == 0
    stored = {(ext["source"], ext["id"]) for work in db.works.find()
              for ext in work["external_ids"]}
    assert {identifier for _, identifier in results}.issubset(stored)
    # The existing DSpace parser already emits thesis; DOI coordination must
    # introduce no further fields into that established document shape.
    allowed_fields = set(empty_work()) | {"_id", "thesis"}
    assert all(set(work) <= allowed_fields for work in db.works.find()), [
        set(work) - allowed_fields for work in db.works.find()]
