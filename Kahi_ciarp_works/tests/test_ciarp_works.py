import importlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import BoundedSemaphore, Lock
from time import sleep
from unittest.mock import MagicMock, patch

from pandas import DataFrame
from bson import ObjectId
from kahi.KahiBase import KahiBase
from kahi_impactu_utils.Mapping import canonical_ror_url, ciarp_mapping

import kahi_ciarp_works.process_one as process_module
from kahi_ciarp_works.parser import parse_ciarp


ciarp_module = importlib.import_module("kahi_ciarp_works.Kahi_ciarp_works")


def test_doi_task_skips_institution_without_valid_doi_before_mapping():
    instance = object.__new__(ciarp_module.Kahi_ciarp_works)
    instance.task = "doi"
    instance.verbose = 1
    instance.mongodb_url = "localhost:27017"
    instance.config = {
        "database_name": "target",
        "ciarp_works": {},
    }
    affiliation = {
        "_id": "04cmc9894",
        "names": [{"name": "Universidad Pedagógica Nacional"}],
        "types": [],
    }
    affiliations = MagicMock()
    affiliations.find_one.return_value = affiliation
    instance.db = {"affiliations": affiliations}
    instance._institution_ids = lambda: ["04cmc9894"]
    instance._config_for_institution = lambda _institution_id: {}
    instance._load_ciarp_data = lambda _database, _institution_id: DataFrame(
        [{"doi": "0", "ranking": "Artículo", "identificación": "1"}]
    )

    with patch.object(ciarp_module, "ciarp_mapping") as mapping:
        instance.process_ciarp()

    mapping.assert_not_called()
    affiliations.find_one.assert_any_call({
        "external_ids.id": "https://ror.org/04cmc9894"
    })


def test_process_ciarp_reports_progress_per_institution(capsys):
    instance = object.__new__(ciarp_module.Kahi_ciarp_works)
    instance.task = "doi"
    instance.verbose = 0
    instance.n_jobs = 2
    instance.es_handler = None
    instance.es_semaphore = None
    instance.mongodb_url = "localhost:27017"
    instance.config = {"database_name": "target", "ciarp_works": {}}
    instance.empty_work = lambda: {}
    affiliations = MagicMock()
    affiliations.find_one.return_value = {"_id": "04cmc9894"}
    instance.db = {"affiliations": affiliations}
    instance._institution_ids = lambda: ["04cmc9894"]
    instance._config_for_institution = lambda _institution_id: {}
    instance._load_ciarp_data = lambda _database, _institution_id: DataFrame([
        {"doi": "10.1234/a", "ranking": "Artículo", "título": "Uno", "identificación": "1"},
        {"doi": "10.1234/b", "ranking": "Artículo", "título": "Dos", "identificación": "2"},
        {"doi": "10.1234/c", "ranking": "Artículo", "título": "Tres", "identificación": "3"},
    ])
    outcomes = {"Uno": "nuevos", "Dos": "actualizados", "Tres": None}

    with patch.object(ciarp_module, "ciarp_mapping", return_value=["Artículo"]), \
            patch.object(ciarp_module, "MongoClient", MagicMock()), \
            patch.object(ciarp_module, "process_one",
                         lambda paper, *_a, **_k: outcomes[paper["título"]]):
        instance.process_ciarp()

    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == ("[ciarp_works:doi_04cmc9894] START total=3 | "
                        "institution=https://ror.org/04cmc9894 num_jobs=2")
    assert lines[-1].startswith("[ciarp_works:doi_04cmc9894] DONE 3 reg en ")
    for count in ("nuevos=1", "actualizados=1", "omitidos=1"):
        assert count in lines[-1]


def ciarp_record():
    return {
        "título": "Controlled work",
        "idioma": "es",
        "doi": "",
        "issn": "",
        "isbn": "",
        "revista": "",
        "año": "2024",
        "volumen": "",
        "issue": "",
        "primera_página": "10",
        "última_página": "20",
        "ranking": "Artículo",
        "identificación": "123",
        "código_unidad_académica": "",
        "código_subunidad_académica": "",
        "index": "timestamp-id",
        "source_record_id": "mongo-source-id",
    }


def affiliation():
    return {
        "_id": ObjectId(),
        "names": [{"name": "University", "lang": "en", "source": "ror"}],
        "types": [],
    }


def test_parser_preserves_timestamp_adds_stable_id_and_correct_pages():
    entry = parse_ciarp(ciarp_record(), affiliation(), KahiBase.empty_work(None))

    assert {ext["source"] for ext in entry["external_ids"]} == {
        "ciarp", "ciarp_record"}
    assert process_module.ciarp_record_id(entry) == "mongo-source-id"
    assert entry["bibliographic_info"] == {
        "start_page": "10", "end_page": "20"}


def test_es_document_uses_numeric_year_and_correct_pages():
    record = ciarp_record()
    record["año"] = ""
    entry = parse_ciarp(record, affiliation(), KahiBase.empty_work(None))

    document = process_module.build_es_work(entry)

    assert document["year"] == 0
    assert document["first_page"] == "10"
    assert document["last_page"] == "20"


def test_es_operations_respect_concurrency_limit():
    semaphore = BoundedSemaphore(2)
    lock = Lock()
    active = 0
    maximum = 0

    def operation():
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
        sleep(0.01)
        with lock:
            active -= 1

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(
            process_module.run_es_operation, semaphore, operation)
            for _ in range(16)]
        for future in futures:
            future.result()

    assert maximum == 2


def test_existing_source_record_reconciles_es_without_search(monkeypatch):
    collection = MagicMock()
    collection.find_one.return_value = {"_id": "mongo-id"}
    es_handler = MagicMock()
    es_handler.es_index = "works"
    es_handler.es.exists.return_value = False
    update = MagicMock()
    monkeypatch.setattr(
        process_module, "parse_ciarp",
        lambda *_args, **_kwargs: parse_ciarp(
            ciarp_record(), affiliation(), KahiBase.empty_work(None)))
    monkeypatch.setattr(process_module, "process_one_update", update)

    outcome = process_module.process_one(
        ciarp_record(), MagicMock(), collection, affiliation(),
        KahiBase.empty_work(None), True, es_handler,
        es_semaphore=BoundedSemaphore(1))

    assert outcome == "actualizados"
    es_handler.insert_work.assert_called_once()
    es_handler.search_work.assert_not_called()
    update.assert_called_once()


def test_core_metadata_repairs_empty_fields_without_overwriting():
    source_id = ObjectId()
    sources = MagicMock()
    sources.find_one.return_value = {
        "_id": source_id,
        "names": [{"name": "Revista", "lang": "es"}],
    }
    current = {
        "doi": "10.1000/existing",
        "date_published": None,
        "year_published": None,
        "source": {},
    }
    incoming = {
        "doi": "10.1000/incoming",
        "date_published": 123456,
        "year_published": 2024,
        "source": {"external_ids": [{"id": "1234-5678"}]},
    }

    changed = process_module.merge_missing_core_metadata(
        current, incoming, {"sources": sources})

    assert changed == {
        "date_published": 123456,
        "year_published": 2024,
        "source": {"id": source_id, "name": "Revista"},
    }
    assert current["doi"] == "10.1000/existing"


def test_blank_title_is_skipped_before_parser(monkeypatch):
    parser = MagicMock()
    monkeypatch.setattr(process_module, "parse_ciarp", parser)
    record = ciarp_record()
    record["título"] = "   "

    outcome = process_module.process_one(
        record, MagicMock(), MagicMock(), affiliation(),
        KahiBase.empty_work(None), True, MagicMock())

    assert outcome == "omitidos"
    parser.assert_not_called()


def test_insert_preserves_identifier_for_unresolved_author():
    collection = MagicMock()
    collection.update_one.return_value.upserted_id = ObjectId()
    db = {
        "person": MagicMock(),
        "sources": MagicMock(),
        "affiliations": MagicMock(),
    }
    db["person"].find_one.return_value = None
    db["sources"].find_one.return_value = None
    db["affiliations"].find_one.return_value = None

    outcome = process_module.process_one_insert(
        ciarp_record(), db, collection, affiliation(),
        KahiBase.empty_work(None), None,
    )

    assert outcome == "nuevos"
    pipeline = collection.update_one.call_args.args[1]
    inserted = pipeline[0]["$replaceRoot"]["newRoot"]["$cond"][1]["$literal"]
    assert inserted["authors"][0]["id"] == ""
    assert inserted["authors"][0]["external_ids"] == [{
        "provenance": "ciarp",
        "source": "Cédula de Ciudadanía",
        "id": "123",
    }]


def test_update_merges_eligible_affiliations_without_removing_existing():
    work = {"authors": [{
        "id": "person-1", "full_name": "Author",
        "affiliations": [{"id": "openalex-affiliation"}],
    }]}
    incoming = [
        {"id": "institution", "types": [{"type": "education"}]},
        {"id": "faculty", "types": [{"type": "faculty"}]},
        {"id": "department", "types": [{"type": "department"}]},
    ]

    assert process_module.merge_author_affiliations(
        work, "person-1", incoming)
    assert [item["id"] for item in work["authors"][0]["affiliations"]] == [
        "openalex-affiliation", "institution", "faculty", "department"]


def test_update_does_not_merge_affiliations_for_another_author():
    work = {"authors": [{
        "id": "person-1", "full_name": "Author", "affiliations": []}]}

    assert not process_module.merge_author_affiliations(
        work, "person-2", [{"id": "institution"}])
    assert work["authors"][0]["affiliations"] == []


def run_temporal_update(monkeypatch, year):
    entry = KahiBase.empty_work(None)
    entry.update({
        "year_published": year,
        "titles": [{"title": "Controlled work"}],
        "external_ids": [],
        "authors": [{
            "id": "",
            "full_name": "",
            "external_ids": [{"source": "Cédula de Ciudadanía", "id": "123"}],
            "affiliations": [
                {"external_ids": [{"id": "institution"}], "name": "",
                 "types": [{"type": "education"}]},
                {"external_ids": [{"id": "faculty"}], "name": "",
                 "types": [{"type": "faculty"}]},
            ],
        }],
    })
    monkeypatch.setattr(process_module, "parse_ciarp", lambda *_args: entry)
    person = {
        "_id": "person-1", "full_name": "Author", "external_ids": [],
        "affiliations": [{
            "id": "institution",
            "start_date": int(datetime(
                2022, 1, 1, tzinfo=timezone.utc).timestamp()),
            "end_date": int(datetime(
                2023, 12, 31, tzinfo=timezone.utc).timestamp()),
        }],
    }
    people = MagicMock()
    people.find_one.return_value = person
    affiliations = MagicMock()
    affiliation_docs = {
        "institution": {
            "_id": "institution", "names": [{
                "name": "University", "lang": "en", "source": "ror"}],
            "types": [{"type": "education"}],
        },
        "faculty": {
            "_id": "faculty", "names": [{
                "name": "Faculty", "lang": "en", "source": "staff"}],
            "types": [{"type": "faculty"}],
        },
    }
    affiliations.find_one.side_effect = lambda query, *_args: (
        affiliation_docs.get(query.get("_id")))
    db = {"person": people, "affiliations": affiliations}
    collection = MagicMock()
    current = {
        "_id": ObjectId(), "updated": [], "titles": [], "external_ids": [],
        "authors": [{
            "id": "person-1", "full_name": "Author",
            "affiliations": [{"id": "external-source"}],
        }],
    }

    process_module.process_one_update(
        ciarp_record(), current, db, collection, {
            "_id": "institution", "names": [], "types": []},
        KahiBase.empty_work(None),
    )
    return collection.update_one.call_args.args[1]["$set"]["authors"][0]


def test_existing_author_receives_only_parent_when_staff_has_no_units(monkeypatch):
    author = run_temporal_update(monkeypatch, 2023)
    assert [item["id"] for item in author["affiliations"]] == [
        "external-source", "institution"]


def test_existing_author_keeps_other_sources_outside_interval(monkeypatch):
    author = run_temporal_update(monkeypatch, 2020)
    assert author["affiliations"] == [{"id": "external-source"}]


def test_temporal_contract_treats_missing_end_as_current():
    author = {"affiliations": [{
        "id": "institution",
        "start_date": int(datetime(
            2022, 1, 1, tzinfo=timezone.utc).timestamp()),
        "end_date": -1,
    }]}

    assert process_module.publication_year_in_parent_affiliation(
        author, "institution", 2026, current_year=2026)
    assert process_module.publication_year_in_parent_affiliation(
        author, "institution", 2027, current_year=2026)
    assert not process_module.publication_year_in_parent_affiliation(
        author, "institution", 2028, current_year=2026)


def test_temporal_contract_rejects_missing_start():
    author = {"affiliations": [{
        "id": "institution", "start_date": None, "end_date": -1}]}

    assert not process_module.publication_year_in_parent_affiliation(
        author, "institution", 2024, current_year=2026)


def test_temporal_contract_checks_all_parent_history_entries():
    author = {"affiliations": [
        {"id": "institution", "start_date": None, "end_date": -1},
        {
            "id": "institution",
            "start_date": int(datetime(
                2018, 1, 1, tzinfo=timezone.utc).timestamp()),
            "end_date": int(datetime(
                2020, 12, 31, tzinfo=timezone.utc).timestamp()),
        },
    ]}

    assert process_module.publication_year_in_parent_affiliation(
        author, "institution", 2019, current_year=2026)


def test_shared_ciarp_catalogue_includes_04cmc9894():
    assert "Articulo en revista Tipo A1" in ciarp_mapping(
        "https://ror.org/04cmc9894", "works")


def test_fucs_ciarp_catalogue_routes_entity_categories():
    ror = "https://ror.org/02yr3f298"

    assert ciarp_mapping(ror, "works") == [
        "Artículo de revista", "Capítulo de libro", "Libro"]
    assert "Ponencias en eventos" in ciarp_mapping(ror, "events")
    assert ciarp_mapping(ror, "misc") == ["Norma técnica"]


def test_canonical_ror_url_repairs_legacy_spacing():
    assert canonical_ror_url("https: //ror.org/02yr3f298") == (
        "https://ror.org/02yr3f298")


def test_process_author_stops_once_the_person_is_already_an_author():
    import mongomock

    db = mongomock.MongoClient().kahi
    db.person.insert_many([
        {"_id": "p1", "full_name": "Ana Pérez", "first_names": ["Ana"], "last_names": ["Pérez"],
         "initials": "A", "affiliations": [], "external_ids": [{"source": "cedula", "id": "123"}]},
        {"_id": "p2", "full_name": "Ana Pérez", "first_names": ["Ana"], "last_names": ["Pérez"],
         "initials": "A", "affiliations": [], "external_ids": []},
    ])
    entry = {"authors": [{"full_name": "Ana Pérez", "external_ids": [{"source": "cedula", "id": "123"}],
                          "affiliations": []}], "doi": "https://doi.org/10.1/x", "external_ids": []}
    colav_reg = {"authors": [{"id": "p1", "full_name": "Ana Pérez", "affiliations": []},
                             {"id": "p2", "full_name": "Ana Pérez", "affiliations": []}],
                 "doi": "https://doi.org/10.1/x", "external_ids": []}
    lookups = []
    original = db.person.find_one

    def counting_find_one(*args, **kwargs):
        lookups.append(args[0])
        return original(*args, **kwargs)
    db.person.find_one = counting_find_one

    process_module.process_author(entry, colav_reg, db)

    assert [a["id"] for a in colav_reg["authors"]] == ["p1", "p2"]
    assert not any(q == {"_id": "p2"} for q in lookups)
