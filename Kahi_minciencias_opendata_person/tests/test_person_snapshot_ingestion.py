import copy
import importlib

import mongomock
import pytest


plugin_module = importlib.import_module(
    "kahi_minciencias_opendata_person.Kahi_minciencias_opendata_person"
)
Plugin = plugin_module.Kahi_minciencias_opendata_person
ENTITIES = ("works", "projects", "patents", "events", "persons", "affiliations")


def person_snapshot():
    return {
        "_id": "0000000001",
        "updated": [{"source": "minciencias", "time": 1}],
        "full_name": "Ada Lovelace",
        "first_names": ["Ada"],
        "last_names": ["Lovelace"],
        "initials": "A",
        "aliases": ["Augusta Ada Lovelace"],
        "affiliations": [{
            "id": "COL0000001", "name": "Grupo fuente",
            "types": [{"source": "scienti", "type": "group"}],
            "start_date": 1, "end_date": -1, "position": "Investigadora",
        }, {
            "id": "", "name": "Universidad Exacta", "types": [],
            "start_date": 2, "end_date": 3, "position": "Docente",
        }],
        "keywords": ["Matemáticas"],
        "external_ids": [{
            "provenance": "minciencias", "source": "scienti",
            "id": {"COD_RH": "0000000001"},
        }],
        "sex": "Mujer", "marital_status": None,
        "ranking": [{"source": "minciencias", "rank": "Emérito", "date": 1}],
        "birthplace": {"country": "Colombia"}, "birthdate": -1,
        "degrees": [], "subjects": [], "citations_count": [],
        "products_count": 0,
        "related_works": [{
            "provenance": "minciencias", "source": "doi",
            "id": "https://doi.org/10.1000/example", "author_count": 2,
        }],
    }


def config(**source_changes):
    source = {
        "database_url": "mongodb://source", "database_name": "dam",
        "release_name": "release_six", "batch_size": 50, "verbose": 0,
    }
    source.update(source_changes)
    return {
        "database_url": "mongodb://target", "database_name": "kahi",
        "minciencias_opendata_person": source,
    }


def prepare(monkeypatch, *, entity_count=6):
    clients = {
        "mongodb://target": mongomock.MongoClient(),
        "mongodb://source": mongomock.MongoClient(),
    }
    monkeypatch.setattr(plugin_module, "MongoClient", lambda uri: clients[uri])
    target = clients["mongodb://target"].kahi
    source = clients["mongodb://source"].dam
    target.affiliations.insert_many([{
        "_id": "group-target", "external_ids": [
            {"source": "minciencias", "id": "COL0000001"}
        ],
        "names": [{"lang": "es", "name": "Grupo canónico"}],
        "types": [{"source": "minciencias", "type": "group"}],
    }, {
        "_id": "institution-target", "external_ids": [],
        "names": [{"lang": "es", "name": "Universidad Exacta"}],
        "types": [{"source": "ror", "type": "education"}],
    }])
    source.persons_final.insert_one(person_snapshot())
    collections = {entity: "final_{}".format(entity) for entity in ENTITIES}
    collections["persons"] = "persons_final"
    runs = {entity: "run_{}".format(entity) for entity in ENTITIES}
    source.scienti_final_release_publications.insert_one({
        "_id": "release_six", "status": "published",
        "entity_count": entity_count, "audit": "release_six_audit",
        "collections": collections, "materialization_runs": runs,
    })
    source.scienti_final_release_audits.insert_one({
        "_id": "release_six_audit", "release_name": "release_six",
        "status": "passed", "critical_anomalies": 0,
        "collections": collections, "materialization_runs": runs,
        "evidence": {"persons": {
            "collection": "persons_final", "documents": 1,
            "materialization_run": "run_persons",
        }},
    })
    return target, source


def test_streams_snapshot_resolves_affiliations_and_is_idempotent(monkeypatch):
    target, source = prepare(monkeypatch)
    source_indexes = source.persons_final.index_information()

    first = Plugin(config()).run()
    repeated = Plugin(config()).run()

    person = target.person.find_one({"_id": "0000000001"})
    assert first["inserted"] == 1
    assert repeated == first
    assert person["affiliations"][0]["id"] == "group-target"
    assert person["affiliations"][0]["name"] == "Grupo canónico"
    assert person["affiliations"][1]["id"] == "institution-target"
    assert person["related_works"] == person_snapshot()["related_works"]
    assert len(person["affiliations"]) == 2
    assert source.persons_final.index_information() == source_indexes


def test_replaces_legacy_minciencias_works_but_preserves_other_sources(monkeypatch):
    target, _ = prepare(monkeypatch)
    existing = person_snapshot()
    existing["related_works"] = [{
        "provenance": "minciencias", "source": "scienti",
        "id": {"COD_RH": "0000000001", "COD_PRODUCTO": "5"},
    }, {
        "provenance": "openalex", "source": "openalex", "id": "W1",
    }]
    existing["citations_count"] = [{"source": "openalex", "count": 7}]
    existing["products_count"] = 8
    target.person.insert_one(existing)

    summary = Plugin(config()).run()
    person = target.person.find_one({"_id": "0000000001"})

    assert summary["removed_legacy_related_works"] == 1
    assert {item["source"] for item in person["related_works"]} == {"doi", "openalex"}
    assert person["citations_count"] == [{"source": "openalex", "count": 7}]
    assert person["products_count"] == 8


def test_rejects_legacy_options_and_non_explicit_release(monkeypatch):
    prepare(monkeypatch)
    with pytest.raises(ValueError, match="legacy person source options"):
        Plugin(config(groups_production="gruplac_production_data"))
    with pytest.raises(ValueError, match="explicit immutable release_name"):
        Plugin(config(release_name="current"))


def test_rejects_four_entity_release_and_missing_target_group(monkeypatch):
    prepare(monkeypatch, entity_count=4)
    with pytest.raises(RuntimeError, match="six-entity person snapshot"):
        Plugin(config())

    target, source = prepare(monkeypatch)
    target.affiliations.delete_one({"_id": "group-target"})
    with pytest.raises(RuntimeError, match="groups must be imported"):
        Plugin(config()).run()
    assert target.person.count_documents({}) == 0


def test_checkpoint_resumes_and_records_source_identity(monkeypatch):
    target, source = prepare(monkeypatch)
    invalid = copy.deepcopy(person_snapshot())
    invalid["_id"] = "0000000002"
    invalid["external_ids"][0]["id"]["COD_RH"] = "0000000002"
    invalid.pop("birthdate")
    source.persons_final.insert_one(invalid)
    source.scienti_final_release_audits.update_one(
        {"_id": "release_six_audit"},
        {"$set": {"evidence.persons.documents": 2}},
    )

    with pytest.raises(RuntimeError, match="does not follow the Kahi schema"):
        Plugin(config(batch_size=1)).run()
    run = target.minciencias_opendata_person_runs.find_one()
    assert run["last_id"] == "0000000001"
    assert run["contract_version"] == plugin_module.CONTRACT_VERSION

    invalid["birthdate"] = -1
    source.persons_final.replace_one({"_id": "0000000002"}, invalid)
    summary = Plugin(config(batch_size=1)).run()
    assert summary["processed"] == 2
    assert summary["source_collection"] == "persons_final"


def test_rejects_ambiguous_target_cod_rh(monkeypatch):
    target, _ = prepare(monkeypatch)
    external_ids = person_snapshot()["external_ids"]
    target.person.insert_many([
        {"_id": "duplicate-1", "external_ids": copy.deepcopy(external_ids)},
        {"_id": "duplicate-2", "external_ids": copy.deepcopy(external_ids)},
    ])

    with pytest.raises(RuntimeError, match="COD_RH resolves to multiple"):
        Plugin(config()).run()
