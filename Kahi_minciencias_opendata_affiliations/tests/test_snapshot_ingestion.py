import copy
import importlib

import mongomock
import pytest


plugin_module = importlib.import_module(
    "kahi_minciencias_opendata_affiliations."
    "Kahi_minciencias_opendata_affiliations"
)
Plugin = plugin_module.Kahi_minciencias_opendata_affiliations

ENTITIES = ("works", "projects", "patents", "events", "persons", "affiliations")


def group_snapshot():
    return {
        "_id": "COL0000001",
        "updated": [{"source": "scienti", "time": 1}],
        "names": [{"source": "scienti", "lang": "es", "name": "Grupo Uno"}],
        "aliases": ["Grupo Uno Histórico"],
        "abbreviations": [],
        "types": [{"source": "scienti", "type": "group"}],
        "year_established": 2001,
        "status": [],
        "relations": [{"id": "", "name": "CEMENTOS ARGOS SA", "types": []}],
        "addresses": [{
            "lat": "", "lng": "", "postcode": "", "state": "Antioquia",
            "city": "Medellín", "country": "Colombia", "country_code": "CO",
        }],
        "external_urls": [],
        "external_ids": [{"source": "minciencias", "id": "COL0000001"}],
        "subjects": [],
        "ranking": [{"source": "minciencias", "rank": "Reconocido", "date": 1}],
        "description": [],
    }


def config(source_mode="snapshot"):
    return {
        "database_url": "mongodb://target",
        "database_name": "kahi",
        "minciencias_opendata_affiliations": {
            "database_url": "mongodb://source",
            "database_name": "dam",
            "source_mode": source_mode,
            "release_name": "release_six",
            "num_jobs": 1,
            "verbose": 0,
        },
    }


def prepare_databases(monkeypatch, *, entity_count=6):
    clients = {
        "mongodb://target": mongomock.MongoClient(),
        "mongodb://source": mongomock.MongoClient(),
    }

    def client(uri):
        return clients[uri]

    monkeypatch.setattr(plugin_module, "MongoClient", client)
    target = clients["mongodb://target"].kahi
    source = clients["mongodb://source"].dam
    target.affiliations.insert_one({
        "_id": "canonical-argos",
        "updated": [],
        "names": [{"source": "scienti", "lang": "es", "name": "CEMENTOS ARGOS SA"}],
        "aliases": [], "abbreviations": [],
        "types": [{"source": "scienti", "type": "company"}],
        "year_established": None, "status": [], "relations": [],
        "addresses": [{
            "lat": 6.2, "lng": -75.5, "postcode": "", "state": "Antioquia",
            "city": "Medellín", "country": "Colombia", "country_code": "CO",
        }],
        "external_urls": [],
        "external_ids": [{"source": "minciencias", "id": "000000009892"}],
        "subjects": [], "ranking": [], "description": [],
        "citation_count": [], "products_count": 0,
    })
    source.affiliations_final.insert_one(group_snapshot())
    collections = {entity: f"final_{entity}" for entity in ENTITIES}
    collections["affiliations"] = "affiliations_final"
    runs = {entity: f"run_{entity}" for entity in ENTITIES}
    source.scienti_final_release_publications.insert_one({
        "_id": "release_six", "status": "published", "entity_count": entity_count,
        "audit": "release_six_audit", "collections": collections,
        "materialization_runs": runs,
    })
    source.scienti_final_release_audits.insert_one({
        "_id": "release_six_audit", "release_name": "release_six",
        "status": "passed", "critical_anomalies": 0, "collections": collections,
        "materialization_runs": runs,
        "evidence": {
            "affiliations": {
                "collection": "affiliations_final", "documents": 1,
                "materialization_run": "run_affiliations",
            }
        },
    })
    return target, source


def test_consumes_proven_six_entity_snapshot_without_mutating_dam(monkeypatch):
    target, source = prepare_databases(monkeypatch)
    source_indexes = source.affiliations_final.index_information()

    summary = Plugin(config()).run()
    repeated = Plugin(config()).run()

    group = target.affiliations.find_one({"_id": "COL0000001"})
    assert summary["source_documents"] == 1
    assert summary["inserted"] == 1
    assert repeated == summary
    assert group["ranking"][0]["rank"] == "Reconocido"
    assert group["relations"][0]["id"] == "canonical-argos"
    assert target.affiliations.count_documents({"_id": {"$regex": "^IUA"}}) == 0
    assert len(group["relations"]) == 1
    assert source.affiliations_final.index_information() == source_indexes


def test_rejects_a_four_entity_release(monkeypatch):
    prepare_databases(monkeypatch, entity_count=4)

    with pytest.raises(RuntimeError, match="six-entity affiliation snapshot"):
        Plugin(config())


def test_legacy_mode_must_be_explicit(monkeypatch):
    prepare_databases(monkeypatch)
    invalid = config()
    invalid["minciencias_opendata_affiliations"].pop("source_mode")

    with pytest.raises(ValueError, match="source_mode must explicitly"):
        Plugin(invalid)


def test_checkpoint_resumes_and_records_source_identity(monkeypatch):
    target, source = prepare_databases(monkeypatch)
    second = copy.deepcopy(group_snapshot())
    second["_id"] = "COL0000002"
    second["external_ids"][0]["id"] = "COL0000002"
    source.affiliations_final.insert_one(second)
    source.scienti_final_release_audits.update_one(
        {"_id": "release_six_audit"},
        {"$set": {"evidence.affiliations.documents": 2}},
    )
    plugin = Plugin(config())
    plugin.batch_size = 1
    original = plugin.process_snapshot_one

    def fail_second(reg, collection, verbose):
        if reg["_id"] == "COL0000002":
            raise RuntimeError("controlled failure")
        return original(reg, collection, verbose)

    plugin.process_snapshot_one = fail_second
    with pytest.raises(RuntimeError, match="controlled failure"):
        plugin.run()
    run = target.minciencias_opendata_affiliations_runs.find_one()
    assert run["last_id"] == "COL0000001"
    assert run["contract_version"] == plugin_module.CONTRACT_VERSION

    summary = Plugin(config()).run()
    assert summary["processed"] == 2
    assert summary["source_collection"] == "affiliations_final"
