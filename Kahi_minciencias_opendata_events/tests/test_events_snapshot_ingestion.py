import copy
import importlib

import mongomock
import pytest


plugin_module = importlib.import_module(
    "kahi_minciencias_opendata_events.Kahi_minciencias_opendata_events"
)
Plugin = plugin_module.Kahi_minciencias_opendata_events
ENTITIES = ("works", "projects", "patents", "events", "persons", "affiliations")


def event_snapshot(identifier="event-1", group_code="COL0000001"):
    return {
        "_id": identifier,
        "titles": [{"title": "Evento fuente", "lang": "es", "source": "scienti"}],
        "updated": [{"source": "scienti", "time": 1}],
        "abstract": "Resumen fuente",
        "types": [{"provenance": "scienti", "source": "impactu", "type": "Evento"}],
        "external_ids": [{
            "provenance": "scienti", "source": "scienti",
            "id": {"source_kind": "cvlac", "source_id": "0000000001"},
        }],
        "external_urls": [], "date_held": 1704067200, "year_held": 2024,
        "author_count": 2,
        "authors": [{
            "id": "0000000001", "full_name": "Nombre fuente",
            "affiliations": [{"group_code": group_code}],
        }, {"id": "", "full_name": "Autor sin ID", "affiliations": []}],
        "ranking": [],
        "groups": [{"id": group_code, "name": "Grupo fuente"}],
        "source_metadata": {"large": "evidence"},
    }


def config(**changes):
    source = {
        "database_url": "mongodb://source", "database_name": "dam",
        "release_name": "release_six", "batch_size": 10, "verbose": 0,
    }
    source.update(changes)
    return {
        "database_url": "mongodb://target", "database_name": "kahi",
        "minciencias_opendata_events": source,
    }


def prepare(monkeypatch, events=None, entity_count=6):
    events = events or [event_snapshot()]
    clients = {
        "mongodb://target": mongomock.MongoClient(),
        "mongodb://source": mongomock.MongoClient(),
    }
    monkeypatch.setattr(plugin_module, "MongoClient", lambda uri: clients[uri])
    target = clients["mongodb://target"].kahi
    source = clients["mongodb://source"].dam
    target.person.insert_one({
        "_id": "person-canonical", "full_name": "Nombre canónico",
        "external_ids": [{
            "provenance": "minciencias", "source": "scienti",
            "id": {"COD_RH": "0000000001"},
        }],
        "affiliations": [{
            "id": "group-canonical", "name": "Grupo canónico",
            "types": [{"source": "minciencias", "type": "group"}],
            "start_date": 1, "end_date": -1, "position": "Investigador",
        }],
    })
    target.affiliations.insert_one({
        "_id": "group-canonical",
        "names": [{"lang": "es", "name": "Grupo canónico"}],
        "types": [{"source": "minciencias", "type": "group"}],
        "external_ids": [{"source": "minciencias", "id": "COL0000001"}],
    })
    source.events_final.insert_many(copy.deepcopy(events))
    collections = {entity: "final_{}".format(entity) for entity in ENTITIES}
    collections["events"] = "events_final"
    runs = {entity: "run_{}".format(entity) for entity in ENTITIES}
    source.scienti_final_release_publications.insert_one({
        "_id": "release_six", "status": "published", "entity_count": entity_count,
        "audit": "release_six_audit", "collections": collections,
        "materialization_runs": runs,
    })
    source.scienti_final_release_audits.insert_one({
        "_id": "release_six_audit", "release_name": "release_six",
        "status": "passed", "critical_anomalies": 0,
        "collections": collections, "materialization_runs": runs,
        "evidence": {"events": {
            "collection": "events_final", "documents": len(events),
            "materialization_run": "run_events",
        }},
    })
    return target, source


def test_imports_snapshot_resolves_references_and_is_idempotent(monkeypatch):
    target, source = prepare(monkeypatch)
    indexes = source.events_final.index_information()
    people_before = list(target.person.find())

    summary = Plugin(config()).run()
    repeated = Plugin(config()).run()

    event = target.events.find_one({"_id": "event-1"})
    assert summary["inserted"] == 1
    assert repeated == summary
    assert event["authors"][0]["id"] == "person-canonical"
    assert event["authors"][1]["id"] == ""
    assert event["authors"][0]["affiliations"][0]["id"] == "group-canonical"
    assert event["groups"] == [{"id": "group-canonical", "name": "Grupo canónico"}]
    assert "source_metadata" not in event
    assert set(event) == {"_id"} | plugin_module.EVENT_FIELDS
    assert list(target.person.find()) == people_before
    assert source.events_final.index_information() == indexes


def test_merge_preserves_existing_scalars_and_adds_scienti_metadata(monkeypatch):
    target, _ = prepare(monkeypatch)
    existing = event_snapshot()
    existing.pop("source_metadata")
    existing["abstract"] = "Resumen previo"
    existing["date_held"] = 1
    existing["year_held"] = 1970
    existing["titles"] = [{"title": "Evento previo", "lang": "es", "source": "otro"}]
    existing["authors"] = []
    existing["groups"] = []
    existing["author_count"] = 0
    target.events.insert_one(existing)

    Plugin(config()).run()
    event = target.events.find_one({"_id": "event-1"})

    assert event["abstract"] == "Resumen previo"
    assert event["date_held"] == 1
    assert event["year_held"] == 1970
    assert {item["source"] for item in event["titles"]} == {"otro", "scienti"}
    assert event["authors"][0]["id"] == "person-canonical"


def test_checkpoint_resumes_after_missing_group_is_added(monkeypatch):
    events = [
        event_snapshot("event-1"),
        event_snapshot("event-2", "COL0000002"),
    ]
    target, _ = prepare(monkeypatch, events=events)

    with pytest.raises(RuntimeError, match="unresolved event groups"):
        Plugin(config(batch_size=1)).run()
    assert target.minciencias_opendata_events_runs.find_one()["last_id"] == "event-1"
    assert target.events.count_documents({}) == 1

    target.affiliations.insert_one({
        "_id": "group-2", "names": [{"lang": "es", "name": "Grupo dos"}],
        "types": [{"source": "minciencias", "type": "group"}],
        "external_ids": [{"source": "minciencias", "id": "COL0000002"}],
    })
    summary = Plugin(config(batch_size=1)).run()
    assert summary["processed"] == 2
    assert target.events.count_documents({}) == 2


def test_rejects_legacy_options_current_and_four_entity_release(monkeypatch):
    prepare(monkeypatch)
    with pytest.raises(ValueError, match="legacy event options"):
        Plugin(config(num_jobs=4))
    with pytest.raises(ValueError, match="explicit immutable release_name"):
        Plugin(config(release_name="current"))

    prepare(monkeypatch, entity_count=4)
    with pytest.raises(RuntimeError, match="six-entity event snapshot"):
        Plugin(config())
