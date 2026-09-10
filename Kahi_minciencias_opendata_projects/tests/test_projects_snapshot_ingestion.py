import copy
import importlib

import mongomock
import pytest


plugin_module = importlib.import_module(
    "kahi_minciencias_opendata_projects.Kahi_minciencias_opendata_projects"
)
Plugin = plugin_module.Kahi_minciencias_opendata_projects
ENTITIES = ("works", "projects", "patents", "events", "persons", "affiliations")


def project_snapshot(identifier="project-1", group_code="COL0000001"):
    return {
        "_id": identifier,
        "titles": [{"title": "Proyecto fuente", "lang": "es", "source": "scienti"}],
        "updated": [{"source": "scienti", "time": 1}],
        "abstract": "Resumen fuente",
        "types": [{"provenance": "scienti", "source": "impactu", "type": "Proyecto"}],
        "external_ids": [{
            "provenance": "scienti", "source": "scienti",
            "id": {"source_kind": "cvlac", "source_id": "0000000001"},
        }],
        "external_urls": [], "date_init": 1, "date_end": None,
        "year_init": 2024, "year_end": None, "author_count": 2,
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
        "minciencias_opendata_projects": source,
    }


def prepare(monkeypatch, projects=None, entity_count=6):
    projects = projects or [project_snapshot()]
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
    source.projects_final.insert_many(copy.deepcopy(projects))
    collections = {entity: "final_{}".format(entity) for entity in ENTITIES}
    collections["projects"] = "projects_final"
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
        "evidence": {"projects": {
            "collection": "projects_final", "documents": len(projects),
            "materialization_run": "run_projects",
        }},
    })
    return target, source


def test_imports_snapshot_resolves_references_and_is_idempotent(monkeypatch):
    target, source = prepare(monkeypatch)
    indexes = source.projects_final.index_information()
    people_before = list(target.person.find())

    summary = Plugin(config()).run()
    repeated = Plugin(config()).run()

    project = target.projects.find_one({"_id": "project-1"})
    assert summary["inserted"] == 1
    assert repeated == summary
    assert project["authors"][0]["id"] == "person-canonical"
    assert project["authors"][1]["id"] == ""
    assert project["authors"][0]["affiliations"][0]["id"] == "group-canonical"
    assert project["groups"] == [{"id": "group-canonical", "name": "Grupo canónico"}]
    assert "source_metadata" not in project
    assert set(project) == {"_id"} | plugin_module.PROJECT_FIELDS
    assert list(target.person.find()) == people_before
    assert source.projects_final.index_information() == indexes


def test_merge_preserves_siiu_values_and_adds_scienti_metadata(monkeypatch):
    target, _ = prepare(monkeypatch)
    existing = project_snapshot()
    existing.pop("source_metadata")
    existing["abstract"] = "Resumen SIIU"
    existing["titles"] = [{"title": "Título SIIU", "lang": "es", "source": "siiu"}]
    existing["authors"] = []
    existing["groups"] = []
    existing["author_count"] = 0
    target.projects.insert_one(existing)

    Plugin(config()).run()
    project = target.projects.find_one({"_id": "project-1"})

    assert project["abstract"] == "Resumen SIIU"
    assert {item["source"] for item in project["titles"]} == {"siiu", "scienti"}
    assert project["authors"][0]["id"] == "person-canonical"


def test_checkpoint_resumes_after_missing_group_is_added(monkeypatch):
    projects = [
        project_snapshot("project-1"),
        project_snapshot("project-2", "COL0000002"),
    ]
    target, _ = prepare(monkeypatch, projects=projects)

    with pytest.raises(RuntimeError, match="unresolved project groups"):
        Plugin(config(batch_size=1)).run()
    assert target.minciencias_opendata_projects_runs.find_one()["last_id"] == "project-1"
    assert target.projects.count_documents({}) == 1

    target.affiliations.insert_one({
        "_id": "group-2", "names": [{"lang": "es", "name": "Grupo dos"}],
        "types": [{"source": "minciencias", "type": "group"}],
        "external_ids": [{"source": "minciencias", "id": "COL0000002"}],
    })
    summary = Plugin(config(batch_size=1)).run()
    assert summary["processed"] == 2
    assert target.projects.count_documents({}) == 2


def test_rejects_legacy_options_current_and_four_entity_release(monkeypatch):
    prepare(monkeypatch)
    with pytest.raises(ValueError, match="legacy project options"):
        Plugin(config(num_jobs=4))
    with pytest.raises(ValueError, match="explicit immutable release_name"):
        Plugin(config(release_name="current"))

    prepare(monkeypatch, entity_count=4)
    with pytest.raises(RuntimeError, match="six-entity project snapshot"):
        Plugin(config())
