import copy
import importlib

import mongomock
import pytest


plugin_module = importlib.import_module(
    "kahi_minciencias_opendata_works.Kahi_minciencias_opendata_works"
)
Plugin = plugin_module.Kahi_minciencias_opendata_works
ENTITIES = ("works", "projects", "patents", "events", "persons", "affiliations")


def work_snapshot(identifier="work-1", group_code="COL0000001"):
    return {
        "_id": identifier,
        "updated": [{"source": "minciencias", "time": 1}],
        "titles": [{"title": "Trabajo fuente", "lang": "es", "source": "minciencias"}],
        "doi": "https://doi.org/10.1000/example", "abstracts": [],
        "keywords": ["ciencia"], "types": [{
            "provenance": "minciencias", "source": "impactu",
            "type": "Artículo de revista", "level": 0,
        }],
        "external_ids": [{
            "provenance": "minciencias", "source": "minciencias", "id": "P-1",
        }],
        "external_urls": [], "date_published": None, "year_published": 2024,
        "bibliographic_info": {
            "volume": "3",
            "minciencias": {"schema_version": "v1", "product_ids": ["P-1"]},
        },
        "open_access": {}, "apc": {"paid": {}}, "references_count": None,
        "references": [], "citations_count": [], "citations": [],
        "author_count": 2,
        "authors": [{
            "id": "0000000001", "full_name": "Nombre fuente", "type": "advisor",
            "affiliations": [{
                "institution": "Universidad Exacta", "source": "gruplac",
                "group_code": group_code, "membership_period": "2020 - Actual",
                "product_in_group": True,
            }],
        }, {
            "id": "", "full_name": "Autor sin ID", "type": "author",
            "affiliations": [],
        }],
        "source": {}, "ranking": [], "subjects": [], "citations_by_year": [],
        "groups": [{
            "id": group_code, "name": "Grupo fuente",
            "affiliations": "Universidad Exacta",
        }],
        "rights": [], "primary_topic": {}, "topics": [],
        "source_metadata": {"large": "evidence"}, "authorship_status": "resolved",
    }


def config(**changes):
    source = {
        "database_url": "mongodb://source", "database_name": "dam",
        "release_name": "release_six", "batch_size": 10, "verbose": 0,
    }
    source.update(changes)
    return {
        "database_url": "mongodb://target", "database_name": "kahi",
        "minciencias_opendata_works": source,
    }


def prepare(monkeypatch, works=None, entity_count=6):
    works = works or [work_snapshot()]
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
            "id": "institution-canonical", "name": "Universidad Exacta",
            "types": [{"source": "ror", "type": "education"}],
            "start_date": 1, "end_date": -1, "position": "Docente",
        }],
    })
    target.affiliations.insert_many([{
        "_id": "group-canonical",
        "names": [{"lang": "es", "name": "Grupo canónico"}],
        "types": [{"source": "minciencias", "type": "group"}],
        "external_ids": [{"source": "minciencias", "id": "COL0000001"}],
        "relations": [{
            "id": "institution-canonical", "name": "Universidad Exacta",
        }],
    }, {
        "_id": "institution-canonical",
        "names": [{"lang": "es", "name": "Universidad Exacta"}],
        "types": [{"source": "ror", "type": "education"}],
        "external_ids": [], "relations": [],
    }])
    source.works_final.insert_many(copy.deepcopy(works))
    collections = {entity: "final_{}".format(entity) for entity in ENTITIES}
    collections["works"] = "works_final"
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
        "evidence": {"works": {
            "collection": "works_final", "documents": len(works),
            "materialization_run": "run_works",
        }},
    })
    return target, source


def test_imports_exact_snapshot_shape_and_resolves_references(monkeypatch):
    target, source = prepare(monkeypatch)
    source_indexes = source.works_final.index_information()
    people_before = list(target.person.find())

    summary = Plugin(config()).run()
    repeated = Plugin(config()).run()

    work = target.works.find_one({"_id": "work-1"})
    assert summary["inserted"] == 1
    assert repeated == summary
    assert work["authors"][0]["id"] == "person-canonical"
    assert work["authors"][1]["id"] == ""
    assert {item["id"] for item in work["authors"][0]["affiliations"]} == {
        "group-canonical", "institution-canonical",
    }
    assert work["groups"] == [{"id": "group-canonical", "name": "Grupo canónico"}]
    assert work["bibliographic_info"] == work_snapshot()["bibliographic_info"]
    assert "source_metadata" not in work and "authorship_status" not in work
    assert list(target.person.find()) == people_before
    assert source.works_final.index_information() == source_indexes


def test_merge_preserves_existing_metadata_and_enriches_bibliography(monkeypatch):
    target, _ = prepare(monkeypatch)
    existing = work_snapshot()
    existing.pop("source_metadata")
    existing.pop("authorship_status")
    existing["titles"] = [{
        "title": "Título OpenAlex", "lang": "es", "source": "openalex",
    }]
    existing["bibliographic_info"] = {"volume": "99", "issue": "2"}
    existing["citations_count"] = [{"source": "openalex", "count": 7}]
    existing["authors"] = []
    existing["author_count"] = 0
    target.works.insert_one(existing)

    Plugin(config()).run()
    work = target.works.find_one({"_id": "work-1"})

    assert work["bibliographic_info"]["volume"] == "99"
    assert work["bibliographic_info"]["issue"] == "2"
    assert work["bibliographic_info"]["minciencias"]["product_ids"] == ["P-1"]
    assert {item["source"] for item in work["titles"]} == {
        "openalex", "minciencias",
    }
    assert work["citations_count"] == [{"source": "openalex", "count": 7}]


def test_checkpoint_resumes_after_a_missing_group_is_fixed(monkeypatch):
    works = [work_snapshot("work-1"), work_snapshot("work-2", "COL0000002")]
    target, _ = prepare(monkeypatch, works=works)

    with pytest.raises(RuntimeError, match="unresolved work groups"):
        Plugin(config(batch_size=1)).run()
    run = target.minciencias_opendata_works_runs.find_one()
    assert run["last_id"] == "work-1"
    assert target.works.count_documents({}) == 1

    target.affiliations.insert_one({
        "_id": "group-2", "names": [{"lang": "es", "name": "Grupo dos"}],
        "types": [{"source": "minciencias", "type": "group"}],
        "external_ids": [{"source": "minciencias", "id": "COL0000002"}],
        "relations": [],
    })
    summary = Plugin(config(batch_size=1)).run()
    assert summary["processed"] == 2
    assert target.works.count_documents({}) == 2


def test_rejects_legacy_options_current_and_four_entity_release(monkeypatch):
    prepare(monkeypatch)
    with pytest.raises(ValueError, match="legacy works options"):
        Plugin(config(person_related_works=True))
    with pytest.raises(ValueError, match="explicit immutable release_name"):
        Plugin(config(release_name="current"))

    prepare(monkeypatch, entity_count=4)
    with pytest.raises(RuntimeError, match="six-entity works snapshot"):
        Plugin(config())
