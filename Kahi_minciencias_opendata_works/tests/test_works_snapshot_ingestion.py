import copy
from hashlib import sha256
import importlib

import mongomock
import pytest
from bson import ObjectId


plugin_module = importlib.import_module(
    "kahi_minciencias_opendata_works.Kahi_minciencias_opendata_works"
)
Plugin = plugin_module.Kahi_minciencias_opendata_works
legacy_module = importlib.import_module("kahi_minciencias_opendata_works.legacy")
ENTITIES = ("works", "projects", "patents", "events", "persons", "affiliations")
WORK_1 = ObjectId("000000000000000000000001")
WORK_2 = ObjectId("000000000000000000000002")


def work_snapshot(identifier=WORK_1, group_code="COL0000001"):
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
        "authorship_candidates": [{"candidate": "person-canonical"}],
        "authorship_evidence": [{"source": "scienti"}],
        "authorship_authority": {"source": "scienti"},
        "identity_collision_of": "original-work-id",
    }


def config(**changes):
    source = {
        "database_url": "mongodb://source", "database_name": "dam",
        "release_name": "release_six", "batch_size": 10, "verbose": 0,
        "task": "all",
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

    work = target.works.find_one({"_id": WORK_1})
    assert summary["inserted"] == 1
    assert repeated == summary
    assert work["authors"][0]["id"] == "person-canonical"
    assert work["authors"][1]["id"] == ""
    assert {item["id"] for item in work["authors"][0]["affiliations"]} == {
        "group-canonical", "institution-canonical",
    }
    assert work["groups"] == [{"id": "group-canonical", "name": "Grupo canónico"}]
    assert work["bibliographic_info"] == work_snapshot()["bibliographic_info"]
    assert not ({
        "source_metadata", "authorship_status", "authorship_candidates",
        "authorship_evidence", "authorship_authority", "identity_collision_of",
    } & set(work))
    assert list(target.person.find()) == people_before
    assert source.works_final.index_information() == source_indexes


def test_converts_legacy_sha256_snapshot_id_to_object_id(monkeypatch):
    legacy_id = "c79770132fd33e53d78a1bfbee3694a6460a7c341a8a1d06fafe0f0a0b67a791"
    target, _ = prepare(monkeypatch, works=[work_snapshot(legacy_id)])

    Plugin(config()).run()

    expected = ObjectId(legacy_id[:24])
    assert target.works.find_one({"_id": expected}) is not None
    assert target.works.find_one({"_id": legacy_id}) is None


def test_converts_official_product_id_to_object_id(monkeypatch):
    source_id = "AAD-0000000594-116-1"
    target, _ = prepare(monkeypatch, works=[work_snapshot(source_id)])

    Plugin(config()).run()

    expected = ObjectId(sha256(source_id.encode("utf-8")).digest()[:12])
    assert target.works.find_one({"_id": expected}) is not None


def test_merge_preserves_existing_metadata_and_enriches_bibliography(monkeypatch):
    target, _ = prepare(monkeypatch)
    existing = work_snapshot()
    existing.pop("source_metadata")
    existing.pop("authorship_status")
    existing["titles"] = [{
        "title": "TRABAJO FUENTE", "lang": "es", "source": "openalex",
    }]
    existing["bibliographic_info"] = {"volume": "99", "issue": "2"}
    existing["citations_count"] = [{"source": "openalex", "count": 7}]
    existing["authors"] = []
    existing["author_count"] = 0
    target.works.insert_one(existing)

    Plugin(config()).run()
    work = target.works.find_one({"_id": WORK_1})

    assert work["bibliographic_info"]["volume"] == "99"
    assert work["bibliographic_info"]["issue"] == "2"
    assert work["bibliographic_info"]["minciencias"]["product_ids"] == ["P-1"]
    assert {item["source"] for item in work["titles"]} == {
        "openalex", "minciencias",
    }
    assert work["citations_count"] == [{"source": "openalex", "count": 7}]


def test_checkpoint_resumes_after_a_missing_group_is_fixed(monkeypatch):
    works = [work_snapshot(WORK_1), work_snapshot(WORK_2, "COL0000002")]
    works[1]["doi"] = "https://doi.org/10.1000/second"
    target, _ = prepare(monkeypatch, works=works)

    with pytest.raises(RuntimeError, match="unresolved work groups"):
        Plugin(config(batch_size=1)).run()
    run = target.minciencias_opendata_works_runs.find_one()
    assert run["last_id"] == WORK_1
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


def test_duplicate_cod_rh_uses_the_unique_name_compatible_person(monkeypatch):
    work = work_snapshot()
    work["authors"][0].update({
        "id": "0000085979", "full_name": "Ludovic Dorkis",
    })
    target, _ = prepare(monkeypatch, works=[work])
    target.person.insert_many([
        {
            "_id": "german", "full_name": "German Sierra Gallego",
            "aliases": [], "affiliations": [],
            "external_ids": [{"source": "scienti", "id": {
                "COD_RH": "0000085979",
            }}],
        },
        {
            "_id": "ludovic", "full_name": "Ludovic Dorkis",
            "aliases": [], "affiliations": [],
            "external_ids": [{"source": "scienti", "id": {
                "COD_RH": "0000085979",
            }}],
        },
    ])

    Plugin(config()).run()

    assert target.works.find_one()["authors"][0]["id"] == "ludovic"


def test_rejects_legacy_options_current_and_four_entity_release(monkeypatch):
    prepare(monkeypatch)
    assert Plugin(config(task="all")).run()["processed"] == 1
    with pytest.raises(ValueError, match="legacy works options"):
        Plugin(config(person_related_works=True))
    with pytest.raises(ValueError, match="legacy works options"):
        Plugin(config(task="legacy"))
    with pytest.raises(ValueError, match="explicit immutable release_name"):
        Plugin(config(release_name="current"))

    prepare(monkeypatch, entity_count=4)
    with pytest.raises(RuntimeError, match="six-entity works snapshot"):
        Plugin(config())


def test_explicit_legacy_mode_delegates_to_historical_reader(monkeypatch):
    captured = {}

    class DummyLegacy:
        def __init__(self, value):
            captured.update(value["minciencias_opendata_works"])

        def run(self):
            return "legacy"

    monkeypatch.setattr(legacy_module, "Kahi_minciencias_opendata_works", DummyLegacy)
    plugin = Plugin(config(source_mode="legacy"))

    assert plugin.run() == "legacy"
    assert captured["collection_name"] == "gruplac_production_data"
    assert captured["person_related_works"] is False


def test_snapshot_keeps_distinct_titles_with_the_same_doi(monkeypatch):
    first, second = work_snapshot(WORK_1), work_snapshot(WORK_2)
    second["titles"][0]["title"] = "Otro trabajo completamente distinto"
    target, _ = prepare(monkeypatch, works=[first, second])
    summary = Plugin(config()).run()
    assert summary["processed"] == 2
    assert target.works.count_documents({"doi": first["doi"]}) == 2
    assert target.works.find_one({"_id": WORK_1})["titles"] == first["titles"]
    assert target.works.find_one({"_id": WORK_2})["titles"] == second["titles"]


def test_snapshot_updates_matching_doi_under_another_work_id(monkeypatch):
    target, _ = prepare(monkeypatch)
    existing_id = ObjectId("000000000000000000000099")
    target.works.insert_one({
        "_id": existing_id, "doi": "10.1000/example",
        "titles": [{"title": "TRABAJO FUENTE", "source": "openalex"}],
        "authors": [], "external_ids": [], "bibliographic_info": {},
    })
    summary = Plugin(config()).run()
    assert summary["inserted"] == 0 and summary["updated"] == 1
    assert target.works.count_documents({}) == 1
    assert target.works.find_one()["_id"] == existing_id
    assert target.works.find_one()["bibliographic_info"]["minciencias"]["product_ids"] == ["P-1"]


def test_snapshot_source_id_collision_cannot_merge_distinct_titles(monkeypatch):
    target, source = prepare(monkeypatch)
    original = work_snapshot()
    original["titles"] = [{"title": "Otra publicación completamente diferente", "source": "openalex"}]
    target.works.insert_one(original)
    before = target.works.find_one({"_id": WORK_1})
    plugin = Plugin(config())
    plugin.run()
    assert target.works.count_documents({}) == 2
    assert target.works.find_one({"_id": WORK_1}) == before
    # Reprocess the source batch independently of the run checkpoint.
    plugin._process_batch(list(source.works_final.find()), {"inserted": 0, "updated": 0,
                                                            "unresolved_institution_evidence": 0})
    assert target.works.count_documents({}) == 2


def test_snapshot_matching_source_id_can_acquire_a_missing_doi(monkeypatch):
    target, _ = prepare(monkeypatch)
    existing = work_snapshot()
    existing["doi"] = ""
    target.works.insert_one(existing)
    summary = Plugin(config()).run()
    assert summary["updated"] == 1
    assert target.works.count_documents({}) == 1
    assert target.works.find_one()["doi"] == work_snapshot()["doi"]


def test_titles_without_language_are_detected_and_updated_time_is_import_time(monkeypatch):
    snapshot = work_snapshot()
    snapshot["titles"] = [{"title": "Prevalencia de parasitismo intestinal en la comunidad indígena",
                           "source": "minciencias"}]
    target, _ = prepare(monkeypatch, works=[snapshot])
    monkeypatch.setattr(plugin_module, "time", lambda: 1_800_000_000)

    Plugin(config()).run()
    work = target.works.find_one({"_id": WORK_1})

    assert work["titles"][0]["lang"] == "es"
    assert work["updated"] == [{"source": "minciencias", "time": 1_800_000_000}]


def test_merge_keeps_one_updated_entry_per_source(monkeypatch):
    target, _ = prepare(monkeypatch)
    existing = work_snapshot()
    for key in ("source_metadata", "authorship_status", "authorship_candidates", "authorship_evidence"):
        existing.pop(key, None)
    existing["updated"] = [{"source": "openalex", "time": 5}, {"source": "minciencias", "time": 1}]
    target.works.insert_one(existing)
    monkeypatch.setattr(plugin_module, "time", lambda: 1_800_000_000)

    Plugin(config()).run()
    work = target.works.find_one({"_id": WORK_1})

    assert work["updated"] == [{"source": "openalex", "time": 5},
                               {"source": "minciencias", "time": 1_800_000_000}]


def test_materialization_marker_is_not_imported_as_a_source(monkeypatch):
    snapshot = work_snapshot()
    snapshot["updated"] = [{"source": "minciencias", "time": 1},
                           {"source": "minciencias_materialization", "time": 2}]
    target, _ = prepare(monkeypatch, works=[snapshot])
    monkeypatch.setattr(plugin_module, "time", lambda: 1_800_000_000)

    Plugin(config()).run()

    assert target.works.find_one({"_id": WORK_1})["updated"] == [
        {"source": "minciencias", "time": 1_800_000_000}]
