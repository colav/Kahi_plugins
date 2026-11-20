"""Entity current, code changes and Kahi logs must select the same audited input."""
import copy
import importlib

import pytest
import yaml

from test_snapshot_ingestion import Plugin, config, group_snapshot, prepare_databases


def automatic_config():
    cfg = config()
    cfg["minciencias_opendata_affiliations"].pop("release_name")
    return cfg


def publish(source, version):
    run = f"affiliations_run_{version}"
    collection = f"minciencias_affiliations_{version}"
    audit = run + "_audit"
    group = group_snapshot()
    group["names"][0]["name"] = f"Grupo {version}"
    source[collection].insert_one(group)
    fingerprints = {"recognized": {"documents": 1, "sha256": version}}
    source.scienti_affiliation_publications.insert_one({
        "_id": run, "status": "published", "collection": collection,
        "audit": audit, "documents": 1, "source_fingerprints": fingerprints,
    })
    source.scienti_affiliation_materialization_audits.insert_one({
        "_id": audit, "run_name": run, "status": "passed", "collection": collection,
        "documents": 1, "critical_anomalies": 0, "source_fingerprints": fingerprints,
    })
    source.scienti_affiliation_materialization_runs.insert_one({
        "_id": run, "status": "complete", "config": {"target_collection": collection},
        "summary": {"collection": collection, "documents": 1, "audit": audit},
    })
    source.scienti_affiliation_publications.replace_one({"_id": "current"}, {
        "_id": "current", "current_run": run, "collection": collection, "audit": audit,
    }, upsert=True)
    return collection


def test_entity_current_is_pinned_once_and_can_advance_without_a_six_entity_release(monkeypatch):
    target, source = prepare_databases(monkeypatch)
    first_collection = publish(source, "v14")
    first = Plugin(automatic_config())
    second_collection = publish(source, "v15")

    # Existing six-entity release still references affiliations_final.
    summary = first.run()
    assert summary["source_collection"] == first_collection
    assert target.affiliations.find_one({"_id": "COL0000001"})["names"][0]["name"] == "Grupo v14"

    second = Plugin(automatic_config()).run()
    assert second["source_collection"] == second_collection
    assert second["publication_kind"] == "affiliation_publication"
    assert "Grupo v15" in {item["name"] for item in target.affiliations.find_one({"_id": "COL0000001"})["names"]}
    assert Plugin(automatic_config()).run() == second
    assert target.minciencias_opendata_affiliations_runs.count_documents({}) == 2
    assert list(target.affiliations.find_one({"_id": "COL0000001"})) == ["_id", *Plugin.empty_affiliation(None)]
    assert source[first_collection].count_documents({}) == source[second_collection].count_documents({}) == 1


@pytest.mark.parametrize("broken", [
    "pointer", "audit_failed", "audit_run", "audit_count", "fingerprints",
    "run_failed", "run_target", "summary", "count", "missing_collection",
])
def test_invalid_entity_current_never_falls_back_to_the_joint_release(monkeypatch, broken):
    target, source = prepare_databases(monkeypatch)
    collection = publish(source, "v14")
    if broken == "pointer":
        source.scienti_affiliation_publications.update_one({"_id": "current"}, {"$set": {"audit": "wrong"}})
    elif broken.startswith("audit_"):
        key, value = {
            "audit_failed": ("status", "failed"),
            "audit_run": ("run_name", "other_run"),
            "audit_count": ("documents", 2),
        }[broken]
        source.scienti_affiliation_materialization_audits.update_one({}, {"$set": {key: value}})
    elif broken == "fingerprints":
        source.scienti_affiliation_publications.update_one({"_id": "affiliations_run_v14"}, {"$set": {"source_fingerprints": {}}})
    elif broken in {"run_failed", "run_target", "summary"}:
        key, value = {
            "run_failed": ("status", "failed"), "run_target": ("config.target_collection", "wrong"),
            "summary": ("summary.audit", "wrong"),
        }[broken]
        source.scienti_affiliation_materialization_runs.update_one({}, {"$set": {key: value}})
    elif broken == "count":
        source[collection].insert_one({"_id": "unexpected"})
    else:
        source.drop_collection(collection)
    before = list(target.affiliations.find())
    with pytest.raises(RuntimeError, match="affiliation snapshot"):
        Plugin(automatic_config())
    assert list(target.affiliations.find()) == before


def test_missing_entity_pointer_fails_even_when_joint_release_exists(monkeypatch):
    _, source = prepare_databases(monkeypatch)
    assert source.scienti_final_release_publications.find_one({"_id": "release_six"})
    with pytest.raises(RuntimeError, match="no published target"):
        Plugin(automatic_config())


def test_unchanged_workflow_picks_new_current_and_new_code_despite_success_log(monkeypatch, tmp_path):
    target, source = prepare_databases(monkeypatch)
    core = importlib.import_module("kahi.Kahi")
    plugin_module = importlib.import_module(Plugin.__module__)
    monkeypatch.setattr(core, "MongoClient", plugin_module.MongoClient)
    cfg = automatic_config()
    params = cfg.pop("minciencias_opendata_affiliations")
    cfg.update(log_database="kahi", log_collection="log", profile=False)
    path = tmp_path / "workflow.yml"
    path.write_text(yaml.safe_dump({"config": cfg, "workflow": {"minciencias_opendata_affiliations": params}}))
    original = path.read_bytes()
    publish(source, "v14")
    core.Kahi(str(path)).run()
    old_log = copy.deepcopy(target.log.find_one())

    publish(source, "v15")
    core.Kahi(str(path)).run()
    assert "Grupo v15" in {item["name"] for item in target.affiliations.find_one({"_id": "COL0000001"})["names"]}
    assert target.log.find_one()["config"] == old_log["config"]

    monkeypatch.setattr(Plugin, "implementation_fingerprint", staticmethod(lambda: "upgraded-code"))
    core.Kahi(str(path)).run()
    assert target.minciencias_opendata_affiliations_runs.count_documents({"status": "complete"}) == 3
    core.Kahi(str(path)).run()
    assert target.minciencias_opendata_affiliations_runs.count_documents({}) == 3
    assert path.read_bytes() == original
