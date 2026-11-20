import copy

import pytest

from test_snapshot_ingestion import Plugin, config, prepare_databases


ROOT_CODE = "625100000880"
CAMPUS_CODE = "000000008424"
ROOT_ID = "059yx9a68"


def campus_fixture(monkeypatch):
    target, source = prepare_databases(monkeypatch)
    principal = {"_id": ROOT_ID, **Plugin.empty_affiliation(None)}
    principal["names"] = [{"source": "ror", "name": "Universidad Nacional de Colombia"}]
    principal["types"] = [{"source": "ror", "type": "education"}]
    principal["external_ids"] = [
        {"source": "ror", "id": ROOT_ID, "provenance": "ror"},
        {"source": "snies", "id": "1101", "provenance": "snies"},
    ]
    principal["campuses"] = [{
        "id": "snies_1102", "name": "Universidad Nacional de Colombia - Medellín",
        "external_ids": [{"source": "snies", "id": "1102", "provenance": "snies"}],
    }]
    target.affiliations.insert_one(principal)
    source.institution.insert_many([
        {"COD_INST": ROOT_CODE, "COD_INST_MACRO": ROOT_CODE,
         "NME_INST": "UNIVERSIDAD NACIONAL DE COLOMBIA", "COD_IES": "1101",
         "TXT_CIUDAD_INST": "Bogotá"},
        {"COD_INST": CAMPUS_CODE, "COD_INST_MACRO": ROOT_CODE,
         "NME_INST": "UNIVERSIDAD NACIONAL DE COLOMBIA SEDE MEDELLÍN",
         "COD_IES": "1102", "TXT_CIUDAD_INST": "Medellín"},
    ])
    source.affiliations_final.update_one({"_id": "COL0000001"}, {"$set": {
        "relations": [{"id": "", "name": "UNIVERSIDAD NACIONAL DE COLOMBIA SEDE MEDELLÍN"}],
    }})
    cfg = config()
    cfg["minciencias_opendata_affiliations"]["scienti_databases"] = [{
        "database_url": "mongodb://source", "database_name": "dam",
        "institution_collection_name": "institution",
    }]
    return target, source, cfg, principal


def test_snapshot_matches_legacy_campus_policy_and_preserves_schema(monkeypatch):
    snapshot_target, _, snapshot_config, principal = campus_fixture(monkeypatch)
    summary = Plugin(snapshot_config).run()
    snapshot_group = snapshot_target.affiliations.find_one({"_id": "COL0000001"})
    assert {item["id"] for item in snapshot_group["relations"]} == {ROOT_ID}
    assert summary["institution_resolution"]["institutions_created_scienti"] == 1
    assert summary["institution_resolution"]["institutions_reused"] == 1
    assert snapshot_target.affiliations.find_one({"_id": ROOT_ID})["campuses"] == principal["campuses"]
    for document in snapshot_target.affiliations.find():
        expected = ["_id", *Plugin.empty_affiliation(None)]
        assert list(document) == expected

    legacy_target, legacy_source, legacy_config, _ = campus_fixture(monkeypatch)
    legacy_source.gruplac_groups_data.insert_one({
        "cod_grupo_gr": "COL0000001", "nme_grupo_gr": "Grupo Uno",
        "inst_aval": "UNIVERSIDAD NACIONAL DE COLOMBIA SEDE MEDELLÍN",
        "nme_departamento_gr": "Antioquia", "nme_municipio_gr": "Medellín",
        "ano_convo": "2021-02-25T00:00:00.000",
    })
    legacy_config["minciencias_opendata_affiliations"]["source_mode"] = "legacy"
    Plugin(legacy_config).run()
    legacy_group = legacy_target.affiliations.find_one({"_id": "COL0000001"})
    assert legacy_group["relations"] == snapshot_group["relations"]
    assert set(legacy_target.affiliations.distinct("_id")) == set(snapshot_target.affiliations.distinct("_id"))


def test_replaces_proven_obsolete_campus_and_iua_links_only(monkeypatch):
    target, _, cfg, _ = campus_fixture(monkeypatch)
    plugin = Plugin(cfg)
    snapshot = plugin.openadata_collection.find_one({"_id": "COL0000001"})
    campus = plugin.materialize_scienti_institution(plugin.scienti_catalog[CAMPUS_CODE])
    entry = {"_id": "COL0000001", **plugin.empty_affiliation()}
    entry["external_ids"] = snapshot["external_ids"]
    entry["relations"] = [
        {"id": campus["_id"], "name": "Sede"},
        {"id": plugin.aval_institution_id(snapshot["relations"][0]["name"]), "name": "Anterior"},
        {"id": "canonical-argos", "name": "CEMENTOS ARGOS SA"},
    ]
    target.affiliations.insert_one(entry)
    plugin.run()
    group = target.affiliations.find_one({"_id": entry["_id"]})
    assert {item["id"] for item in group["relations"]} == {ROOT_ID, "canonical-argos"}
    assert target.affiliations.find_one({"_id": CAMPUS_CODE}) is not None


def test_keeps_authoritative_campus_and_parent_when_both_are_authoritative(monkeypatch):
    target, _, cfg, _ = campus_fixture(monkeypatch)
    campus = {"_id": CAMPUS_CODE, **Plugin.empty_affiliation(None)}
    campus["names"] = [{"source": "snies", "name": "Universidad Nacional de Colombia Sede Medellín"}]
    campus["types"] = [{"source": "snies", "type": "education"}]
    campus["external_ids"] = [
        {"source": "COD_INST", "id": CAMPUS_CODE, "provenance": "scienti"},
        {"source": "snies", "id": "1102", "provenance": "snies"},
    ]
    target.affiliations.insert_one(campus)
    Plugin(cfg).run()
    group = target.affiliations.find_one({"_id": "COL0000001"})
    assert {item["id"] for item in group["relations"]} == {ROOT_ID, CAMPUS_CODE}


def test_resume_keeps_creation_counters_after_uncheckpointed_group(monkeypatch):
    target, _, cfg, _ = campus_fixture(monkeypatch)
    plugin = Plugin(cfg)
    original = plugin.process_snapshot_one

    def fail_after_insert(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("failure after materialization")

    plugin.process_snapshot_one = fail_after_insert
    with pytest.raises(RuntimeError, match="failure after materialization"):
        plugin.run()
    summary = Plugin(cfg).run()
    repeated = Plugin(cfg).run()
    assert repeated == summary
    assert summary["institution_resolution"]["institutions_created"] == 1
    assert summary["institution_resolution"]["relations_processed"] == 1
    assert target.affiliations.count_documents({"_id": CAMPUS_CODE}) == 1


def test_parallel_groups_count_shared_institutions_once(monkeypatch):
    target, source, cfg, _ = campus_fixture(monkeypatch)
    second = source.affiliations_final.find_one({"_id": "COL0000001"})
    second["_id"] = "COL0000002"
    second["external_ids"][0]["id"] = "COL0000002"
    source.affiliations_final.insert_one(second)
    source.scienti_final_release_audits.update_one({}, {"$set": {"evidence.affiliations.documents": 2}})
    cfg["minciencias_opendata_affiliations"]["num_jobs"] = 2
    summary = Plugin(cfg).run()
    audit = summary["institution_resolution"]
    assert audit["institutions"] == 2
    assert audit["institutions_created"] == 1
    assert audit["relations_catalog"] == 2
    assert audit["missing_relation_targets"] == 0
    assert target.affiliations.count_documents({"_id": CAMPUS_CODE}) == 1


def test_resume_after_entity_write_before_audit_confirmation(monkeypatch):
    target, _, cfg, _ = campus_fixture(monkeypatch)
    plugin = Plugin(cfg)
    record = plugin.record_institution_resolution

    def interrupt_confirmation(institution, **kwargs):
        if kwargs.get("code") == CAMPUS_CODE and kwargs.get("committed", True):
            raise RuntimeError("interruption before audit confirmation")
        record(institution, **kwargs)

    plugin.record_institution_resolution = interrupt_confirmation
    with pytest.raises(RuntimeError, match="before audit confirmation"):
        plugin.run()
    assert target.affiliations.find_one({"_id": CAMPUS_CODE}) is not None
    pending = target.minciencias_opendata_affiliations_institution_resolutions.find_one({
        "institution_id": CAMPUS_CODE,
    })
    assert pending["created"] is True
    assert pending["committed"] is False
    audit = Plugin(cfg).run()["institution_resolution"]
    assert audit["institutions_created_scienti"] == 1
    assert audit["pending_institutions"] == 0


def test_catalog_and_code_changes_automatically_reprocess_the_same_snapshot(monkeypatch):
    target, source, cfg, _ = campus_fixture(monkeypatch)
    first = Plugin(cfg).run()
    source.institution.update_one({"COD_INST": CAMPUS_CODE}, {"$set": {"SGL_INST": "UNAL-M"}})
    second = Plugin(cfg).run()
    monkeypatch.setattr(Plugin, "implementation_fingerprint", staticmethod(lambda: "new-code"))
    third = Plugin(cfg).run()
    assert first["catalog_fingerprint"] != second["catalog_fingerprint"]
    assert second["updated"] == third["updated"] == 1
    assert second["implementation_sha256"] != third["implementation_sha256"]
    assert target.minciencias_opendata_affiliations_runs.count_documents({"status": "complete"}) == 3
    assert target.affiliations.count_documents({"_id": CAMPUS_CODE}) == 1


def test_obsolete_workflow_revision_cannot_override_implementation_identity(monkeypatch):
    target, _, cfg, _ = campus_fixture(monkeypatch)
    first = Plugin(cfg).run()
    revised = copy.deepcopy(cfg)
    revised["minciencias_opendata_affiliations"]["import_revision"] = "forgotten-old-value"
    assert Plugin(revised).run() == first
    assert target.minciencias_opendata_affiliations_runs.count_documents({}) == 1


def test_catalog_ties_are_independent_of_source_cursor_order(monkeypatch):
    _, source, cfg, _ = campus_fixture(monkeypatch)
    duplicate = source.institution.find_one({"COD_INST": CAMPUS_CODE})
    duplicate.pop("_id")
    duplicate["TXT_CIUDAD_INST"] = "Otra ciudad"
    source.institution.insert_one(duplicate)
    first = Plugin(cfg)
    records = list(source.institution.find({}, {"_id": 0}))
    source.institution.delete_many({})
    source.institution.insert_many(list(reversed(records)))
    second = Plugin(cfg)
    assert first.scienti_catalog == second.scienti_catalog
    assert first.catalog_fingerprint == second.catalog_fingerprint
    assert first.run_id == second.run_id


def test_unresolved_relation_prevents_successful_import(monkeypatch):
    target, source = prepare_databases(monkeypatch)
    source.affiliations_final.update_one({}, {"$set": {"relations": [{"id": "missing", "name": ""}]}})
    with pytest.raises(RuntimeError, match="unresolved institution relations"):
        Plugin(config()).run()
    assert target.minciencias_opendata_affiliations_runs.find_one()["status"] == "failed"
    record = target.minciencias_opendata_affiliations_institution_resolutions.find_one({"kind": "relation"})
    assert record["outcome"] == "unresolved"
    assert record["resolved_ids"] == []
