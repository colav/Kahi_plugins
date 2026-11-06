import copy

import pytest

from kahi_minciencias_opendata_affiliations.Kahi_minciencias_opendata_affiliations import (
    split_institution_names,
)
from test_snapshot_ingestion import Plugin, config, group_snapshot, prepare_databases


COMPOUNDS = [
    "FUNDACION SANTA FE DE BOGOTA; UNIVERSIDAD DE LOS ANDES; UNIVERSIDAD EL BOSQUE",
    "FUNDACIÓN SANTA FÉ DE BOGOTÁ - FSFB; UNIVERSIDAD DE LOS ANDES - UNIANDES; UNIVERSIDAD EL BOSQUE",
]
INSTITUTIONS = [
    ("03ezapm74", "626100000888", "FUNDACION SANTA FE DE BOGOTA", "FUNDACIÓN SANTA FÉ DE BOGOTÁ - FSFB"),
    ("02mhbdp94", "005000000885", "UNIVERSIDAD DE LOS ANDES", "UNIVERSIDAD DE LOS ANDES - UNIANDES"),
    ("04m9gzq43", "004800000881", "UNIVERSIDAD EL BOSQUE", "UNIVERSIDAD EL BOSQUE"),
]


def anestesia_fixture(monkeypatch, *, old_snapshot):
    target, source = prepare_databases(monkeypatch)
    records = []
    for identifier, code, name, alias in INSTITUTIONS:
        entry = {"_id": identifier, **Plugin.empty_affiliation(None)}
        entry["names"] = [{"source": "ror", "name": name}]
        entry["external_ids"] = [{"source": "ror", "id": identifier}, {"source": "COD_INST", "id": code}]
        entry["types"] = [{"source": "ror", "type": "education"}]
        if identifier == "02mhbdp94":
            entry["campuses"] = [{"id": "snies-example", "external_ids": []}]
        target.affiliations.insert_one(entry)
        records.append({"COD_INST": code, "COD_INST_MACRO": code, "NME_INST": name,
                        "NME_INST_FILTRO": alias})
    source.institution.insert_many(records)
    cfg = config()
    cfg["minciencias_opendata_affiliations"]["scienti_databases"] = [{
        "database_url": "mongodb://source", "database_name": "dam",
        "institution_collection_name": "institution",
    }]
    snapshot = group_snapshot()
    snapshot["_id"] = "COL0034399"
    snapshot["external_ids"][0]["id"] = "COL0034399"
    snapshot["names"][0]["name"] = "ANESTESIA"
    names = [row[2] for row in INSTITUTIONS]
    if old_snapshot:
        names += COMPOUNDS + ["UNIVERSIDAD EL BOSQUE;FUNDACION SANTA FE DE BOGOTA;UNIVERSIDAD DE LOS ANDES"]
    snapshot["relations"] = [{"id": "", "name": name, "types": []} for name in names]
    source.affiliations_final.delete_many({})
    source.affiliations_final.insert_one(snapshot)
    plugin = Plugin(cfg)
    group = {"_id": snapshot["_id"], **plugin.empty_affiliation()}
    group["names"] = copy.deepcopy(snapshot["names"])
    group["external_ids"] = copy.deepcopy(snapshot["external_ids"])
    group["relations"] = [{"id": row[0], "name": row[2]} for row in INSTITUTIONS]
    for name in COMPOUNDS:
        provisional = {"_id": plugin.aval_institution_id(name), **plugin.empty_affiliation()}
        provisional["names"] = [{"source": "minciencias", "name": name}]
        provisional["types"] = [{"source": "minciencias", "type": "other"}]
        target.affiliations.insert_one(provisional)
        group["relations"].append({"id": provisional["_id"], "name": name})
    target.affiliations.insert_one(group)
    return plugin, target, source, cfg


@pytest.mark.parametrize("old_snapshot", [True, False])
def test_anestesia_reimport_replaces_only_compound_links_and_keeps_three_institutions(monkeypatch, old_snapshot):
    plugin, target, _, cfg = anestesia_fixture(monkeypatch, old_snapshot=old_snapshot)
    before = target.affiliations.find_one({"_id": "02mhbdp94"})["campuses"]
    summary = plugin.run()
    group = target.affiliations.find_one({"_id": "COL0034399"})
    assert {item["id"] for item in group["relations"]} == {row[0] for row in INSTITUTIONS}
    assert list(group) == ["_id", *plugin.empty_affiliation()]
    assert target.affiliations.find_one({"_id": "02mhbdp94"})["campuses"] == before
    assert target.affiliations.count_documents({"_id": {"$regex": "^IUA_"}}) == 2
    assert summary["institution_resolution"]["institutions_created"] == 0
    assert summary["institution_resolution"]["compound_declarations"] >= 2
    assert Plugin(cfg).run() == summary
    assert target.affiliations.find_one({"_id": "COL0034399"}) == group


def test_compound_names_are_not_matched_as_one_institution_or_used_as_candidates(monkeypatch):
    plugin, _, _, _ = anestesia_fixture(monkeypatch, old_snapshot=True)
    assert [plugin.aval_institution_id(name) for name in COMPOUNDS] == [
        "IUA_0021f9d4c4f5", "IUA_23140cee3190",
    ]
    assert plugin.find_matching_institution(COMPOUNDS[0]) is None
    assert plugin.institution_match_score({"names": [{"name": COMPOUNDS[0]}]},
                                          "fundacion santa fe de bogota") is None
    with pytest.raises(ValueError, match="must be split"):
        plugin.get_or_create_aval_institution(COMPOUNDS[0], plugin.collection, {})


def test_compound_catalog_label_cannot_alias_one_component(monkeypatch):
    plugin, _, _, _ = anestesia_fixture(monkeypatch, old_snapshot=True)
    record = {"COD_INST": "111111111111", "NME_INST": "UNIVERSIDAD DE LA SALLE; UNIVERSIDAD EJEMPLO"}
    index = plugin.index_scienti_catalog({record["COD_INST"]: record})
    assert plugin.normalize_inst_aval("UNIVERSIDAD DE LA SALLE") not in index


def test_partial_resolution_preserves_old_link_and_fails_the_run(monkeypatch):
    plugin, target, source, _ = anestesia_fixture(monkeypatch, old_snapshot=False)
    unresolved = "CEMENTOS ARGOS SA; INSTITUCIÓN DESCONOCIDA"
    provisional = {"_id": plugin.aval_institution_id(unresolved), **plugin.empty_affiliation()}
    provisional["names"] = [{"source": "minciencias", "name": unresolved}]
    provisional["types"] = [{"source": "minciencias", "type": "other"}]
    target.affiliations.insert_one(provisional)
    target.affiliations.update_one({"_id": "COL0034399"}, {"$push": {
        "relations": {"id": provisional["_id"], "name": unresolved},
    }})
    original = plugin.get_or_create_aval_institution
    plugin.get_or_create_aval_institution = lambda name, *args, **kwargs: (
        None if name == "INSTITUCIÓN DESCONOCIDA" else original(name, *args, **kwargs))
    with pytest.raises(RuntimeError, match="unresolved institution relations"):
        plugin.run()
    group = target.affiliations.find_one({"_id": "COL0034399"})
    assert provisional["_id"] in {item["id"] for item in group["relations"]}


def test_list_splitter_preserves_campus_parentheses_and_legal_punctuation():
    assert split_institution_names(" ; Universidad A (Unidad X; Unidad Y) | Instituto X, Y y Z - IXY; ") == [
        "Universidad A (Unidad X; Unidad Y)", "Instituto X, Y y Z - IXY",
    ]


def test_registry_enriched_iua_is_not_removed_as_an_unverified_provisional(monkeypatch):
    plugin, target, _, _ = anestesia_fixture(monkeypatch, old_snapshot=False)
    enriched_id = plugin.aval_institution_id(COMPOUNDS[0])
    target.affiliations.update_one({"_id": enriched_id}, {"$set": {
        "external_ids": [{"source": "COD_INST", "id": "000000000123"}],
    }})
    with pytest.raises(RuntimeError, match="unverified compound institution"):
        plugin.run()
    group = target.affiliations.find_one({"_id": "COL0034399"})
    assert enriched_id in {item["id"] for item in group["relations"]}


@pytest.mark.parametrize("fail_second", [True, False])
def test_alias_hash_collision_does_not_reuse_compound_or_remove_it_prematurely(monkeypatch, fail_second):
    plugin, target, _, _ = anestesia_fixture(monkeypatch, old_snapshot=False)
    name = "CORPORACIÓN UNIVERSITARIA MINUTO DE DIOS; INSTITUTO NUEVO"
    identifier = plugin.aval_institution_id(name)
    assert identifier == plugin.aval_institution_id("CORPORACIÓN UNIVERSITARIA MINUTO DE DIOS")
    old = {"_id": identifier, **plugin.empty_affiliation()}
    old["names"] = [{"source": "minciencias", "name": name}]
    old["types"] = [{"source": "minciencias", "type": "other"}]
    target.affiliations.insert_one(old)
    target.affiliations.update_one({"_id": "COL0034399"}, {"$push": {
        "relations": {"id": identifier, "name": name},
    }})
    plugin.find_matching_institution = lambda *args, **kwargs: None
    original = plugin.get_or_create_aval_institution
    if fail_second:
        plugin.get_or_create_aval_institution = lambda name, *args, **kwargs: (
            None if name == "INSTITUTO NUEVO" else original(name, *args, **kwargs))
        with pytest.raises(RuntimeError, match="unresolved institution relations"):
            plugin.run()
    else:
        plugin.run()
    group = target.affiliations.find_one({"_id": "COL0034399"})
    assert (identifier in {item["id"] for item in group["relations"]}) == fail_second
    individual = target.affiliations.find_one({"names.name": "CORPORACIÓN UNIVERSITARIA MINUTO DE DIOS"})
    assert individual["_id"] != identifier
    assert target.affiliations.find_one({"_id": identifier})["names"] == old["names"]
