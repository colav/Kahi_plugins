import copy

import pytest

from kahi_minciencias_opendata_affiliations.Kahi_minciencias_opendata_affiliations import is_strict_institution_match
from test_snapshot_ingestion import Plugin, config, group_snapshot, prepare_databases


def test_country_only_cannot_match_a_foundation_that_contains_the_country_name():
    plugin = Plugin.__new__(Plugin)
    foundation = {"names": [{"name": "FUNDACIÓN CONTRUCCIÓN COLOMBIA"}]}
    assert plugin.institution_match_score(foundation, "colombia") is None
    assert plugin.find_matching_institution("COLOMBIA") is None
    evidence = {}
    assert plugin.get_or_create_aval_institution("COLOMBIA", None, {}, resolution=evidence) is None
    assert evidence == {"outcome": "excluded", "reason": "country_only_fragment"}
    assert is_strict_institution_match(plugin.institution_match_score(
        foundation, "fundacion contruccion colombia"))


@pytest.mark.parametrize("mode,compound", [("snapshot", False), ("snapshot", True), ("legacy", True)])
def test_country_fragment_is_excluded_without_losing_real_institutions(monkeypatch, mode, compound):
    target, source = prepare_databases(monkeypatch)
    snapshot = group_snapshot()
    snapshot["relations"] = ([{"id": "", "name": "COLOMBIA; CEMENTOS ARGOS SA", "types": []}]
                             if compound else [{"id": "", "name": label, "types": []}
                                                   for label in ("COLOMBIA", "CEMENTOS ARGOS SA")])
    source.affiliations_final.replace_one({}, snapshot)
    cfg = config(mode)
    if mode == "legacy":
        source.gruplac_groups_data.insert_one({
            "cod_grupo_gr": snapshot["_id"], "inst_aval": "COLOMBIA | CEMENTOS ARGOS SA",
            "nme_grupo_gr": "Grupo Uno",
        })
    plugin = Plugin(cfg)
    country_id = plugin.aval_institution_id("COLOMBIA")
    country = {"_id": country_id, **plugin.empty_affiliation()}
    country["names"] = [{"source": "minciencias", "name": "COLOMBIA"}]
    country["external_ids"] = [{"source": "minciencias", "id": country_id}]
    target.affiliations.insert_one(country)
    entry = {"_id": snapshot["_id"], **plugin.empty_affiliation()}
    entry["names"] = copy.deepcopy(snapshot["names"])
    entry["external_ids"] = copy.deepcopy(snapshot["external_ids"])
    entry["types"] = [{"source": "minciencias", "type": "group"}]
    entry["relations"] = [{"id": country_id, "name": "COLOMBIA"}]
    target.affiliations.insert_one(entry)
    summary = plugin.run()
    group = target.affiliations.find_one({"_id": snapshot["_id"]})
    assert [r["id"] for r in group["relations"]] == ["canonical-argos"]
    assert target.affiliations.find_one({"_id": country_id}) == country
    if mode == "snapshot":
        assert summary["institution_resolution"]["relations_excluded_non_institution"] >= 1
        assert summary["institution_resolution"]["compound_declarations_unresolved"] == 0
        assert list(group) == ["_id", *plugin.empty_affiliation()]


def test_registered_country_identity_is_not_automatically_removed(monkeypatch):
    target, _ = prepare_databases(monkeypatch)
    plugin = Plugin(config())
    country_id = plugin.aval_institution_id("COLOMBIA")
    institution = {"_id": country_id, "names": [{"name": "COLOMBIA"}],
                   "external_ids": [{"source": "COD_INST", "id": "unverified"}]}
    target.affiliations.insert_one(institution)
    entry = {"_id": "COL0000001", "relations": [{"id": country_id, "name": "COLOMBIA"}]}
    with pytest.raises(RuntimeError, match="unverified institutional identity"):
        plugin.remove_non_institution_relations({}, entry, target.affiliations)
    assert len(entry["relations"]) == 1
    assert target.affiliations.find_one({"_id": country_id}) == institution
