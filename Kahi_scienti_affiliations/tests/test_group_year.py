from copy import deepcopy
from importlib import import_module

import mongomock
import pytest


module = import_module("kahi_scienti_affiliations.Kahi_scienti_affiliations")
Plugin = module.Kahi_scienti_affiliations


def source_group(code="COL0000001", year="2004"):
    return {
        "COD_ID_GRUPO": code, "NRO_ID_GRUPO": code, "NME_GRUPO": code,
        "ANO_FORMACAO": year, "STA_ELIMINADO": "F", "knowledge_area": [{}],
    }


def prepare(monkeypatch, groups, existing=None):
    client = mongomock.MongoClient()
    instance = Plugin.__new__(Plugin)
    instance.collection = client.target.affiliations
    instance.verbose = 0
    instance.group_parent_institutions = lambda *_: []
    instance.extract_subject = lambda *_: []
    client.source.product.insert_one({"group": groups})
    if existing is not None:
        instance.collection.insert_one(existing)
    monkeypatch.setattr(module, "MongoClient", lambda *_: client)
    config = {"database_url": "mock", "database_name": "source", "collection_name": "product"}
    return instance, config


def test_matching_group_year_is_stored_without_birthdate_or_month(monkeypatch):
    plugin, config = prepare(monkeypatch, [
        source_group("COL0000002", "2011"), source_group(),
    ])
    plugin.process_scienti_groups(config)
    group = plugin.collection.find_one({"_id": "COL0000001"})
    assert group["year_established"] == 2004
    assert "birthdate" not in group
    assert group["campuses"] == [] and list(group)[-1] == "campuses"
    before = deepcopy(group)
    plugin.process_scienti_groups(config)
    assert plugin.collection.find_one({"_id": group["_id"]}) == before


@pytest.mark.parametrize("existing_year,expected", [(None, 2004), ("", 2004), (-1, 2004), (1989, 1989)])
def test_existing_group_year_is_only_filled_when_missing(monkeypatch, existing_year, expected):
    existing = {"_id": "COL0000001", **Plugin.empty_affiliation(None)}
    existing["external_ids"] = [{"source": "minciencias", "id": existing["_id"]}]
    existing["year_established"] = existing_year
    existing["campuses"] = [{"id": "existing-campus"}]
    plugin, config = prepare(monkeypatch, [source_group()], existing)
    plugin.process_scienti_groups(config)
    actual = plugin.collection.find_one({"_id": existing["_id"]})
    assert actual == {**existing, "year_established": expected}
    assert list(actual)[-1] == "campuses"


@pytest.mark.parametrize("year", [None, "", "4", "3025", "2004.5", True, "0000"])
def test_invalid_group_year_stays_unknown(monkeypatch, year):
    plugin, config = prepare(monkeypatch, [source_group(year=year)])
    plugin.process_scienti_groups(config)
    group = plugin.collection.find_one({"_id": "COL0000001"})
    assert group["year_established"] is None
    assert "birthdate" not in group
