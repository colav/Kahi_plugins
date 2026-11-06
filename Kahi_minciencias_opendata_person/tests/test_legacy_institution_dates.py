"""Product dates must not authorize institutional affiliation years."""

from copy import deepcopy
import importlib

import mongomock
import pytest


legacy = importlib.import_module("kahi_minciencias_opendata_person.legacy")
ciarp = importlib.import_module("kahi_ciarp_works.process_one")


@pytest.fixture
def source_data():
    client = mongomock.MongoClient()
    client.kahi.affiliations.insert_many([
        {"_id": "institution", "names": [{"name": "Universidad", "lang": "es"}],
         "types": [{"type": "education"}]},
        {"_id": "group", "external_ids": [{"id": "COL0000001"}],
         "names": [{"name": "Grupo", "lang": "es"}],
         "types": [{"type": "group"}],
         "relations": [{"id": "institution", "name": "Universidad",
                        "types": [{"type": "education"}]}]},
    ])
    client.dam.production.insert_one({
        "id_persona_pd": "author", "id_producto_pd": "ART-0000000001-1",
        "ano_convo": 2024, "cod_grupo_gr": "COL0000001",
        "fcreacion_pd": "01/01/2020",
    })
    return client


def enrich(client, entry):
    legacy.process_info_from_works(
        client.kahi, {"id_persona_pr": "author"}, entry,
        client.dam.production, client.dam.html,
    )


def test_product_keeps_relation_without_institutional_interval(source_data):
    entry = {"affiliations": [], "related_works": []}
    enrich(source_data, entry)
    affiliations = {aff["id"]: aff for aff in entry["affiliations"]}

    assert affiliations["group"]["start_date"] == legacy.check_date_format("01/01/2020")
    institution = affiliations["institution"]
    assert institution["start_date"] is None
    assert institution["end_date"] == -1
    assert institution["years"] == []
    parent = source_data.kahi.affiliations.find_one({"_id": "institution"})
    for year in (2020, 2021, 2025):
        assert not ciarp.affiliation_covers_year(institution, year, current_year=2026)
        assert ciarp.select_work_affiliations(
            entry, parent, year, source_data.kahi, current_year=2026,
        ) == []


@pytest.mark.parametrize("history", [
    {"start_date": 1577836800, "end_date": -1, "years": [2020, 2024]},
    {"start_date": 1577836800, "end_date": 1609372800},
    {"start_date": None, "end_date": -1, "years": []},
])
def test_existing_institution_history_is_preserved(source_data, history):
    institution = {"id": "institution", "name": "Universidad", **history}
    entry = {"affiliations": [deepcopy(institution)], "related_works": []}
    enrich(source_data, entry)
    institutions = [aff for aff in entry["affiliations"] if aff["id"] == "institution"]
    assert institutions == [institution]
