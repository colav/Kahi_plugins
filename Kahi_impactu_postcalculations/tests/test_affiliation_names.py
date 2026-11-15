"""Copied affiliation names are rewritten with the Spanish name."""
import mongomock
import mongomock.collection
import pytest

from kahi_impactu_postcalculations.affiliation_names import (
    normalize_affiliation_names,
    spanish_name,
)

UDEA = {"_id": "udea", "types": [{"source": "ror", "type": "education"}], "names": [
    {"source": "ror", "name": "University of Antioquia", "lang": "en"},
    {"source": "ror", "name": "Universidad de Antioquia", "lang": "es"},
    {"source": "scienti", "name": "UNIVERSIDAD DE ANTIOQUIA", "lang": "es"}]}
ADELAIDE = {"_id": "adelaide", "types": [{"source": "ror", "type": "education"}], "names": [
    {"source": "ror", "name": "The University of Adelaide", "lang": "en"},
    {"source": "wikidata", "name": "Universidad de Adelaida", "lang": "es"}]}
ECOPETROL = {"_id": "eco", "types": [{"source": "ror", "type": "company"}], "names": [
    {"source": "openalex", "name": "Ecopetrol", "lang": ""},
    {"source": "scienti", "name": "ECOPETROL S. A.", "lang": "es"}]}
INSTITUTE = {"_id": "inst", "types": [{"source": "minciencias", "type": "institute"}],
             "names": [{"source": "minciencias", "name": "Instituto Humboldt", "lang": "es"}]}
GROUP = {"_id": "group", "types": [{"source": "minciencias", "type": "group"}],
         "names": [{"source": "minciencias", "name": "GRUPO DE FÍSICA", "lang": "es"}]}
SNIES = {"_id": "snies", "types": [], "names": [
    {"source": "openalex", "name": "Icesi University", "lang": ""},
    {"source": "snies", "name": "Universidad Icesi", "lang": "es"}]}


@pytest.fixture(autouse=True)
def bulk_write_without_sort(monkeypatch):
    builder = mongomock.collection.BulkOperationBuilder
    original = builder.add_update

    def add_update(self, *args, sort=None, **kwargs):
        return original(self, *args, **kwargs)

    monkeypatch.setattr(builder, "add_update", add_update)


@pytest.mark.parametrize("affiliation, expected", [
    (UDEA, "Universidad de Antioquia"),
    (SNIES, "Universidad Icesi"),
    (INSTITUTE, "Instituto Humboldt"),
    (ADELAIDE, None),      # only Wikidata has a Spanish name
    (ECOPETROL, None),     # only ScienTI has a Spanish name
    (GROUP, None),         # a group keeps the name each source gave it
    ({"names": []}, None),
])
def test_spanish_name(affiliation, expected):
    assert spanish_name(affiliation) == expected


def test_copied_names_become_spanish_everywhere_and_only_once():
    db = mongomock.MongoClient().kahi
    faculty = {"_id": "arts", "types": [{"type": "faculty"}],
               "names": [{"source": "staff", "name": "Facultad de Artes", "lang": "es"}],
               "relations": [{"id": "udea", "name": "University of Antioquia", "types": []},
                             {"id": "adelaide", "name": "The University of Adelaide"}]}
    db.affiliations.insert_many([UDEA, ADELAIDE, ECOPETROL, INSTITUTE, GROUP, SNIES, faculty])
    db.person.insert_one({"_id": "p", "affiliations": [
        {"id": "udea", "name": "UNIVERSIDAD DE ANTIOQUIA", "start_date": 1},
        {"id": "eco", "name": "Ecopetrol"}]})
    authors = [
        {"id": "p", "type": "advisor", "affiliations": [
            {"id": "snies", "name": "Icesi University", "types": [{"type": "education"}]},
            {"id": "group", "name": "Grupo de Física"},
            {"id": "", "name": "Sin identificar"}]},
        {"id": "", "affiliations": []},
        {"id": "q", "affiliations": [{"id": "udea", "name": "Universidad de Antioquia"},
                                     {"id": "inst", "name": "Humboldt Institute"}]},
    ]
    db.works.insert_one({"_id": "w", "authors": authors})
    db.patents.insert_one({"_id": "pt", "authors": [
        {"id": "p", "affiliations": [{"id": "udea", "name": "University of Antioquia"}]}]})

    report = normalize_affiliation_names(db)

    assert report == {
        "spanish_names": 3,
        "affiliations": {"documents": 1, "names": 1},
        "person": {"documents": 1, "names": 1},
        "works": {"documents": 1, "names": 2},
        "patents": {"documents": 1, "names": 1},
    }
    assert [item["name"] for item in db.affiliations.find_one({"_id": "arts"})["relations"]] == [
        "Universidad de Antioquia", "The University of Adelaide"]
    person = db.person.find_one({"_id": "p"})["affiliations"]
    assert person == [{"id": "udea", "name": "Universidad de Antioquia", "start_date": 1},
                      {"id": "eco", "name": "Ecopetrol"}]
    work = db.works.find_one({"_id": "w"})["authors"]
    assert work[0]["type"] == "advisor"
    assert work[0]["affiliations"] == [
        {"id": "snies", "name": "Universidad Icesi", "types": [{"type": "education"}]},
        {"id": "group", "name": "Grupo de Física"},
        {"id": "", "name": "Sin identificar"}]
    assert [item["name"] for item in work[2]["affiliations"]] == [
        "Universidad de Antioquia", "Instituto Humboldt"]
    assert db.patents.find_one()["authors"][0]["affiliations"][0]["name"] == \
        "Universidad de Antioquia"

    again = normalize_affiliation_names(db)
    assert all(again[key] == {"documents": 0, "names": 0}
               for key in ("affiliations", "person", "works", "patents"))
