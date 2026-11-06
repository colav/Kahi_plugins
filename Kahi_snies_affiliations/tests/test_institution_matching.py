import mongomock
import pytest

from kahi_snies_affiliations.Kahi_snies_affiliations import (
    Kahi_snies_affiliations,
    candidate_match_score,
    find_by_snies_code,
    has_different_snies_code,
    is_candidate_match,
    link_snies_hierarchy,
    normalize_nit,
    process_one,
    repair_snies_code_fusions,
)


@pytest.mark.parametrize(
    "value", [None, "", "1", "000000000", "0000000000", "000000000011",
              "000000010", "111111111", "123456789", "999999999", "123-4"])
def test_invalid_nits_are_not_identity_evidence(value):
    assert normalize_nit(value) == ""


def test_valid_nit_is_normalized_without_verification_digit():
    assert normalize_nit("901.167.074-2") == "901167074"


@pytest.mark.parametrize("canonical, unrelated", [
    ("Fundación Universitaria San Alfonso",
     "Fundación Universitaria San Mateo Educación Superior"),
    ("Institución Universitaria de Envigado",
     "Institución Universitaria de Colombia"),
])
def test_generic_legal_terms_do_not_match_unrelated_institutions(
        canonical, unrelated):
    match = candidate_match_score(
        {"names": [{"source": "ror", "name": canonical}]},
        unrelated.lower(),
    )

    assert not is_candidate_match(match, unrelated.lower())


def test_name_match_rejects_a_different_existing_snies_code():
    candidate = {
        "external_ids": [
            {"provenance": "snies", "source": "snies", "id": "1001"},
        ],
    }

    assert has_different_snies_code(candidate, "2002")
    assert not has_different_snies_code(candidate, "1001")


def test_snies_lookup_ignores_codes_copied_by_scienti():
    collection = mongomock.MongoClient().db.affiliations
    collection.insert_many([
        {
            "_id": "unit",
            "external_ids": [{
                "provenance": "scienti", "source": "snies", "id": "1215"}],
        },
        {
            "_id": "canonical",
            "external_ids": [{
                "provenance": "snies", "source": "snies", "id": "1215"}],
        },
    ])

    assert find_by_snies_code(collection, "1215")["_id"] == "canonical"


def test_snies_sectional_is_embedded_in_principal_idempotently():
    plugin = Kahi_snies_affiliations.__new__(Kahi_snies_affiliations)
    collection = mongomock.MongoClient().db.affiliations
    principal = plugin.empty_affiliation()
    principal["_id"] = "university"
    principal["names"].append({
        "source": "snies", "name": "Universidad de Cundinamarca", "lang": "es"})
    principal["types"].append({
        "provenance": "snies", "source": "type", "type": "public"})
    principal["addresses"].append({"country": "Colombia"})
    collection.insert_one(principal)
    principal_record = {
        "CÓDIGO_IES_PADRE": "1214",
        "CÓDIGO_INSTITUCIÓN": "1214",
        "NOMBRE_INSTITUCIÓN": "UNIVERSIDAD DE CUNDINAMARCA",
        "PRINCIPAL_SECCIONAL": "Principal",
        "MUNICIPIO_DOMICILIO": "Fusagasugá",
    }
    sectional = {
        "CÓDIGO_IES_PADRE": "1214",
        "CÓDIGO_INSTITUCIÓN": "1215",
        "NOMBRE_INSTITUCIÓN": "UNIVERSIDAD DE CUNDINAMARCA",
        "PRINCIPAL_SECCIONAL": "Seccional",
        "MUNICIPIO_DOMICILIO": "Girardot",
    }

    process_one(principal_record, collection, plugin.empty_affiliation())
    process_one(sectional, collection, plugin.empty_affiliation())
    assert link_snies_hierarchy(collection, [sectional]) == 1
    assert link_snies_hierarchy(collection, [sectional]) == 0

    assert collection.find_one({"_id": "snies_1215"}) is None
    root = collection.find_one({"_id": "university"})
    assert [item["id"] for item in root["external_ids"]] == ["1214"]
    assert root["campuses"] == [{
        "id": "snies_1215",
        "name": "Universidad de Cundinamarca",
        "types": [{
            "provenance": "snies", "source": "snies", "type": "campus",
        }],
        "external_ids": [{
            "provenance": "snies", "source": "snies", "id": "1215",
        }],
        "addresses": [{
            "lat": "", "lng": "", "postcode": "", "state": "",
            "city": "Girardot", "country": "Colombia", "country_code": "CO",
        }],
        "status": [],
    }]


@pytest.mark.parametrize("affiliation_id, canonical, retained, unrelated", [
    ("05avmn224", "Fundación Universitaria San Alfonso", "111", "222"),
    ("05jj6ma85", "Institución Universitaria de Envigado", "333", "444"),
])
def test_repair_removes_unrelated_snies_metadata(
        affiliation_id, canonical, retained, unrelated):
    collection = mongomock.MongoClient().db.affiliations
    collection.insert_one({
        "_id": affiliation_id,
        "names": [
            {"source": "ror", "name": canonical},
            {"source": "snies", "name": canonical},
            {"source": "snies", "name": "Unrelated"},
        ],
        "types": [{"provenance": "ror", "type": "education"}],
        "external_urls": [],
        "external_ids": [
            {"provenance": "snies", "source": "snies", "id": retained},
            {"provenance": "snies", "source": "snies", "id": unrelated},
        ],
        "ranking": [],
        "updated": [{"source": "snies", "time": 1}],
    })
    records = [
        {"CÓDIGO_INSTITUCIÓN": retained, "NOMBRE_INSTITUCIÓN": canonical},
        {
            "CÓDIGO_INSTITUCIÓN": unrelated,
            "NOMBRE_INSTITUCIÓN": (
                "Fundación Universitaria San Mateo Educación Superior"
                if affiliation_id == "05avmn224"
                else "Institución Universitaria de Colombia"
            ),
        },
    ]

    repaired = repair_snies_code_fusions(collection, records)
    result = collection.find_one({"_id": affiliation_id})

    assert repaired == {affiliation_id}
    assert {item["name"] for item in result["names"]} == {canonical}
    assert {
        item["id"] for item in result["external_ids"]
        if item.get("source") == "snies"
    } == {retained}
