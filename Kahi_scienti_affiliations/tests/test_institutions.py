from copy import deepcopy

import mongomock
import pytest

from kahi_scienti_affiliations.Kahi_scienti_affiliations import (
    Kahi_scienti_affiliations,
    apply_scienti_institution_data,
    institution_name_match,
    normalize_nit,
    normalize_url,
    scienti_address,
    scienti_external_ids,
)


def institution_record():
    return {
        "COD_INST": "000000009892",
        "ID_INSTITUCION": 481,
        "NME_INST": "CEMENTOS ARGOS S.A",
        "NME_INST_FILTRO": "CEMENTOS ARGOS",
        "SGL_INST": "Argos",
        "SGL_PAIS": "1",
        "SGL_DEPARTAMENTO": "31",
        "TXT_CIUDAD_INST": "Medellín",
        "TXT_NIT": "890.100.251",
        "TXT_DIGITO_VERIFICADOR": "0",
        "COD_IES": "1201",
        "URL_HOME_PAGE": "argos.com.co",
        "DTA_CONSTITUCION": "1944-08-14 00:00:00",
        "DTA_ACTUALIZACION": "2021-08-17 16:28:47",
        "city": [{
            "SGL_PAIS": "COL",
            "SGL_DEPARTAMENTO": "CU",
            "TXT_NME_MUNICIPIO": "TOCAIMA",
            "department": [{
                "TXT_NME_DEPARTAMENTO": "CUNDINAMARCA",
                "country": [{
                    "SGL_PAIS": "COL",
                    "TXT_NME_PAIS": "Colombia",
                    "SGL_PAIS_ISO_2": "CO",
                }],
            }],
        }],
    }


def test_address_prefers_explicit_city_over_conflicting_nested_city():
    address = scienti_address(institution_record())

    assert address["city"] == "Medellín"
    assert address["state"] == ""
    assert address["country"] == "Colombia"
    assert address["country_code"] == "CO"


def test_address_rejects_cross_country_nested_city():
    reg = {
        "SGL_PAIS": "COL",
        "city": [{
            "SGL_PAIS": "BRA",
            "TXT_NME_MUNICIPIO": "Guaranésia",
            "department": [{
                "TXT_NME_DEPARTAMENTO": "Minas Gerais",
                "country": [{
                    "SGL_PAIS": "BRA",
                    "TXT_NME_PAIS": "Brasil",
                    "SGL_PAIS_ISO_2": "BR",
                }],
            }],
        }],
    }

    address = scienti_address(reg)

    assert address["city"] == ""
    assert address["state"] == ""
    assert address["country"] == "Colombia"


def test_scienti_metadata_uses_only_existing_affiliation_fields():
    entry = apply_scienti_institution_data(
        {"year_established": None}, institution_record())

    assert entry["names"] == [{
        "source": "scienti", "name": "CEMENTOS ARGOS S.A", "lang": "es"}]
    assert entry["abbreviations"] == ["Argos"]
    assert entry["year_established"] == 1944
    assert entry["external_urls"] == [{
        "provenance": "scienti",
        "source": "site",
        "url": "https://argos.com.co",
    }]
    assert {(item["source"], item["id"]) for item in entry["external_ids"]} == {
        ("COD_INST", "000000009892"),
        ("ID_INSTITUCION", "481"),
        ("nit", "890100251"),
        ("snies", "1201"),
    }
    assert entry["types"] == [{
        "provenance": "scienti", "source": "scienti", "type": "education"}]
    assert set(entry) >= {
        "names", "abbreviations", "year_established", "addresses",
        "external_urls", "external_ids", "types", "updated",
    }


def test_scienti_enrichment_preserves_existing_year_and_geography():
    entry = {
        "year_established": 1950,
        "addresses": [{
            "lat": 6.25,
            "lng": -75.56,
            "postcode": "050021",
            "state": "Antioquia",
            "city": "Medellín",
            "country": "Colombia",
            "country_code": "CO",
        }],
    }
    original_address = deepcopy(entry["addresses"])

    apply_scienti_institution_data(entry, institution_record())

    assert entry["year_established"] == 1950
    assert entry["addresses"] == original_address


def test_placeholder_identifiers_and_urls_are_rejected():
    reg = institution_record()
    reg["TXT_NIT"] = "000000000"

    assert normalize_nit(reg) == ""
    assert normalize_url("Pendiente") == ""
    assert normalize_url("contacto@example.org") == ""


def test_scienti_external_ids_can_omit_parent_snies_code():
    identifiers = scienti_external_ids(
        institution_record(), include_snies_id=False)

    assert not any(item["source"] == "snies" for item in identifiers)


def canonical_snies_affiliation(plugin):
    entry = plugin.empty_affiliation()
    entry["_id"] = "canonical"
    entry["names"].append({
        "source": "snies", "name": "Universidad Matriz", "lang": "es"})
    entry["types"].append({
        "provenance": "snies", "source": "type", "type": "public"})
    entry["external_ids"].append({
        "provenance": "snies", "source": "snies", "id": "1201"})
    plugin.collection.insert_one(entry)
    return plugin.collection.find_one({"_id": "canonical"})


def test_scienti_unit_keeps_own_ids_and_relates_to_snies_canonical():
    plugin = Kahi_scienti_affiliations.__new__(Kahi_scienti_affiliations)
    plugin.collection = mongomock.MongoClient().db.affiliations
    canonical_snies_affiliation(plugin)
    reg = institution_record()
    reg.update({
        "COD_INST": "unit-code",
        "ID_INSTITUCION": "unit-internal",
        "COD_IES": "1201",
        "NME_INST": "Sede Regional",
    })

    affiliation_id, inserted = plugin.save_scienti_institution(None, reg)
    result = plugin.collection.find_one({"_id": affiliation_id})

    assert inserted
    assert affiliation_id == "unit-code"
    assert {(item["source"], item["id"]) for item in result["external_ids"]} >= {
        ("COD_INST", "unit-code"),
        ("ID_INSTITUCION", "unit-internal"),
    }
    assert not any(
        item["source"] == "snies" and item["id"] == "1201"
        for item in result["external_ids"])
    assert [item["id"] for item in result["relations"]] == ["canonical"]


def test_scienti_update_removes_copied_snies_id_idempotently():
    plugin = Kahi_scienti_affiliations.__new__(Kahi_scienti_affiliations)
    plugin.collection = mongomock.MongoClient().db.affiliations
    canonical_snies_affiliation(plugin)
    unit = plugin.empty_affiliation()
    unit["_id"] = "unit"
    unit["external_ids"].append({
        "provenance": "scienti", "source": "snies", "id": "1201"})
    plugin.collection.insert_one(unit)
    reg = institution_record()
    reg.update({"COD_INST": "unit-code", "COD_IES": "1201"})

    plugin.save_scienti_institution(unit, reg)
    plugin.save_scienti_institution(
        plugin.collection.find_one({"_id": "unit"}), reg)
    result = plugin.collection.find_one({"_id": "unit"})

    assert not any(
        item["source"] == "snies" and item["id"] == "1201"
        for item in result["external_ids"])
    assert [item["id"] for item in result["relations"]] == ["canonical"]


def test_scienti_campus_code_resolves_to_principal_without_enriching_it():
    plugin = Kahi_scienti_affiliations.__new__(Kahi_scienti_affiliations)
    plugin.collection = mongomock.MongoClient().db.affiliations
    canonical = canonical_snies_affiliation(plugin)
    canonical["campuses"] = [{
        "id": "snies_1215",
        "name": "Universidad Matriz",
        "external_ids": [{
            "provenance": "snies", "source": "snies", "id": "1215",
        }],
        "addresses": [{"city": "Girardot"}],
    }]
    plugin.collection.replace_one({"_id": canonical["_id"]}, canonical)
    reg = institution_record()
    reg.update({
        "COD_INST": "campus-code", "COD_IES": "1215",
        "NME_INST": "Universidad Matriz - Sede Girardot",
    })

    matched = plugin.find_scienti_institution(reg, set(), {"1215"})
    affiliation_id, inserted = plugin.save_scienti_institution(matched, reg)
    result = plugin.collection.find_one({"_id": "canonical"})

    assert (affiliation_id, inserted) == ("canonical", False)
    assert [item["name"] for item in result["names"]] == ["Universidad Matriz"]
    assert not any(item.get("id") == "campus-code"
                   for item in result["external_ids"])
    assert plugin.collection.find_one({"_id": "campus-code"}) is None


@pytest.mark.parametrize("canonical, unrelated", [
    ("Fundación Universitaria San Alfonso",
     "FUNDACION UNIVERSITARIA SAN MATEO EDUCACION SUPERIOR"),
    ("Institución Universitaria de Envigado",
     "INSTITUCION UNIVERSITARIA DE COLOMBIA"),
    ("Universidad del Norte", "Universidad Tecnica Del Norte. Ibarra. Ec"),
    ("Universidad de Cartagena", "Universidad Politecnica De Cartagena"),
    ("Corporación Politécnico Colombo Andino", "Politécnico Andino"),
    ("Universidad Autónoma de Colombia",
     "Universidad Autonoma Indigena Intercultural De Colombia"),
    ("Universidad Central", "Universidad Central Del Ecuador"),
    ("Universidad de los Llanos",
     "Universidad Experimental De Los Llanos Ezequiel Mora"),
])
def test_generic_legal_terms_do_not_match_unrelated_institutions(
        canonical, unrelated):
    match = institution_name_match(
        {"names": [{"source": "ror", "name": canonical}]},
        [unrelated],
    )

    assert not match["exact"]
    assert not match["strict"]


@pytest.mark.parametrize("affiliation_id, canonical, retained, separated", [
    ("05avmn224", "Fundación Universitaria San Alfonso",
     "000000002271", "000000000572"),
    ("05jj6ma85", "Institución Universitaria de Envigado",
     "000000000426", "000000008781"),
])
def test_repair_separates_unrelated_codes_from_canonical_affiliation(
        affiliation_id, canonical, retained, separated):
    plugin = Kahi_scienti_affiliations.__new__(Kahi_scienti_affiliations)
    plugin.collection = mongomock.MongoClient().db.affiliations
    plugin.verbose = 0
    plugin.collection.insert_one({
        "_id": affiliation_id,
        "names": [
            {"source": "ror", "name": canonical, "lang": "es"},
            {"source": "scienti", "name": canonical, "lang": "es"},
            {"source": "scienti", "name": "Unrelated", "lang": "es"},
        ],
        "abbreviations": [],
        "types": [{"source": "ror", "type": "education"}],
        "external_urls": [],
        "external_ids": [
            {"provenance": "ror", "source": "ror", "id": affiliation_id},
            {"provenance": "scienti", "source": "COD_INST", "id": retained},
            {"provenance": "scienti", "source": "COD_INST", "id": separated},
        ],
        "updated": [],
        "addresses": [],
    })
    catalog = {
        retained: {"COD_INST": retained, "NME_INST": canonical},
        separated: {
            "COD_INST": separated,
            "NME_INST": (
                "FUNDACION UNIVERSITARIA SAN MATEO EDUCACION SUPERIOR"
                if affiliation_id == "05avmn224"
                else "INSTITUCION UNIVERSITARIA DE COLOMBIA"
            ),
        },
    }

    repaired = plugin.repair_scienti_code_fusions(catalog)
    result = plugin.collection.find_one({"_id": affiliation_id})
    codes = {
        item["id"] for item in result["external_ids"]
        if item["source"] == "COD_INST"
    }

    assert repaired == {affiliation_id}
    assert plugin.last_separated_scienti_codes == {separated}
    assert codes == {retained}
    assert {item["name"] for item in result["names"]} == {canonical}


def test_repair_preserves_explicit_campus_hierarchy():
    plugin = Kahi_scienti_affiliations.__new__(Kahi_scienti_affiliations)
    affiliation = {
        "names": [{"source": "ror", "name": "Universidad de Antioquia"}],
    }
    catalog = {
        "root": {
            "COD_INST": "root", "COD_INST_MACRO": "root",
            "NME_INST": "UNIVERSIDAD DE ANTIOQUIA",
        },
        "campus": {
            "COD_INST": "campus", "COD_INST_MACRO": "root",
            "NME_INST": "Sede Carmen de Viboral",
        },
        "foundation": {
            "COD_INST": "foundation", "COD_INST_MACRO": "foundation",
            "NME_INST": "Fundación Universidad de Antioquia",
        },
    }

    assert plugin.compatible_scienti_codes(
        affiliation, set(catalog), catalog) == set(catalog)


def test_group_parent_repair_removes_only_obsolete_repaired_relation():
    plugin = Kahi_scienti_affiliations.__new__(Kahi_scienti_affiliations)
    plugin.repaired_affiliation_ids = {"05avmn224"}
    entry = {
        "relations": [
            {"id": "05avmn224", "name": "San Alfonso", "types": []},
            {"id": "other", "name": "Other", "types": []},
        ],
        "addresses": [],
    }
    parent = {
        "_id": "scienti_000000000572",
        "names": [{"name": "San Mateo", "lang": "es"}],
        "types": [],
        "addresses": [],
    }

    assert plugin.enrich_group_parents(entry, [parent])
    assert {item["id"] for item in entry["relations"]} == {
        "other", "scienti_000000000572"}


def test_group_uses_only_authoritative_parent_of_scienti_campus():
    plugin = Kahi_scienti_affiliations.__new__(Kahi_scienti_affiliations)
    plugin.collection = mongomock.MongoClient().db.affiliations
    root = plugin.empty_affiliation()
    root.update({
        "_id": "root",
        "names": [{"source": "ror", "name": "Universidad", "lang": "es"}],
        "types": [{"source": "ror", "type": "education"}],
        "external_ids": [{
            "provenance": "ror", "source": "ror", "id": "root",
        }],
    })
    campus = plugin.empty_affiliation()
    campus.update({
        "_id": "campus",
        "names": [{"source": "scienti", "name": "Universidad - Sede"}],
        "types": [{"source": "scienti", "type": "education"}],
        "external_ids": [{
            "provenance": "scienti", "source": "COD_INST", "id": "campus",
        }],
        "relations": [{"id": "root"}],
    })
    plugin.collection.insert_many([root, campus])

    resolved = plugin.canonical_group_institutions(campus)

    assert [item["_id"] for item in resolved] == ["root"]


def test_catalog_preparation_does_not_materialize_all_institutions():
    plugin = Kahi_scienti_affiliations.__new__(Kahi_scienti_affiliations)
    plugin.verbose = 0
    catalog = {
        "correct": {"COD_INST": "correct", "NME_INST": "Correct"},
        "unused": {"COD_INST": "unused", "NME_INST": "Unused"},
    }
    plugin.load_institution_catalog = lambda configs: catalog
    plugin.catalog_unique_values = lambda source, field: set()

    mapping = plugin.process_scienti_institutions([], verbose=0)

    assert mapping == {}
    assert plugin.institution_catalog == catalog
    assert plugin.materialized_scienti_codes == {}
