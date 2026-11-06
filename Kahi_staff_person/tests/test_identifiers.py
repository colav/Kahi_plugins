import mongomock
from pandas import DataFrame

from kahi_staff_person.Kahi_staff_person import (
    Kahi_staff_person,
    append_staff_external_ids,
    build_ciarp_related_work_map,
    find_staff_unit,
    normalize_cod_rh,
    normalized_full_name,
    normalize_orcid,
    merge_staff_person,
    staff_person_query,
    staff_primary_person_query,
    staff_affiliation_interval,
    staff_department_id,
    staff_faculty_id,
)


def test_normalize_orcid_as_url():
    assert normalize_orcid("https://orcid.org/0000-0002-0906-7557") == (
        "https://orcid.org/0000-0002-0906-7557")


def test_normalize_cod_rh_from_raw_value_and_cvlac_url():
    assert normalize_cod_rh("0001497861") == "0001497861"
    assert normalize_cod_rh(
        "https://scienti.minciencias.gov.co/cvlac/visualizador/"
        "generarCurriculoCv.do?cod_rh=0001497861") == "0001497861"


def test_adds_primary_orcid_and_cvlac_but_not_scholar():
    external_ids = []
    record = {
        "tipo_documento": "cédula de ciudadanía",
        "orcid": "0000-0002-0906-7557",
        "cvlac": "0001886695",
        "scholar": "https://scholar.google.com/example",
    }

    assert append_staff_external_ids(external_ids, record, "1001199585")
    assert external_ids == [
        {
            "provenance": "staff",
            "source": "Cédula de Ciudadanía",
            "id": "1001199585",
        },
        {
            "provenance": "staff",
            "source": "orcid",
            "id": "https://orcid.org/0000-0002-0906-7557",
        },
        {
            "provenance": "staff",
            "source": "scienti",
            "id": {"COD_RH": "0001886695"},
        },
    ]


def test_cod_rh_primary_identifier_is_preserved_without_duplicates():
    external_ids = []
    record = {
        "tipo_documento": "COD_RH",
        "cvlac": "0001497861",
    }

    assert append_staff_external_ids(external_ids, record, "0001497861")
    assert external_ids == [{
        "provenance": "staff",
        "source": "scienti",
        "id": {"COD_RH": "0001497861"},
    }]


def test_build_ciarp_related_work_map_keeps_only_allowed_valid_dois():
    data = DataFrame([
        {
            "identificación": "123",
            "ranking": "Artículo de revista",
            "doi": "10.1000/valid",
            "año": "2024",
        },
        {
            "identificación": "123",
            "ranking": "Posters presentados",
            "doi": "10.1000/excluded",
            "año": "2024",
        },
    ])

    assert build_ciarp_related_work_map(data, ["Artículo de revista"]) == {
        "123": [{
            "provenance": "ciarp",
            "source": "doi",
            "id": "https://doi.org/10.1000/valid",
            "year": "2024",
        }]
    }


def test_ciarp_backfill_updates_existing_person_idempotently():
    db = mongomock.MongoClient().db
    db.person.insert_one({
        "_id": "person-1",
        "external_ids": [{
            "provenance": "staff",
            "source": "Cédula de Ciudadanía",
            "id": "123",
        }],
        "related_works": [],
    })
    staff = DataFrame([{
        "identificación": "123",
        "tipo_documento": "cédula de ciudadanía",
        "orcid": "",
        "cvlac": "",
    }])
    ciarp = DataFrame([{
        "identificación": "123",
        "ranking": "Artículo de revista",
        "doi": "10.1000/valid",
        "año": "2024",
    }])
    plugin = object.__new__(Kahi_staff_person)
    plugin.collection = db.person
    plugin.verbose = 0
    plugin._institution_ids = lambda: ["02yr3f298"]
    plugin._config_for_institution = lambda _institution_id: {}
    plugin._load_staff_data = lambda _config, _institution_id: staff
    plugin._load_ciarp_data = lambda _config, _institution_id: ciarp

    assert plugin.backfill_ciarp_related_works() == 0
    assert plugin.backfill_ciarp_related_works() == 0

    person = db.person.find_one({"_id": "person-1"})
    assert person["related_works"] == [{
        "provenance": "ciarp",
        "source": "doi",
        "id": "https://doi.org/10.1000/valid",
        "year": "2024",
    }]


def test_ciarp_backfill_disambiguates_identifier_collision_by_exact_name():
    db = mongomock.MongoClient().db
    external_id = {
        "provenance": "staff",
        "source": "scienti",
        "id": {"COD_RH": "0000085979"},
    }
    db.person.insert_many([
        {
            "_id": "wrong-person",
            "full_name": "German Sierra Gallego",
            "external_ids": [external_id],
            "related_works": [],
        },
        {
            "_id": "right-person",
            "full_name": "Ludovic Dorkis",
            "external_ids": [external_id],
            "related_works": [],
        },
    ])
    staff = DataFrame([{
        "identificación": "0000085979",
        "tipo_documento": "COD_RH",
        "nombres": "Ludovic",
        "primer_apellido": "Dorkis",
        "segundo_apellido": "",
        "orcid": "",
        "cvlac": "",
    }])
    ciarp = DataFrame([{
        "identificación": "0000085979",
        "ranking": "Artículo de revista",
        "doi": "10.1000/ludovic",
        "año": "2024",
    }])
    plugin = object.__new__(Kahi_staff_person)
    plugin.collection = db.person
    plugin.verbose = 0
    plugin._institution_ids = lambda: ["059yx9a68"]
    plugin._config_for_institution = lambda _institution_id: {}
    plugin._load_staff_data = lambda _config, _institution_id: staff
    plugin._load_ciarp_data = lambda _config, _institution_id: ciarp

    assert plugin.backfill_ciarp_related_works() == 0
    assert db.person.find_one({"_id": "wrong-person"})["related_works"] == []
    assert len(db.person.find_one({"_id": "right-person"})["related_works"]) == 1


def test_normalized_full_name_is_case_and_accent_insensitive():
    assert normalized_full_name("  LUDÓVIC-Dorkis ") == "ludovic dorkis"


def test_staff_units_resolve_by_deterministic_codes_not_names():
    db = mongomock.MongoClient().db
    record = {
        "código_unidad_académica": "124",
        "unidad_académica": "Facultad Compartida",
        "código_subunidad_académica": "501",
        "subunidad_académica": "Departamento Compartido",
    }
    expected_id = staff_department_id("parent-a", record)
    db.affiliations.insert_many([{
        "_id": expected_id,
        "names": [{"name": "Departamento Compartido"}],
        "types": [{"source": "staff", "type": "department"}],
        "relations": [{"id": "parent-a"}],
    }, {
        "_id": staff_department_id("parent-b", record),
        "names": [{"name": "Departamento Compartido"}],
        "types": [{"source": "staff", "type": "department"}],
        "relations": [{"id": "parent-b"}],
    }])

    resolved = find_staff_unit(
        db.affiliations, expected_id, "parent-a", "department")

    assert resolved["_id"] == expected_id
    assert staff_faculty_id("parent-a", record) == "parent-a_124"


def test_primary_query_does_not_use_supplementary_orcid():
    query = staff_primary_person_query([{
        "tipo_documento": "cédula de ciudadanía",
        "orcid": "0000-0002-3272-5327",
        "cvlac": "0001339069",
    }], "80539136")

    assert query == {"$or": [{"external_ids": {"$elemMatch": {
        "source": "Cédula de Ciudadanía",
        "id": "80539136",
    }}}]}


def test_staff_person_query_uses_canonical_document_source():
    query = staff_person_query([{
        "tipo_documento": "cédula de ciudadanía",
        "orcid": "",
        "cvlac": "",
    }], "123")

    assert query == {"$or": [{"external_ids": {"$elemMatch": {
        "source": "Cédula de Ciudadanía",
        "id": "123",
    }}}]}


def test_existing_person_receives_parent_and_multiple_staff_units():
    existing = {
        "_id": "person-1",
        "full_name": "Existing Name",
        "external_ids": [{"source": "openalex", "id": "A1"}],
        "affiliations": [{"id": "external-institution"}],
    }
    staff = {
        "full_name": "Staff Name",
        "external_ids": [{"source": "staff", "id": "123"}],
        "affiliations": [
            {"id": "parent", "start_date": 1, "end_date": -1,
             "years": [1970, 1971]},
            {"id": "faculty"},
            {"id": "department-1"},
            {"id": "department-2"},
        ],
    }

    merged = merge_staff_person(existing, staff)
    merged = merge_staff_person(merged, staff)

    assert merged["full_name"] == "Existing Name"
    assert [item["id"] for item in merged["affiliations"]] == [
        "external-institution", "parent", "faculty",
        "department-1", "department-2"]
    assert merged["external_ids"] == [
        {"source": "openalex", "id": "A1"},
        {"source": "staff", "id": "123"},
    ]


def test_staff_enriches_existing_parent_without_duplicating_it():
    existing = {"_id": "person-1", "affiliations": [{
        "id": "parent", "name": "University", "types": [
            {"source": "ror", "type": "education"}],
    }]}
    staff = {"affiliations": [{
        "id": "parent", "start_date": 1640995200,
        "end_date": -1, "years": [2022, 2023],
    }]}

    merged = merge_staff_person(existing, staff)

    assert len(merged["affiliations"]) == 1
    assert merged["affiliations"][0]["start_date"] == 1640995200
    assert merged["affiliations"][0]["years"] == [2022, 2023]


def test_staff_interval_unions_multiple_units_and_open_end():
    records = [
        {"fecha_inicial_vinculación": "01/01/2018",
         "fecha_final_vinculación": "31/12/2020"},
        {"fecha_inicial_vinculación": "01/01/2022",
         "fecha_final_vinculación": ""},
    ]

    start, end, years = staff_affiliation_interval(records, current_year=2026)

    assert start == 1514764800
    assert end == -1
    assert years == [2018, 2019, 2020, 2022, 2023, 2024, 2025, 2026]


def test_staff_interval_without_start_has_no_covered_years():
    assert staff_affiliation_interval([{
        "fecha_inicial_vinculación": "",
        "fecha_final_vinculación": "",
    }], current_year=2026) == (None, -1, [])
