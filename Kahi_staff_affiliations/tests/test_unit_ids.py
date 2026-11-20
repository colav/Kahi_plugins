import mongomock
from pandas import DataFrame

from kahi_staff_affiliations.Kahi_staff_affiliations import (
    Kahi_staff_affiliations,
    staff_department_id,
    staff_faculty_id,
)


def test_unit_ids_use_source_codes():
    record = {
        "código_unidad_académica": "124",
        "unidad_académica": "Facultad de Educación",
        "código_subunidad_académica": "501",
        "subunidad_académica": "Departamento de Pedagogía",
    }
    assert staff_faculty_id("00jb9vg53", record) == "00jb9vg53_124"
    assert staff_department_id("00jb9vg53", record) == "00jb9vg53_124_501"


def test_missing_department_codes_use_distinct_stable_name_fingerprints():
    first = {
        "código_unidad_académica": "124",
        "unidad_académica": "Facultad de Educación",
        "código_subunidad_académica": "",
        "subunidad_académica": "Educación Física y Deportes",
    }
    equivalent = dict(first, subunidad_académica="EDUCACION FISICA Y DEPORTES")
    second = dict(first, subunidad_académica="Educación en Ciencia y Tecnología")

    assert staff_department_id("00jb9vg53", first) == staff_department_id(
        "00jb9vg53", equivalent)
    assert staff_department_id("00jb9vg53", first) != staff_department_id(
        "00jb9vg53", second)
    assert "_name_" in staff_department_id("00jb9vg53", first)


def test_missing_code_departments_are_materialized_without_collisions():
    plugin = object.__new__(Kahi_staff_affiliations)
    plugin.collection = mongomock.MongoClient().db.affiliations
    plugin.facs_inserted = {}
    plugin.deps_inserted = {}
    plugin.fac_dep = []
    parent = plugin.empty_affiliation()
    parent.update({
        "_id": "00jb9vg53",
        "names": [{"source": "ror", "lang": "es", "name": "Universidad del Valle"}],
        "types": [{"source": "ror", "type": "education"}],
    })
    plugin.collection.insert_one(parent)
    rows = DataFrame([{
        "código_unidad_académica": "124",
        "unidad_académica": "FACULTAD DE EDUCACION Y PEDAGOGIA",
        "código_subunidad_académica": "",
        "subunidad_académica": name,
    } for name in [
        "EDUCACION FISICA Y DEPORTES",
        "EDUCACION EN CIENCIA Y TECNOLOGIA",
    ]])

    plugin.staff_affiliation(
        rows, "00jb9vg53", "Universidad del Valle", parent)

    departments = list(plugin.collection.find({
        "types": {"$elemMatch": {"source": "staff", "type": "department"}},
    }))
    assert len(departments) == 2
    assert len({item["_id"] for item in departments}) == 2
    assert {item["names"][0]["name"] for item in departments} == {
        "Educacion Fisica y Deportes",
        "Educacion en Ciencia y Tecnologia",
    }
