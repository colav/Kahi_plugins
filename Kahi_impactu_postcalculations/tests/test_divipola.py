"""Colombian addresses are mapped to DIVIPOLA; groups stay in Colombia."""
import mongomock
import mongomock.collection
import pytest

from kahi_impactu_postcalculations.divipola import (
    Divipola,
    map_affiliation_addresses,
    normalize_colombian_addresses,
)

DIVIPOLA = Divipola()


def address(city, state="", country="Colombia", code="CO", lat="", lng=""):
    return {"lat": lat, "lng": lng, "postcode": "", "state": state, "city": city,
            "country": country, "country_code": code}


@pytest.fixture(autouse=True)
def bulk_write_without_sort(monkeypatch):
    builder = mongomock.collection.BulkOperationBuilder
    original = builder.add_update

    def add_update(self, *args, sort=None, **kwargs):
        return original(self, *args, **kwargs)

    monkeypatch.setattr(builder, "add_update", add_update)


def test_catalog_is_the_whole_divipola():
    assert len(DIVIPOLA.by_code) == 1122
    assert len({entry["state_code"] for entry in DIVIPOLA.by_code.values()}) == 33


@pytest.mark.parametrize("city, state, expected", [
    ("Bogotá", "Bogota D.C.", ("Bogotá", "Bogotá, D.C.", "11001", "11")),
    ("Bogotá, D.C.", "Bogotá, D. C.", ("Bogotá", "Bogotá, D.C.", "11001", "11")),
    ("Bogotá", "NARIÑO", ("Bogotá", "Bogotá, D.C.", "11001", "11")),          # wrong department
    ("Santiago de Cali", "Valle del Cauca Department", ("Cali", "Valle del Cauca", "76001", "76")),
    ("Cali", "BOYACÁ", ("Cali", "Valle del Cauca", "76001", "76")),
    ("Cartagena de Indias", "Bolívar", ("Cartagena", "Bolívar", "13001", "13")),
    ("Cucuta", "NORTE DE SANTANDER", ("Cúcuta", "Norte de Santander", "54001", "54")),
    ("PUPIALES", "NARIÑO", ("Pupiales", "Nariño", "52585", "52")),
    ("Armenia", "", ("Armenia", "Quindío", "63001", "63")),                    # default
    ("Armenia", "Antioquia", ("Armenia", "Antioquia", "05059", "05")),
    ("Rionegro", "", ("Rionegro", "Antioquia", "05615", "05")),
    ("Rionegro", "Santander", ("Rionegro", "Santander", "68615", "68")),
    ("Florencia", "Badahsan", ("Florencia", "Caquetá", "18001", "18")),
    ("Florencia", "Cauca", ("Florencia", "Cauca", "19290", "19")),
    ("Guayabal de Síquima", "Cundinamarca", ("Guayabal de Síquima", "Cundinamarca", "25328", "25")),
])
def test_find(city, state, expected):
    found = DIVIPOLA.find(city, state)
    assert (found["city"], found["state"], found["city_code"], found["state_code"]) == expected


@pytest.mark.parametrize("city, state", [("San Pedro", ""), ("Colima", "Colima"), ("", "Antioquia")])
def test_ambiguous_or_unknown_cities_are_not_guessed(city, state):
    assert DIVIPOLA.find(city, state) is None


def test_colombian_address_gets_codes_names_and_missing_coordinates():
    known = address("Medellín", "Antioquia", lat=6.25184, lng=-75.56359)
    result, counts = normalize_colombian_addresses(
        [address("Bogotá", "NARIÑO"), known, address("San Pedro"), address("Paris", "", "Francia", "FR")],
        DIVIPOLA)
    assert result[0] == {**address("Bogotá", "Bogotá, D.C."), "city_code": "11001",
                         "state_code": "11", "lat": 4.649251, "lng": -74.106992}
    assert (result[1]["lat"], result[1]["lng"], result[1]["city_code"]) == (6.25184, -75.56359, "05001")
    assert result[2] == address("San Pedro") and result[3] == address("Paris", "", "Francia", "FR")
    assert counts == {"mapped": 2, "unresolved": 1, "country_fixed": 0, "foreign_removed": 0,
                      "duplicates_removed": 0, "unlocated_removed": 0, "groups_without_city": 0,
                      "foreign_place_cleared": 0}


def test_group_keeps_only_colombian_addresses_once_per_city():
    addresses = [
        address("Medellín", "Antioquia", lat=6.25184, lng=-75.56359),
        address("Colima", "Colima", "Mexico", "MX"),
        address("Medellín", ""),
        address("Pereira", "", "Afganistán", "AF"),
        address("Bogotá", "", "", ""),
        address("", "", "No disponible", ""),
    ]
    result, counts = normalize_colombian_addresses(addresses, DIVIPOLA, is_group=True)
    assert [(item["city"], item["country_code"], item.get("city_code")) for item in result] == [
        ("Medellín", "CO", "05001"), ("Pereira", "CO", "66001"), ("Bogotá", "CO", "11001")]
    assert result[0]["lat"] == 6.25184
    assert counts == {"mapped": 4, "unresolved": 0, "country_fixed": 2, "foreign_removed": 1,
                      "duplicates_removed": 1, "unlocated_removed": 1, "groups_without_city": 0,
                      "foreign_place_cleared": 0}


def test_group_drops_addresses_that_cannot_be_placed_in_colombia():
    result, counts = normalize_colombian_addresses(
        [address("Pereira", "Risaralda"), address("Koh Band", "Kapisa"),
         address("No disponible", "No disponible", "No disponible", ""), address("", "")],
        DIVIPOLA, is_group=True)
    assert [(item["city"], item["city_code"]) for item in result] == [("Pereira", "66001")]
    assert counts["unlocated_removed"] == 3 and counts["groups_without_city"] == 0


def test_group_without_any_located_address_only_says_colombia():
    only_colombia = {"lat": "", "lng": "", "postcode": "", "state": "", "city": "",
                     "country": "Colombia", "country_code": "CO"}
    for addresses in (
        [address("Khwahan", "Badahsan"), address("No disponible", "No disponible", "No disponible", "")],
        [address("", "")],
        [address("Madrid", "Madrid", "Spain", "ES"), address("", "", "", "")],
        [address("Madrid", "Madrid", "Spain", "ES")],
    ):
        result, counts = normalize_colombian_addresses(addresses, DIVIPOLA, is_group=True)
        assert result == [only_colombia] and counts["groups_without_city"] == 1
        assert normalize_colombian_addresses(result, DIVIPOLA, is_group=True)[0] == result
    assert normalize_colombian_addresses([], DIVIPOLA, is_group=True)[0] == []


def test_institution_with_the_misassigned_country_and_a_colombian_city_is_colombian():
    result, counts = normalize_colombian_addresses(
        [address("Armenia", "", "Afganistán", "AF"), address("Kabul", "Kabul", "Afganistán", "AF"),
         address("Medellín", "", "", "")], DIVIPOLA)
    assert (result[0]["country"], result[0]["country_code"], result[0]["city_code"],
            result[0]["state"]) == ("Colombia", "CO", "63001", "Quindío")
    assert result[1] == address("Kabul", "Kabul", "Afganistán", "AF")
    assert result[2] == address("Medellín", "", "", "")   # only groups assume Colombia
    assert counts["country_fixed"] == 1


def test_a_foreign_place_under_a_colombian_address_keeps_only_the_country():
    addresses, counts = normalize_colombian_addresses(
        [address("Yawan", "Badahsan"),
         address("Albacete", "Provincia de Albacete", lat=1, lng=2)],
        DIVIPOLA)
    assert addresses == [address(""), address("")]
    assert counts["foreign_place_cleared"] == 2 and counts["unresolved"] == 0
    # A Colombian department, or none, leaves an unknown city as it is.
    addresses, counts = normalize_colombian_addresses(
        [address("Saint Andrews", "San Andres y Providencia"), address("Vereda La Hoya")],
        DIVIPOLA)
    assert addresses == [address("Saint Andrews", "San Andres y Providencia"),
                         address("Vereda La Hoya")]
    assert counts["foreign_place_cleared"] == 0 and counts["unresolved"] == 2


def test_institutions_keep_foreign_and_repeated_addresses():
    addresses = [address("Madrid", "Madrid", "España", "ES"), address("Cali"), address("Cali")]
    result, counts = normalize_colombian_addresses(addresses, DIVIPOLA)
    assert len(result) == 3 and result[0] == addresses[0]
    assert counts["foreign_removed"] == 0 and counts["duplicates_removed"] == 0


def test_collection_is_mapped_and_a_second_run_changes_nothing():
    collection = mongomock.MongoClient().kahi.affiliations
    collection.insert_many([
        {"_id": "COL1", "types": [{"type": "group"}],
         "addresses": [address("Medellín", "Antioquia"), address("Madrid", "Madrid", "Spain", "ES")]},
        {"_id": "udea", "types": [{"type": "education"}],
         "addresses": [address("Medellín", "Antioquia", lat=6.2, lng=-75.5)]},
        {"_id": "mit", "types": [{"type": "education"}],
         "addresses": [address("Cambridge", "Massachusetts", "Estados Unidos", "US")]},
    ])
    untouched = collection.find_one({"_id": "mit"})

    dry = map_affiliation_addresses(collection, dry_run=True)
    assert dry["documents_changed"] == 2 and collection.find_one({"_id": "mit"}) == untouched
    assert len(collection.find_one({"_id": "COL1"})["addresses"]) == 2

    report = map_affiliation_addresses(collection)
    assert (report["documents_changed"], report["mapped"], report["foreign_removed"]) == (2, 2, 1)
    group = collection.find_one({"_id": "COL1"})["addresses"]
    assert [(item["city"], item["city_code"]) for item in group] == [("Medellín", "05001")]
    assert collection.find_one({"_id": "udea"})["addresses"][0]["lat"] == 6.2
    assert collection.find_one({"_id": "mit"}) == untouched
    assert map_affiliation_addresses(collection)["documents_changed"] == 0
