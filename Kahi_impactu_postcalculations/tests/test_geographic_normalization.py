from copy import deepcopy

from kahi_impactu_postcalculations.geographic_normalization import (
    SpanishGeographicNormalizer,
    normalized_key,
)


def test_normalized_key_ignores_accents_case_and_punctuation():
    assert normalized_key("BOGOTÁ, D. C.") == normalized_key("Bogota D.C.")


def test_normalizes_country_department_and_city_without_changing_schema():
    normalizer = SpanishGeographicNormalizer()
    address = {
        "country": "Colombia",
        "country_code": "CO",
        "state": "Valle del Cauca Department",
        "city": "Santiago de Cali",
        "latitude": 3.45,
    }

    normalized, issues = normalizer.normalize_address(address)

    assert normalized == {
        "country": "Colombia",
        "country_code": "CO",
        "state": "Valle del Cauca",
        "city": "Cali",
        "latitude": 3.45,
    }
    assert issues == []
    assert set(normalized) == set(address)


def test_translates_countries_and_curated_city_exonyms():
    normalizer = SpanishGeographicNormalizer()

    london, issues = normalizer.normalize_address({
        "country": "United Kingdom", "country_code": "GB", "city": "London"
    })
    mexico, _ = normalizer.normalize_address({
        "country": "Mexico", "country_code": "MX", "city": "Mexico City"
    })

    assert london["country"] == "Reino Unido"
    assert london["city"] == "Londres"
    assert mexico["country"] == "México"
    assert mexico["city"] == "Ciudad de México"
    assert issues == []


def test_uses_country_code_for_an_unrecognized_source_alias():
    normalizer = SpanishGeographicNormalizer()

    normalized, issues = normalizer.normalize_address({
        "country": "Hong Kong", "country_code": "HK", "city": "Hong Kong"
    })

    assert normalized["country"] == "RAE de Hong Kong (China)"
    assert issues == []


def test_each_address_is_normalized_with_its_own_country_context():
    normalizer = SpanishGeographicNormalizer()
    addresses = [
        {"country": "Colombia", "country_code": "CO", "state": "Antioquia", "city": "Medellín"},
        {"country": "United Kingdom", "country_code": "GB", "state": "England", "city": "London"},
    ]

    normalized, changed, issues = normalizer.normalize_addresses(addresses)

    assert normalized[0]["state"] == "Antioquia"
    assert normalized[1]["country"] == "Reino Unido"
    assert normalized[1]["state"] == "England"
    assert normalized[1]["city"] == "Londres"
    assert changed == 1
    assert issues == []


def test_reports_unknown_values_and_is_idempotent():
    normalizer = SpanishGeographicNormalizer()
    original = {"country": "Atlantis", "state": "Ocean", "city": "Poseidon"}
    unknown, issues = normalizer.normalize_address(original)
    first, _, _ = normalizer.normalize_addresses([{
        "country": "Colombia", "country_code": "CO",
        "state": "Bogota D.C.", "city": "Bogotá, D.C.",
    }])
    second, changed_again, _ = normalizer.normalize_addresses(deepcopy(first))

    assert unknown == original
    assert issues == [("country", "Atlantis")]
    assert first == second
    assert changed_again == 0
