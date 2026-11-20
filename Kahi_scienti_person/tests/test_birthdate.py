from kahi_scienti_person.Kahi_scienti_person import normalize_birthdate


def test_normalize_birthdate_preserves_valid_timestamp():
    value = normalize_birthdate("1980-01-01")

    assert isinstance(value, int)
    assert value != -1


def test_normalize_birthdate_maps_empty_value_to_unknown():
    assert normalize_birthdate("") == -1
    assert normalize_birthdate(None) == -1


def test_normalize_birthdate_maps_unrecognized_value_to_unknown():
    assert normalize_birthdate("00000000") == -1
