from kahi_minciencias_opendata_person.Kahi_minciencias_opendata_person import normalize_person_name_dashes


def test_dashed_snapshot_names_are_normalized_with_alias():
    doc = {"full_name": "Adalira Saenz-Ludlow", "first_names": ["Adalira"],
           "last_names": ["Saenz-Ludlow"], "initials": "A", "aliases": []}
    normalize_person_name_dashes(doc)
    assert doc["full_name"] == "Adalira Saenz Ludlow"
    assert doc["last_names"] == ["Saenz", "Ludlow"]
    assert doc["initials"] == "A"
    assert doc["aliases"] == ["adalira saenz-ludlow"]


def test_dashed_first_names_update_initials():
    doc = {"full_name": "Jean‐Pierre Dupont", "first_names": ["Jean‐Pierre"],
           "last_names": ["Dupont"], "initials": "J", "aliases": ["x"]}
    normalize_person_name_dashes(doc)
    assert doc["first_names"] == ["Jean", "Pierre"]
    assert doc["initials"] == "JP"
    assert doc["aliases"] == ["x", "jean‐pierre dupont"]


def test_clean_document_is_untouched():
    doc = {"full_name": "Ana Pérez", "first_names": ["Ana"], "last_names": ["Pérez"], "initials": "A", "aliases": []}
    assert normalize_person_name_dashes(dict(doc)) == doc
