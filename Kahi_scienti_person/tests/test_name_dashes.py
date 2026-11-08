from kahi_scienti_person.Kahi_scienti_person import scienti_author_names


def test_ascii_dash_in_last_name_becomes_space_and_alias():
    names = scienti_author_names({"TXT_NAMES_RH": "ADALIRA", "TXT_PRIM_APELL": "SAENZ-LUDLOW", "TXT_SEG_APELL": None})
    assert names["first_names"] == ["Adalira"]
    assert names["last_names"] == ["Saenz Ludlow"]
    assert names["initials"] == "A"
    assert names["alias"] == "adalira saenz-ludlow"


def test_unicode_dash_and_dots_in_first_names():
    names = scienti_author_names({"TXT_NME_RH": "JEAN‑PIERRE M.", "TXT_PRIM_APELL": "DUPONT", "TXT_SEG_APELL": "DE LA CRUZ"})
    assert names["first_names"] == ["Jean", "Pierre", "M"]
    assert names["last_names"] == ["Dupont", "de la Cruz"]
    assert names["initials"] == "JPM"


def test_name_without_dash_has_no_alias():
    names = scienti_author_names({"TXT_NAMES_RH": "GABRIEL", "TXT_PRIM_APELL": "GARCIA", "TXT_SEG_APELL": "MARQUEZ"})
    assert names["last_names"] == ["Garcia", "Marquez"]
    assert names["alias"] == ""
