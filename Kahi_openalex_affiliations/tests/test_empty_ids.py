import mongomock

from kahi_openalex_affiliations.Kahi_openalex_affiliations import openalex_ids, process_one


def institution(**ids):
    return {
        "id": "https://openalex.org/I1",
        "ror": ids.get("ror"),
        "ids": {"openalex": "https://openalex.org/I1", **ids},
        "geo": {"latitude": 0, "longitude": 0, "region": "", "city": "", "geonames_city_id": "",
                "country": "", "country_code": ""},
        "display_name": "Inst",
        "international": None,
        "homepage_url": None,
        "image_url": None,
        "type": "education",
        "display_name_alternatives": [],
    }


def empty_affiliation():
    return {"updated": [], "names": [], "external_ids": [], "external_urls": [],
            "types": [], "abbreviations": [], "addresses": []}


def test_openalex_ids_skips_empty_values():
    inst = institution(ror="https://ror.org/1", wikidata=None, wikipedia="", mag=123)
    assert list(openalex_ids(inst)) == [
        ("openalex", "https://openalex.org/I1"), ("ror", "https://ror.org/1"), ("mag", 123)]


def test_new_affiliation_has_no_empty_external_ids_or_urls():
    collection = mongomock.MongoClient().kahi.affiliations
    process_one(institution(wikidata=None, wikipedia=""), collection, empty_affiliation())
    reg = collection.find_one({})
    assert all(ext["id"] not in (None, "") for ext in reg["external_ids"])
    assert all(ext["url"] not in (None, "") for ext in reg["external_urls"])
    assert {ext["source"] for ext in reg["external_ids"]} == {"openalex"}


def test_existing_affiliation_is_not_polluted():
    collection = mongomock.MongoClient().kahi.affiliations
    collection.insert_one({
        "updated": [{"source": "ror", "time": 1}], "names": [],
        "external_ids": [{"provenance": "ror", "source": "ror", "id": "https://ror.org/1"}],
        "external_urls": [], "types": [], "abbreviations": [], "addresses": [],
    })
    process_one(institution(ror="https://ror.org/1", wikidata=None), collection, empty_affiliation())
    reg = collection.find_one({})
    assert [ext["source"] for ext in reg["external_ids"]] == ["ror", "openalex"]
    assert all(ext["url"] for ext in reg["external_urls"])
