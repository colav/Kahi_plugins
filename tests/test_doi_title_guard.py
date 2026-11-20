"""Exercise title decisions through the importers, not only the comparator."""
import importlib

import pytest

from kahi_impactu_utils.Work import work_titles_match
from test_work_doi_importers import (  # noqa: F401 (pytest fixture)
    db, empty_work, invoke, make_record, seed_work, source_identifier,
)


DOI = "https://doi.org/10.1136/bmj.d5928"
COCHRANE = "The Cochrane Collaboration's tool for assessing risk of bias in randomised trials"
EXTRACTS = "EXTRACTOS ALERGENICOS PARA INMUNOTERAPIA EN LATINOAMERICA. ALLERGEN EXTRACTS FOR IMMUNOTHERAPY IN LATIN AMERIC"
IMPORTERS = ("openalex", "scienti", "ciarp", "scholar", "dspace")


@pytest.fixture(autouse=True)
def no_registry_requests(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("DOI/title identity must not make HTTP requests during import")
    monkeypatch.setattr("requests.sessions.Session.request", forbidden)


def import_work(plugin, database, monkeypatch, title, index=0):
    if plugin in ("ciarp", "scholar", "dspace"):
        return invoke(plugin, make_record(plugin, index, doi=DOI, title=title), database)
    if plugin == "openalex":
        parser = importlib.import_module("kahi_openalex_works.parser")
        monkeypatch.setattr(parser, "lang_poll", lambda *_a, **_k: "en")
        module = importlib.import_module("kahi_openalex_works.process_one")
        record = {
            "id": f"https://openalex.org/W{index}", "doi": DOI,
            "ids": {"openalex": f"https://openalex.org/W{index}"},
            "title": title, "type": "article", "publication_year": 2011,
            "counts_by_year": [], "cited_by_count": 0, "primary_location": None,
            "biblio": {}, "authorships": [], "concepts": [],
        }
        return module.process_one(record, {"database_name": database.name}, empty_work(),
                                  database.client, None, "threading")
    module = importlib.import_module("kahi_scienti_works.process_one")

    def parser(_record, _template, *args, **kwargs):
        entry = empty_work()
        identifier = {"COD_RH": "0000000001", "COD_PRODUCTO": str(index)}
        entry.update(doi=DOI, titles=[{"title": title, "source": plugin, "lang": "en"}],
                     updated=[{"source": plugin, "time": 1}], year_published=2011,
                     external_ids=[{"source": plugin, "id": identifier}],
                     types=[{"source": plugin, "type": "article"}],
                     source={"name": "", "external_ids": []})
        entry["authors"] = [{"full_name": "Source Author", "affiliations": [],
                             "external_ids": [{"source": "scienti", "id": {"COD_RH": "0000000001"}}]}]
        return entry

    monkeypatch.setattr(module, "parse_scienti", parser)
    monkeypatch.setattr(module, "lang_poll", lambda *_a, **_k: "en")
    record = {"TXT_DOI": DOI, "TXT_NME_PROD": title, "COD_RH": "0000000001", "COD_PRODUCTO": str(index)}
    return module.process_one(record, database, database.works, empty_work(), None, similarity=False)


@pytest.mark.parametrize("plugin", IMPORTERS)
def test_distinct_publications_with_same_doi_do_not_contaminate_each_other(db, monkeypatch, plugin):  # noqa: F811
    original_id = seed_work(db, doi=DOI, title=COCHRANE)
    before = db.works.find_one({"_id": original_id})
    import_work(plugin, db, monkeypatch, EXTRACTS)
    import_work(plugin, db, monkeypatch, EXTRACTS)
    assert db.works.count_documents({}) == 2
    assert db.works.find_one({"_id": original_id}) == before
    incoming = db.works.find_one({"_id": {"$ne": original_id}})
    assert incoming["doi"] == DOI
    assert work_titles_match(incoming["titles"], [{"title": EXTRACTS}])
    assert db.works.count_documents({"_doi_key": {"$exists": True}}) == 0


@pytest.mark.parametrize("plugin", IMPORTERS)
@pytest.mark.parametrize("title", [COCHRANE.upper(), COCHRANE.replace("randomised", "randomized")])
def test_equivalent_titles_update_an_existing_doi_work(db, monkeypatch, plugin, title):  # noqa: F811
    original_id = seed_work(db, doi=DOI, title=COCHRANE)
    import_work(plugin, db, monkeypatch, title)
    assert db.works.count_documents({}) == 1
    assert db.works.find_one()["_id"] == original_id
    assert any(item.get("source") == plugin for item in db.works.find_one()["updated"])


@pytest.mark.parametrize("plugin", ("openalex", "ciarp", "scholar", "dspace"))
def test_source_id_fallback_cannot_bypass_an_incompatible_title(db, monkeypatch, plugin):  # noqa: F811
    source, identifier = (("openalex", "https://openalex.org/W0") if plugin == "openalex"
                          else source_identifier(plugin, 0))
    original_id = seed_work(db, doi=DOI, title=COCHRANE,
                            external_ids=[{"source": source, "id": identifier}])
    before = db.works.find_one({"_id": original_id})
    for _ in range(2):
        import_work(plugin, db, monkeypatch, EXTRACTS)
    assert db.works.count_documents({}) == 2
    assert db.works.find_one({"_id": original_id}) == before


def test_minciencias_related_works_do_not_group_different_titles_by_doi(db, monkeypatch):  # noqa: F811
    module = importlib.import_module("kahi_minciencias_opendata_works.related_works")
    monkeypatch.setattr(module, "lang_poll", lambda *_a, **_k: "en")
    db.person.insert_one({"_id": "person", "full_name": "Source author", "affiliations": [],
                          "related_works": [
                              {"title": title, "year": 2011, "source": "doi", "id": DOI, "doi": [DOI]}
                              for title in (COCHRANE, EXTRACTS)]})
    candidates = module.collect_related_works(db.person)
    assert len(candidates) == 2
    for _ in range(2):
        module.process_person_related_works(db.person, db.works, empty_work, insert_all=True)
    assert db.works.count_documents({"doi": DOI}) == 2
    assert {work["titles"][0]["title"] for work in db.works.find()} == {COCHRANE, EXTRACTS}


def test_minciencias_doi_conflict_does_not_fall_back_to_elasticsearch(db, monkeypatch):  # noqa: F811
    module = importlib.import_module("kahi_minciencias_opendata_works.related_works")
    monkeypatch.setattr(module, "lang_poll", lambda *_a, **_k: "en")

    def forbidden(*args, **kwargs):
        pytest.fail("A valid DOI must not fall back to title-only similarity")
    monkeypatch.setattr(module, "_similar_work", forbidden)
    seed_work(db, doi=DOI, title=COCHRANE)
    db.person.insert_one({"_id": "person", "full_name": "Source author", "affiliations": [],
                          "related_works": [{"title": EXTRACTS, "year": 2018,
                                             "source": "doi", "id": DOI, "doi": [DOI]}]})
    module.process_person_related_works(db.person, db.works, empty_work, insert_all=True)
    assert db.works.count_documents({}) == 2


def test_ciarp_real_cedulas_are_only_attached_to_the_extracts_work(db):  # noqa: F811
    from kahi_impactu_utils.Utils import split_names
    module = importlib.import_module("kahi_ciarp_works.process_one")
    institution = {"_id": "03bp5hc83", "types": [{"source": "ror", "type": "education"}],
                   "names": [{"name": "Universidad de Antioquia", "lang": "es", "source": "ror"}]}
    db.affiliations.insert_one(institution)
    people = [("0000207438", "70120604", "Ricardo Cardona Villa"),
              ("0000690490", "73203191", "Jorge Mario Sanchez Caraballo")]
    for pid, cedula, name in people:
        person = split_names(name)
        person.update(_id=pid, full_name=name,
                      external_ids=[{"source": "Cédula de Ciudadanía", "id": cedula}],
                      affiliations=[{"id": "03bp5hc83", "name": "Universidad de Antioquia",
                                     "years": [2018], "types": institution["types"]}])
        db.person.insert_one(person)
    original_id = seed_work(db, doi=DOI, title=COCHRANE)
    db.works.update_one({"_id": original_id}, {"$set": {
        "authors": [{"id": "higgins", "full_name": "Julian P. T. Higgins", "affiliations": []}],
        "author_count": 1}})
    before = db.works.find_one({"_id": original_id})
    for index, (_, cedula, _) in enumerate(people):
        record = make_record("ciarp", index, doi=DOI, title=EXTRACTS)
        record.update(identificación=cedula, año="2018")
        module.process_one(record, db, db.works, institution, empty_work(), False, None)
    assert db.works.count_documents({}) == 2
    assert db.works.find_one({"_id": original_id}) == before
    other = db.works.find_one({"_id": {"$ne": original_id}})
    assert {a["id"] for a in other["authors"]} == {pid for pid, _, _ in people}
    assert all(a["affiliations"][0]["id"] == "03bp5hc83" for a in other["authors"])
