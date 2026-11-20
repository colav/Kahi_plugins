"""News professors are linked to the release people without identity documents."""
import mongomock
import pytest

from kahi_impactu_postcalculations.news_linking import NEWS_COLLECTIONS, link_news


def _person(_id, *documents):
    return {"_id": _id, "full_name": _id, "external_ids": [
        {"provenance": "staff", "source": source, "id": number} for source, number in documents]}


@pytest.fixture
def databases():
    client = mongomock.MongoClient()
    news, release = client["impactu_news"], client["kahi_dev"]
    release.person.insert_many([
        _person("0000989010", ("Cédula de Ciudadanía", "10002787")),
        _person("0000000156088329", ("Cédula de Extranjería", "11002559")),
        _person("a", ("Cédula de Ciudadanía", "555")),
        _person("b", ("Passport", "555")),
    ])
    news.news_professors_collection.insert_many([
        {"_id": 1, "professor_id": "10002787", "full_name": "Urrea Duque Juan Pablo",
         "classified_urls_ids": ["2_1000"]},
        {"_id": 2, "professor_id": "11002559", "full_name": "Arrieta", "classified_urls_ids": []},
        {"_id": 3, "professor_id": "1000076611", "full_name": "Sin persona",
         "classified_urls_ids": ["2_1001"]},
        {"_id": 4, "professor_id": "555", "full_name": "Dos personas", "classified_urls_ids": []},
    ])
    news.news_media_collection.insert_one({"_id": 10, "medium_id": "2", "medium": "x"})
    news.news_urls_collection.insert_one({"_id": 20, "url_id": "2_1000", "medium_id": "2"})
    return news, release


def test_professors_get_the_person_id_and_lose_the_identity_document(databases):
    news, release = databases

    report = link_news(news, release)

    professors = {item["_id"]: item for item in release.news_professors_collection.find()}
    assert professors[1]["person_id"] == "0000989010"
    assert professors[2]["person_id"] == "0000000156088329"
    assert professors[3]["person_id"] is None
    assert professors[4]["person_id"] is None
    assert all("professor_id" not in item for item in professors.values())
    assert professors[1]["full_name"] == "Urrea Duque Juan Pablo"
    assert report["enlazados"] == 2
    assert report["sin_persona"] == 1
    assert report["ambiguos"] == 1
    assert report["con_noticias"] == 2
    assert report["con_noticias_enlazados"] == 1
    assert report["documentos"] == {name: 1 if name != "news_professors_collection" else 4
                                    for name in NEWS_COLLECTIONS}


def test_the_news_source_is_only_read_and_the_stage_can_be_repeated(databases):
    news, release = databases

    link_news(news, release)
    report = link_news(news, release)

    assert news.news_professors_collection.count_documents({"professor_id": {"$exists": True}}) == 4
    assert release.news_professors_collection.count_documents({}) == 4
    assert release.news_urls_collection.count_documents({}) == 1
    assert report["enlazados"] == 2


def test_it_refuses_to_run_after_the_identity_documents_left_person(databases):
    news, release = databases
    release.person.update_many({}, {"$set": {"external_ids": []}})

    with pytest.raises(RuntimeError, match="before removing them"):
        link_news(news, release)


def test_it_requires_the_three_news_collections(databases):
    news, release = databases
    news.news_urls_collection.drop()

    with pytest.raises(RuntimeError, match="Missing news collections"):
        link_news(news, release)
