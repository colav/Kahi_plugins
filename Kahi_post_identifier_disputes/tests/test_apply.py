import importlib

import mongomock
import pytest

from kahi_post_identifier_disputes.disputes import (
    decide,
    decide_orcid,
    main_title,
    normalize_title,
    same_author,
    title_score,
)

plugin_module = importlib.import_module(
    "kahi_post_identifier_disputes.Kahi_post_identifier_disputes"
)
Plugin = plugin_module.Kahi_post_identifier_disputes


def work(identifier, title, *authors, doi="10.1116/1.577814", source="openalex"):
    url = "https://doi.org/" + doi
    return {
        "_id": identifier,
        "titles": [{"title": title, "lang": "en", "source": source}],
        "authors": [{"full_name": name} for name in authors],
        "doi": url,
        "external_ids": [
            {"provenance": source, "source": source, "id": f"{source}-{identifier}"},
            {"provenance": source, "source": "doi", "id": url},
        ],
        "updated": [{"source": source, "time": 1}],
    }


ARTICLE = {
    "status": "ok", "type": "journal-article",
    "titles": ["Cluster formation in the vapor produced by laser pulsing of the "
               "Y1Ba2Cu3O7 superconducting solid"],
    "authors": ["M. Moalem", "D. R. Olander", "M. Balooch"], "author_count": 3,
}
OWNER = work("owner", ARTICLE["titles"][0], "M Moalem", "D R Olander")
OTHER = work("other", "Effects of target presputtering on stoichiometry of sputtered films",
             "Pedro Prieto", "Maria Gomez", source="minciencias")


def actions(registry, works):
    return [(item["action"], item["reason"]) for item in decide(registry, works)]


def test_titles_ignore_markup_and_tolerate_subtitles():
    assert normalize_title("<scp>SnO<sub>2</sub>–TiO<sub>2</sub></scp> structures") == \
        "sno 2 tio 2 structures"
    assert main_title("Textual exposures : photography in twentieth-century") == \
        "textual exposures"
    assert title_score(["Textual exposures : photography in twentieth-century spanish "
                        "american narrative fiction"], ["Textual Exposures"]) >= 90
    assert title_score(["Medicine, the heart and accordion music"],
                       ["Medicina, corazón y música de acordeón"]) < 90
    # A short generic prefix is not a main-title match.
    assert title_score(["Editorial: nuevos retos"], ["Editorial"]) < 90


def test_same_author_needs_a_surname_and_an_initial():
    assert same_author("M Moalem", "M. Moalem")
    assert same_author("Marco Antonio De León Espitia", "Marco A. de León")
    assert not same_author("Pedro Prieto", "M. Moalem")
    assert not same_author("Juan Moalem", "M. Moalem")


def test_nonexistent_doi_is_removed_from_every_work():
    assert actions({"status": "not_found"}, [OWNER, OTHER]) == [
        ("removed", "doi_not_found"), ("removed", "doi_not_found")]


def test_article_doi_stays_with_the_matching_title_only():
    assert actions(ARTICLE, [OWNER, OTHER]) == [
        ("kept", "title_match"), ("removed", "other_work_doi")]


def test_the_owner_of_the_title_takes_the_doi_from_a_work_with_a_shared_author():
    variant = work("variant", "Formación de cúmulos en el vapor producido por láser",
                   "Mario Moalem", source="scienti")
    assert actions(ARTICLE, [OWNER, variant]) == [
        ("kept", "title_match"), ("removed", "title_owner")]
    # Without an owner nobody can be told apart: the work is left untouched.
    assert actions(ARTICLE, [variant]) == [("ambiguous", "shared_author")]


def test_the_owner_of_the_title_needs_no_author_evidence():
    no_authors = {**ARTICLE, "authors": [], "author_count": 0}
    anonymous = work("anonymous", "Otro título completamente distinto")
    assert actions(no_authors, [OWNER, anonymous]) == [
        ("kept", "title_match"), ("removed", "title_owner")]


def test_a_similar_title_is_left_untouched_even_with_an_owner():
    registry = {"status": "ok", "type": "journal-article", "author_count": 1,
                "titles": ["Effect of Core Magnetization on Frequency Response Analysis "
                           "(FRA) of Power Transformers"], "authors": ["N. Abeywickrama"]}
    owner = work("owner", registry["titles"][0], "N Abeywickrama")
    similar = work("similar", "The Core magnetization effect on the transformer frequency "
                   "response analysis FRA", "Pedro Prieto")
    assert actions(registry, [owner, similar]) == [
        ("kept", "title_match"), ("ambiguous", "similar_title")]


def test_journal_doi_is_removed_from_its_articles_but_not_from_the_journal_record():
    journal = {"status": "ok", "type": "journal", "titles": ["Saber, Ciencia y Libertad"],
               "authors": [], "author_count": 0}
    record = work("journal", "Saber, Ciencia y Libertad")
    article = work("article", "Informalidad laboral y migración venezolana en Colombia",
                   "Ana Pérez")
    assert actions(journal, [record, article]) == [
        ("kept", "title_match"), ("removed", "container_doi")]
    untitled_issue = {"status": "ok", "type": "journal-issue", "titles": [], "authors": []}
    assert actions(untitled_issue, [article]) == [("removed", "container_doi")]


def test_book_without_authors_is_a_container_and_with_authors_follows_the_author_rule():
    chapter = work("chapter", "Webpages Classification with Phishing Content Using Naive "
                   "Bayes", "Ana Pérez")
    volume = {"status": "ok", "type": "book", "titles": ["Knowledge Management in "
              "Organizations"], "authors": [], "author_count": 0}
    assert actions(volume, [chapter]) == [("removed", "container_doi")]
    authored = {**volume, "authors": ["Ana Pérez"], "author_count": 1}
    assert actions(authored, [chapter]) == [("ambiguous", "shared_author")]
    assert actions({**volume, "authors": ["Luis Ruiz"], "author_count": 1}, [chapter]) == [
        ("removed", "other_work_doi")]


def test_missing_author_evidence_is_never_a_removal():
    no_authors = {**ARTICLE, "titles": ["Abstract Book"], "authors": [], "author_count": 0}
    assert actions(no_authors, [OTHER]) == [("ambiguous", "no_author_evidence")]
    truncated = {**ARTICLE, "author_count": 1200}
    assert actions(truncated, [OTHER]) == [("ambiguous", "no_author_evidence")]
    anonymous = work("anonymous", "Otro título completamente distinto")
    assert actions(ARTICLE, [anonymous]) == [("ambiguous", "no_author_evidence")]
    bare = {"status": "ok", "type": None, "titles": [], "authors": []}
    assert actions(bare, [OTHER]) == [("ambiguous", "no_registry_metadata")]


def test_similar_title_without_common_author_is_left_untouched():
    registry = {"status": "ok", "type": "journal-article", "author_count": 1,
                "titles": ["Effect of Core Magnetization on Frequency Response Analysis "
                           "(FRA) of Power Transformers"], "authors": ["N. Abeywickrama"]}
    similar = work("similar", "The Core magnetization effect on the transformer frequency "
                   "response analysis FRA", "Pedro Prieto")
    assert actions(registry, [similar]) == [("ambiguous", "similar_title")]


def twin(identifier, source, sources=None):
    item = work(identifier, ARTICLE["titles"][0].upper(), "Otra Persona", source=source)
    if sources:
        item["updated"] = [{"source": name, "time": 1} for name in sources]
    return item


def test_two_works_with_the_registered_title_leave_the_doi_to_the_best_source():
    assert actions(ARTICLE, [twin("a", "scholar"), twin("b", "openalex")]) == [
        ("removed", "lower_priority_source"), ("kept", "title_match")]
    order = ["openalex", "ciarp", "scienti", "minciencias", "scholar", "dspace"]
    for better, worse in zip(order, order[1:]):
        assert actions(ARTICLE, [twin("z", worse), twin("y", better)])[1] == (
            "kept", "title_match")


def test_a_tie_of_sources_goes_to_more_sources_then_to_the_older_id():
    richer = twin("z", "scholar", sources=["scholar", "dspace"])
    assert actions(ARTICLE, [twin("a", "scholar"), richer])[1] == ("kept", "title_match")
    assert actions(ARTICLE, [twin("b", "dspace"), twin("a", "dspace")])[1] == (
        "kept", "title_match")


def prepare(monkeypatch, works, resolved, dry_run=False):
    clients = {"mongodb://kahi": mongomock.MongoClient(),
               "mongodb://resolver": mongomock.MongoClient()}
    monkeypatch.setattr(plugin_module, "MongoClient", lambda uri: clients[uri])
    kahi = clients["mongodb://kahi"].kahi
    kahi.works.insert_many(works)
    kahi.person.insert_one({"_id": "p", "external_ids": []})
    resolver = clients["mongodb://resolver"].impactu_identifier_resolver
    if resolved:
        resolver.doi.insert_many(resolved)
    config = {
        "database_url": "mongodb://kahi", "database_name": "kahi",
        "post_identifier_disputes": {"resolver_database_url": "mongodb://resolver",
                                     "task": "apply", "dry_run": dry_run},
    }
    return kahi, config


def copies(*works):
    return [{**item, "external_ids": [dict(entry) for entry in item["external_ids"]],
             "updated": [dict(entry) for entry in item["updated"]]} for item in works]


def scenario():
    pending = [work("p1", "Un título", doi="10.1234/pending"),
               work("p2", "Otro título", doi="10.1234/pending")]
    truncated = [work("t1", "Detección de clorpirifos en leche", doi="10.4067/s0718"),
                 work("t2", "Disponibilidad de leche en Sudamérica", doi="10.4067/S0718")]
    unique = work("u", "Trabajo con DOI propio", doi="10.1234/unico")
    works = copies(OWNER, OTHER, *pending, *truncated, unique)
    resolved = [{"_id": "10.1116/1.577814", **ARTICLE},
                {"_id": "10.4067/s0718", "status": "not_found"},
                {"_id": "10.1234/pending", "status": "pending"}]
    return works, resolved


def test_apply_removes_the_doi_and_keeps_the_trace(monkeypatch):
    works, resolved = scenario()
    kahi, config = prepare(monkeypatch, works, resolved)

    Plugin(config).run()

    assert kahi.works.find_one({"_id": "owner"}) == copies(OWNER)[0]
    other = kahi.works.find_one({"_id": "other"})
    assert other["doi"] is None
    assert other["external_ids"] == [
        {"provenance": "minciencias", "source": "minciencias", "id": "minciencias-other"}]
    assert [item["source"] for item in other["updated"]] == [
        "minciencias", "disputes"]
    assert kahi.works.count_documents({"_id": {"$in": ["t1", "t2"]}, "doi": None}) == 2
    assert kahi.works.count_documents({"_id": {"$in": ["p1", "p2", "u"]}, "doi": None}) == 0

    trace = kahi.identifier_disputes.find_one({"_id": "other:doi:10.1116/1.577814"})
    assert trace["action"] == "removed" and trace["reason"] == "other_work_doi"
    assert trace["registry"]["type"] == "journal-article"
    assert trace["removed"] == {
        "doi": "https://doi.org/10.1116/1.577814",
        "external_ids": [{"provenance": "minciencias", "source": "doi",
                          "id": "https://doi.org/10.1116/1.577814"}],
    }
    assert kahi.identifier_disputes.find_one({"work_id": "owner"})["action"] == "kept"
    assert kahi.identifier_disputes.count_documents({"identifier": "10.1234/pending"}) == 0
    run = kahi.identifier_disputes_runs.find_one()
    assert run["doi"]["shared"] == 3 and run["doi"]["works"] == 6
    assert run["doi"]["decisions"]["removed"] == 3
    assert run["doi"]["decisions"]["kept"] == 1 and run["doi"]["decisions"]["pending"] == 2

    # Nothing is shared and resolved any more: the second run changes nothing.
    before = list(kahi.works.find().sort("_id", 1))
    trail = list(kahi.identifier_disputes.find().sort("_id", 1))
    Plugin(config).run()
    assert list(kahi.works.find().sort("_id", 1)) == before
    assert list(kahi.identifier_disputes.find().sort("_id", 1)) == trail


def test_dry_run_changes_no_work(monkeypatch):
    works, resolved = scenario()
    kahi, config = prepare(monkeypatch, works, resolved, dry_run=True)
    before = list(kahi.works.find().sort("_id", 1))

    Plugin(config).run()
    Plugin(config).run()

    assert list(kahi.works.find().sort("_id", 1)) == before
    assert kahi.identifier_disputes.count_documents({}) == 0
    assert kahi.identifier_disputes_dry_run.count_documents({"action": "removed"}) == 3
    assert kahi.identifier_disputes_dry_run.count_documents({}) == 4
    assert kahi.identifier_disputes_runs.find_one()["dry_run"] is True


def test_unknown_task_is_rejected(monkeypatch):
    _, config = prepare(monkeypatch, copies(OWNER), [])
    config["post_identifier_disputes"]["task"] = "merge"
    with pytest.raises(RuntimeError, match="unknown task 'merge'"):
        Plugin(config)


def orcid_actions(registry, *names):
    registry = {"status": "ok", **registry}
    return [(item["action"], item["reason"])
            for item in decide_orcid(registry, [{"full_name": name} for name in names])]


KEPT, LOST = ("kept", "name_match"), ("removed", "other_person_orcid")


@pytest.mark.parametrize("registry, names, expected", [
    ({"given_names": "Juana Valentina", "family_name": "Rozo Suarez"},
     ["juan diego viveros arandia", "Juana Valentina Rozo Suárez"], [LOST, KEPT]),
    ({"given_names": "Héctor Ricardo", "family_name": "Vargas Sánchez",
      "credit_name": "HR Vargas Sánchez"},
     ["Héctor Vargas Vaca", "Héctor Ricardo Vargas Sanchez"], [LOST, KEPT]),
    ({"given_names": "Leonardo", "family_name": "Parra-Agudelo"},
     ["Leonardo Parra Agudelo", "Wilder Agudelo Parra"], [KEPT, LOST]),
    ({"given_names": "Jorge", "family_name": "Tobón", "credit_name": "Jorge I. Tobón"},
     ["Jorge Enrique Tobón Gómez", "Jorge Iván Tobón"], [LOST, KEPT]),
    ({"given_names": "Sandra", "family_name": "Camelo",
      "other_names": ["Sandra Milena CAMELO PINILLA"]},
     ["Sandra Milena Camelo Roa", "Sandra Milena Camelo Pinilla"], [LOST, KEPT]),
    ({"given_names": "Andy", "family_name": "Villamizar"},
     ["Andrey Cordero Villamizar", "Andy Steeth Villamizar Meza"], [LOST, KEPT]),
    # The other name fits inside the registered one: maybe a second profile.
    ({"given_names": "Maritza", "family_name": "Torres-Samuel",
      "other_names": ["Maritza Torres", "M.Torres", "Maritza Torres Samuel"]},
     ["Maritza Torres", "Maritza Torres Samuel"],
     [("ambiguous", "possible_duplicate_person"), KEPT]),
    ({"given_names": "Carlos Andres", "family_name": "González"},
     ["Carlos Andres Gonzalez Restrepo", "Carlos Gonzalez Gonzalez"],
     [KEPT, ("ambiguous", "possible_duplicate_person")]),
    ({"given_names": "Carlos", "family_name": "González"},
     ["Carlos Gonzalez Restrepo", "Carlos Gonzalez Mesa"],
     [("ambiguous", "tie"), ("ambiguous", "tie")]),
    ({"given_names": "Pedro", "family_name": "Ruiz"},
     ["Ana Gomez", "Luis Ruiz"], [("ambiguous", "no_owner"), ("ambiguous", "no_owner")]),
    ({"given_names": None, "family_name": None},
     ["Ana Gomez", "Luis Ruiz"],
     [("ambiguous", "no_registry_name"), ("ambiguous", "no_registry_name")]),
])
def test_orcid_stays_with_the_person_named_in_the_record(registry, names, expected):
    assert orcid_actions(registry, *names) == expected


def test_unresolved_orcid_is_never_removed():
    assert decide_orcid({"status": "not_found"}, [{"full_name": "Ana Gomez"}]) == [
        {"action": "ambiguous", "reason": "orcid_not_found", "score": 0}]


def test_apply_removes_the_orcid_from_the_other_person(monkeypatch):
    url = "https://orcid.org/0000-0001-7556-1150"
    kahi, config = prepare(monkeypatch, copies(OWNER), [])
    kahi.person.insert_many([
        {"_id": "vaca", "full_name": "Héctor Vargas Vaca",
         "updated": [{"source": "scienti", "time": 1}, {"source": "orcid", "time": 2},
                     {"source": "orcid", "time": 3}],
         "external_ids": [{"provenance": "scienti", "source": "scienti", "id": "1"},
                          {"provenance": "scienti", "source": "orcid", "id": url},
                          {"provenance": "orcid", "source": "orcid", "id": url}]},
        {"_id": "sanchez", "full_name": "Héctor Ricardo Vargas Sanchez",
         "updated": [{"source": "scienti"}],
         "external_ids": [{"provenance": "scienti", "source": "orcid", "id": url}]},
    ])
    resolver = plugin_module.MongoClient("mongodb://resolver").impactu_identifier_resolver
    resolver.orcid.insert_one({"_id": "0000-0001-7556-1150", "status": "ok",
                               "given_names": "Héctor Ricardo", "family_name": "Vargas Sánchez"})

    Plugin(config).run()

    vaca = kahi.person.find_one({"_id": "vaca"})
    assert vaca["external_ids"] == [{"provenance": "scienti", "source": "scienti", "id": "1"}]
    assert [item["source"] for item in vaca["updated"]] == ["scienti", "disputes"]
    assert len(kahi.person.find_one({"_id": "sanchez"})["external_ids"]) == 1
    trace = kahi.identifier_disputes.find_one({"_id": "vaca:orcid:0000-0001-7556-1150"})
    assert trace["action"] == "removed" and trace["review"] is True
    assert len(trace["removed"]["external_ids"]) == 2
    assert trace["removed"]["updated"] == [{"source": "orcid", "time": 2},
                                           {"source": "orcid", "time": 3}]
    assert trace["registry"]["family_name"] == "Vargas Sánchez"
    run = kahi.identifier_disputes_runs.find_one()
    assert run["orcid"] == {"shared": 1, "people": 2, "decisions": {
        "removed": 1, "removed:other_person_orcid": 1, "kept": 1, "kept:name_match": 1}}

    before = list(kahi.person.find().sort("_id", 1))
    Plugin(config).run()
    assert list(kahi.person.find().sort("_id", 1)) == before


def test_a_person_with_another_orcid_keeps_the_orcid_source(monkeypatch):
    shared, own = "https://orcid.org/0000-0001-7556-1150", "https://orcid.org/0000-0003-0000-0001"
    kahi, config = prepare(monkeypatch, copies(OWNER), [])
    kahi.person.insert_many([
        {"_id": "vaca", "full_name": "Héctor Vargas Vaca",
         "updated": [{"source": "orcid", "time": 2}],
         "external_ids": [{"provenance": "scienti", "source": "orcid", "id": shared},
                          {"provenance": "orcid", "source": "orcid", "id": own}]},
        {"_id": "sanchez", "full_name": "Héctor Ricardo Vargas Sanchez", "updated": [],
         "external_ids": [{"provenance": "scienti", "source": "orcid", "id": shared}]},
    ])
    resolver = plugin_module.MongoClient("mongodb://resolver").impactu_identifier_resolver
    resolver.orcid.insert_one({"_id": "0000-0001-7556-1150", "status": "ok",
                               "given_names": "Héctor Ricardo", "family_name": "Vargas Sánchez"})

    Plugin(config).run()

    vaca = kahi.person.find_one({"_id": "vaca"})
    assert [item["id"] for item in vaca["external_ids"]] == [own]
    assert [item["source"] for item in vaca["updated"]] == ["orcid", "disputes"]
    trace = kahi.identifier_disputes.find_one({"_id": "vaca:orcid:0000-0001-7556-1150"})
    assert "updated" not in trace["removed"]
