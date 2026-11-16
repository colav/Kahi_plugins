import importlib

import mongomock
import pytest

from kahi_post_identifier_disputes.identifiers import (
    normalize_doi,
    normalize_orcid,
    shared_dois,
    shared_orcids,
)

plugin_module = importlib.import_module(
    "kahi_post_identifier_disputes.Kahi_post_identifier_disputes"
)
Plugin = plugin_module.Kahi_post_identifier_disputes


@pytest.mark.parametrize("raw, expected", [
    ("https://doi.org/10.18041/2382-3240", "10.18041/2382-3240"),
    ("HTTP://DX.DOI.ORG/10.1002/1521-3951(200007)220:1", "10.1002/1521-3951(200007)220:1"),
    ("doi: 10.4067/S0718", "10.4067/s0718"),
    ("https://doi.org/10.1016/s0120-5633%2814%2970279-7", "10.1016/s0120-5633(14)70279-7"),
    ("", None),
    ("https://example.org/10", None),
    (None, None),
])
def test_normalize_doi(raw, expected):
    assert normalize_doi(raw) == expected


def test_normalize_orcid():
    assert normalize_orcid("https://orcid.org/0000-0002-1972-100x") == "0000-0002-1972-100X"
    assert normalize_orcid("sin orcid") is None


def person(identifier, *orcids):
    external_ids = [{"provenance": "scienti", "source": "orcid", "id": orcid}
                    for orcid in orcids]
    external_ids.append({"provenance": "scienti", "source": "scienti", "id": identifier})
    return {"_id": identifier, "external_ids": external_ids}


def prepare(monkeypatch, works, people, task="collect"):
    clients = {"mongodb://kahi": mongomock.MongoClient(),
               "mongodb://resolver": mongomock.MongoClient()}
    monkeypatch.setattr(plugin_module, "MongoClient", lambda uri: clients[uri])
    kahi = clients["mongodb://kahi"].kahi
    kahi.works.insert_many(works)
    kahi.person.insert_many(people)
    config = {
        "database_url": "mongodb://kahi", "database_name": "kahi",
        "post_identifier_disputes": {"resolver_database_url": "mongodb://resolver",
                                     "task": task},
    }
    return kahi, clients["mongodb://resolver"].impactu_identifier_resolver, config


WORKS = [
    {"_id": 1, "doi": "https://doi.org/10.18041/2382-3240"},
    {"_id": 2, "doi": "https://doi.org/10.18041/2382-3240"},
    {"_id": 3, "doi": "https://DOI.org/10.18041/2382-3240"},
    {"_id": 4, "doi": "https://doi.org/10.1234/unico"},
    {"_id": 5, "doi": ""},
    {"_id": 6},
]
PEOPLE = [
    person("A", "https://orcid.org/0000-0002-7847-6258"),
    person("B", "https://orcid.org/0000-0002-7847-6258"),
    # The same ORCID twice on one person is not a dispute.
    person("C", "https://orcid.org/0000-0001-0000-0001", "0000-0001-0000-0001"),
    person("D"),
]


def test_shared_identifiers_count_entities_not_entries():
    db = mongomock.MongoClient().kahi
    db.works.insert_many(WORKS)
    db.person.insert_many(PEOPLE)
    assert shared_dois(db.works) == {"10.18041/2382-3240": 3}
    assert shared_orcids(db.person) == {"0000-0002-7847-6258": 2}


def test_collect_queues_only_new_disputes(monkeypatch):
    kahi, resolver, config = prepare(monkeypatch, WORKS, PEOPLE)

    Plugin(config).run()

    queued = resolver.doi.find_one()
    assert queued["_id"] == "10.18041/2382-3240" and queued["status"] == "pending"
    assert queued["requested_by"] == "kahi"
    assert resolver.doi.count_documents({}) == 1
    assert [doc["_id"] for doc in resolver.orcid.find()] == ["0000-0002-7847-6258"]
    run = kahi.identifier_disputes_runs.find_one()
    assert run["doi"] == {"shared": 1, "works": 3, "resolver": {"pending": 1}}
    assert run["orcid"] == {"shared": 1, "people": 2, "resolver": {"pending": 1}}

    # The capture resolved it; a new dispute appears in the next stage.
    resolver.doi.update_one({"_id": "10.18041/2382-3240"},
                            {"$set": {"status": "ok", "type": "journal"}})
    kahi.works.insert_many([{"_id": 7, "doi": "https://doi.org/10.1234/unico"}])

    Plugin(config).run()

    assert resolver.doi.find_one({"_id": "10.18041/2382-3240"})["type"] == "journal"
    assert resolver.doi.find_one({"_id": "10.1234/unico"})["status"] == "pending"
    last = kahi.identifier_disputes_runs.find_one(sort=[("_id", -1)])
    assert last["doi"]["resolver"] == {"ok": 1, "pending": 1}
    assert resolver.orcid.count_documents({}) == 1


def test_only_a_trial_ignores_the_log():
    assert Plugin.manages_checkpoints({"task": "apply", "dry_run": True}) is True
    assert Plugin.manages_checkpoints({"task": "apply", "dry_run": False}) is False
    assert Plugin.manages_checkpoints({"task": "apply"}) is False
    assert Plugin.manages_checkpoints({"task": "collect"}) is False


def test_unknown_task_is_rejected(monkeypatch):
    _, _, config = prepare(monkeypatch, WORKS, PEOPLE, task="merge")
    with pytest.raises(RuntimeError, match="unknown task 'merge'"):
        Plugin(config)
