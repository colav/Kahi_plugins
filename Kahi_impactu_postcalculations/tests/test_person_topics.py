import mongomock

from kahi_impactu_postcalculations.person_topics import (
    aggregate_person_topics,
    build_person_topic_profile,
    person_identity_evidence,
)


def topic(topic_id, score, name=None):
    return {
        "id": f"https://openalex.org/{topic_id}",
        "display_name": name or topic_id,
        "domain": {
            "id": "https://openalex.org/domains/2",
            "display_name": "Social Sciences",
        },
        "field": {
            "id": "https://openalex.org/fields/33",
            "display_name": "Social Sciences",
        },
        "subfield": {
            "id": "https://openalex.org/subfields/3304",
            "display_name": "Education",
        },
        "score": score,
    }


def databases():
    client = mongomock.MongoClient()
    return (
        client.kahi.person,
        client.kahi.works,
        client.calculations.person_topics_tmp,
        client.calculations.person_topics_runs,
    )


def test_identity_requires_cod_rh_or_recognized_document_type():
    assert person_identity_evidence({
        "external_ids": [{"source": "scienti", "id": {"COD_RH": "123"}}]
    }) == ("COD_RH",)
    assert person_identity_evidence({
        "external_ids": [{"source": "Pasaporte", "id": "AB123"}]
    }) == ("identity_document",)
    assert person_identity_evidence({
        "external_ids": [{"source": "orcid", "id": "0000-0001"}]
    }) == ("ORCID",)
    assert person_identity_evidence({
        "external_ids": [{
            "source": "scienti", "id": {"COD_RH": "0000000000"},
        }]
    }) == ()


def test_profile_keeps_five_topics_and_primary_is_first():
    stats = {}
    for index in range(6):
        value = topic(f"T{index}", 0.9 - index / 10)
        stats[value["id"]] = {
            "topic": value,
            "works_count": 2,
            "weight": value["score"] * 2,
        }
    profile = build_person_topic_profile(stats, min_works=2, limit=5)

    assert len(profile["topics"]) == 5
    assert profile["primary_topic"] == profile["topics"][0]
    assert profile["topics"][0]["id"] == "https://openalex.org/T0"
    assert list(profile["topics"][0]) == [
        "id", "display_name", "domain", "field", "subfield", "score",
    ]


def test_profile_rounding_never_exceeds_one():
    weights = [4.0, 1.0, 1.0]
    stats = {}
    for index, weight in enumerate(weights):
        value = topic(f"T{index}", weight / 2)
        stats[value["id"]] = {
            "topic": value,
            "works_count": 2,
            "weight": weight,
        }

    profile = build_person_topic_profile(stats, min_works=2, limit=5)

    assert sum(item["score"] for item in profile["topics"]) <= 1.0
    assert profile["primary_topic"] == profile["topics"][0]


def test_repeated_topics_take_slots_before_high_confidence_singletons():
    stats = {}
    for index in range(5):
        value = topic(f"REPEATED{index}", 0.2)
        stats[value["id"]] = {
            "topic": value, "works_count": 2, "weight": 0.4,
        }
    singleton = topic("SINGLETON", 0.99)
    stats[singleton["id"]] = {
        "topic": singleton, "works_count": 1, "weight": 0.99,
    }

    profile = build_person_topic_profile(
        stats, min_works=2, limit=5, singleton_min_score=0.9
    )

    assert len(profile["topics"]) == 5
    assert all("REPEATED" in item["id"] for item in profile["topics"])


def test_singletons_fill_slots_and_primary_has_highest_aggregate_score():
    repeated = topic("REPEATED", 0.4)
    accepted = topic("ACCEPTED", 0.95)
    rejected = topic("REJECTED", 0.89)
    stats = {
        repeated["id"]: {
            "topic": repeated, "works_count": 2, "weight": 0.8,
        },
        accepted["id"]: {
            "topic": accepted, "works_count": 1, "weight": 0.95,
        },
        rejected["id"]: {
            "topic": rejected, "works_count": 1, "weight": 0.89,
        },
    }

    profile = build_person_topic_profile(
        stats, min_works=2, limit=5, singleton_min_score=0.9
    )

    assert [item["id"] for item in profile["topics"]] == [
        accepted["id"], repeated["id"],
    ]
    assert profile["primary_topic"] == profile["topics"][0]


def test_dry_run_stages_auditable_profiles_without_changing_person():
    persons, works, staging, runs = databases()
    persons.insert_many([
        {
            "_id": "p1",
            "subjects": [{"name": "Existing"}],
            "external_ids": [{
                "source": "scienti", "id": {"COD_RH": "0000000001"},
            }],
        },
        {
            "_id": "p2",
            "external_ids": [{
                "source": "Cédula de Ciudadanía", "id": "123",
            }],
        },
        {
            "_id": "p3",
            "external_ids": [{"source": "orcid", "id": "0000-0001"}],
        },
        {
            "_id": "p4",
            "external_ids": [
                {"source": "scienti", "id": {"COD_RH": "0000000004"}},
                {"source": "scienti", "id": {"COD_RH": "0000000000"}},
            ],
        },
    ])
    works.insert_many([
        {
            "_id": "w1", "authors": [{"id": "p1"}, {"id": "p1"}],
            "primary_topic": topic("T1", 0.8),
        },
        {
            "_id": "w2", "authors": [{"id": "p1"}],
            "primary_topic": topic("T1", 0.6),
        },
        {
            "_id": "w3", "authors": [{"id": "p1"}],
            "primary_topic": topic("T2", 0.9),
        },
        {
            "_id": "w4", "authors": [{"id": "p2"}],
            "primary_topic": topic("T3", 0.89),
        },
        {
            "_id": "w5", "authors": [{"id": "p3"}],
            "primary_topic": topic("T4", 0.95),
        },
    ])

    report = aggregate_person_topics(
        persons, works, staging, runs,
        dry_run=True, jobs=4, batch_size=1, min_works=2, limit=5,
    )

    assert report["eligible_persons"] == 4
    assert report["eligible_by_orcid"] == 1
    assert report["persons_with_topics"] == 2
    assert "topics" not in persons.find_one({"_id": "p1"})
    assert persons.find_one({"_id": "p1"})["subjects"] == [
        {"name": "Existing"}
    ]
    staged = staging.find_one({"_id": "p1"})
    assert staged["primary_topic"] == staged["topics"][0]
    assert staged["topics"][0]["id"] == "https://openalex.org/T1"
    assert staged["topics"][0]["score"] == 0.608696
    assert staging.find_one({"_id": "p2"})["topics"] == []
    assert staging.find_one({"_id": "p3"})["topics"][0]["id"] == (
        "https://openalex.org/T4"
    )
    assert staging.find_one({"_id": "p4"})["topics"] == []
    run = runs.find_one({"_id": report["run_id"]})
    assert run["status"] == "COMPLETED"
    assert run["version"] == "person-topics-v1"
    assert run["provenance"] == "works.primary_topic"


def test_apply_is_idempotent_and_only_updates_topic_fields():
    persons, works, staging, runs = databases()
    persons.insert_many([
        {
            "_id": "p1",
            "subjects": [{"name": "Existing"}],
            "external_ids": [{"source": "Pasaporte", "id": "AB123"}],
        },
        {
            "_id": "p2",
            "external_ids": [{"source": "Pasaporte", "id": "CD456"}],
            "primary_topic": topic("STALE", 1.0),
            "topics": [topic("STALE", 1.0)],
        },
    ])
    works.insert_many([
        {
            "_id": "w1", "authors": [{"id": "p1"}],
            "primary_topic": topic("T1", 0.8),
        },
        {
            "_id": "w2", "authors": [{"id": "p1"}],
            "primary_topic": topic("T1", 0.7),
        },
    ])

    first = aggregate_person_topics(
        persons, works, staging, runs,
        dry_run=False, jobs=2, batch_size=1, min_works=2, limit=5,
    )
    second = aggregate_person_topics(
        persons, works, staging, runs,
        dry_run=False, jobs=2, batch_size=1, min_works=2, limit=5,
    )

    person = persons.find_one({"_id": "p1"})
    assert first["persons_modified"] == 2
    assert second["persons_modified"] == 0
    assert person["subjects"] == [{"name": "Existing"}]
    assert person["primary_topic"] == person["topics"][0]
    assert person["topics"][0]["score"] == 1.0
    assert "primary_topic" not in persons.find_one({"_id": "p2"})
    assert "topics" not in persons.find_one({"_id": "p2"})
