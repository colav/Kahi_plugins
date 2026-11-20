import importlib.util
from pathlib import Path

from bson import ObjectId


SCRIPT = Path(__file__).parents[1] / "scripts" / "merge_minciencias_work_dedup.py"
SPEC = importlib.util.spec_from_file_location("dedup_merge", SCRIPT)
dedup_merge = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dedup_merge)


def test_merge_preserves_survivor_and_all_bibliographic_lists():
    survivor_id = ObjectId()
    survivor = {
        "_id": survivor_id,
        "titles": [{"source": "openalex", "title": "Rich title"}],
        "external_ids": [{"source": "doi", "id": "10.1234/example"}],
        "updated": [{"source": "openalex", "time": 1}],
        "doi": "https://doi.org/10.1234/example",
        "authors": [],
        "groups": [],
    }
    loser = {
        "_id": ObjectId(),
        "titles": [{"source": "minciencias", "title": "Rich title"}],
        "external_ids": [{"source": "minciencias", "id": "ART-1"}],
        "updated": [{"source": "minciencias", "time": 2}],
        "doi": "",
        "authors": [],
        "groups": [],
    }

    merged = dedup_merge.merge_documents(survivor, [loser])

    assert merged["_id"] == survivor_id
    assert merged["doi"] == survivor["doi"]
    assert len(merged["titles"]) == 2
    assert len(merged["external_ids"]) == 2
    assert len(merged["updated"]) == 2


def test_merge_combines_same_author_id_without_losing_affiliations():
    survivor = {
        "_id": ObjectId(),
        "authors": [
            {"id": "person-1", "full_name": "Ana Example", "affiliations": [{"id": "A"}]}
        ],
    }
    loser = {
        "_id": ObjectId(),
        "authors": [
            {"id": "person-1", "full_name": "Ana Example", "affiliations": [{"id": "B"}]}
        ],
    }

    merged = dedup_merge.merge_documents(survivor, [loser])

    assert len(merged["authors"]) == 1
    assert {item["id"] for item in merged["authors"][0]["affiliations"]} == {"A", "B"}
    assert merged["author_count"] == 1


def test_merge_keeps_different_author_ids_even_when_names_match():
    survivor = {
        "_id": ObjectId(),
        "authors": [{"id": "person-rich", "full_name": "Ana Example"}],
    }
    loser = {
        "_id": ObjectId(),
        "authors": [{"id": "person-minc", "full_name": "Ana Example"}],
    }

    merged = dedup_merge.merge_documents(survivor, [loser])

    assert {item["id"] for item in merged["authors"]} == {"person-rich", "person-minc"}


def test_merge_uses_maximum_for_counts_and_keeps_rich_scalar():
    survivor = {
        "_id": ObjectId(),
        "doi": "10.1234/rich",
        "references_count": 5,
        "citations_count": [{"source": "openalex", "count": 7}],
    }
    loser = {
        "_id": ObjectId(),
        "doi": "10.1234/other",
        "references_count": 9,
        "citations_count": [{"source": "openalex", "count": 12}],
    }

    merged = dedup_merge.merge_documents(survivor, [loser])

    assert merged["doi"] == "10.1234/rich"
    assert merged["references_count"] == 9
    assert merged["citations_count"] == [{"source": "openalex", "count": 12}]


def test_primary_topic_from_loser_remains_in_topics():
    survivor = {"_id": ObjectId(), "primary_topic": {}, "topics": []}
    loser = {
        "_id": ObjectId(),
        "primary_topic": {"id": "T1", "display_name": "Topic"},
        "topics": [],
    }

    merged = dedup_merge.merge_documents(survivor, [loser])

    assert merged["primary_topic"]["id"] == "T1"
    assert [item["id"] for item in merged["topics"]] == ["T1"]
