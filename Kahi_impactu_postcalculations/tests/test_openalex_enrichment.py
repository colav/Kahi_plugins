from copy import deepcopy

from kahi_impactu_postcalculations.openalex_enrichment import (
    build_openalex_enrichment,
    openalex_id,
)


def test_extracts_exact_openalex_identifier():
    work = {"external_ids": [
        {"source": "doi", "id": "10.1/test"},
        {"source": "openalex", "id": "https://openalex.org/W1"},
    ]}

    assert openalex_id(work) == "https://openalex.org/W1"


def test_recovers_missing_abstract_and_topics_without_new_schema_fields():
    work = {"abstracts": [], "primary_topic": None, "topics": []}
    raw = {
        "abstract_inverted_index": {"texto": [0]},
        "language": "es",
        "primary_topic": {"id": "https://openalex.org/T1", "score": 0.9},
        "topics": [{"id": "https://openalex.org/T1", "score": 0.9}],
    }

    fields, abstracts, topics = build_openalex_enrichment(work, raw)

    assert fields["abstracts"][0]["abstract"] == {"texto": [0]}
    assert fields["abstracts"][0]["source"] == "openalex"
    assert fields["primary_topic"]["id"] == "https://openalex.org/T1"
    assert fields["topics"] == raw["topics"]
    assert (abstracts, topics) == (1, 1)


def test_enrichment_preserves_existing_values_and_is_idempotent():
    raw = {
        "abstract_inverted_index": {"openalex": [0]},
        "primary_topic": {"id": "https://openalex.org/T2"},
        "topics": [{"id": "https://openalex.org/T2"}],
    }
    work = {
        "abstracts": [{"abstract": {"existing": [0]}, "source": "scienti"}],
        "primary_topic": {"id": "https://openalex.org/T1"},
        "topics": [{"id": "https://openalex.org/T1"}],
    }
    fields, abstracts, topics = build_openalex_enrichment(work, raw)
    enriched = deepcopy(work)
    enriched.update(fields)
    second_fields, second_abstracts, second_topics = build_openalex_enrichment(
        enriched, raw
    )

    assert "abstracts" not in fields
    assert "primary_topic" not in fields
    assert fields["topics"] == [
        {"id": "https://openalex.org/T1"},
        {"id": "https://openalex.org/T2"},
    ]
    assert (abstracts, topics) == (0, 0)
    assert second_fields == {}
    assert (second_abstracts, second_topics) == (0, 0)
