"""Recover OpenAlex abstracts and topics lost during earlier work merges."""

from copy import deepcopy
from itertools import islice

from pymongo import UpdateOne


def batched(iterable, size):
    iterator = iter(iterable)
    while batch := list(islice(iterator, size)):
        yield batch


def openalex_id(work):
    for external_id in work.get("external_ids", []):
        if external_id.get("source") == "openalex" and external_id.get("id"):
            return external_id["id"]
    return None


def build_openalex_enrichment(work, raw_work):
    """Return fields to set and counters for one matched work."""
    fields = {}
    recovered_abstract = 0
    recovered_topic = 0

    raw_abstract = raw_work.get("abstract_inverted_index")
    if not work.get("abstracts") and raw_abstract:
        fields["abstracts"] = [{
            "abstract": deepcopy(raw_abstract),
            "lang": raw_work.get("language") or "",
            "source": "openalex",
            "provenance": "openalex",
        }]
        recovered_abstract = 1

    current_primary = work.get("primary_topic") or {}
    raw_primary = raw_work.get("primary_topic") or {}
    if not current_primary.get("id") and raw_primary.get("id"):
        fields["primary_topic"] = deepcopy(raw_primary)
        recovered_topic = 1

    current_topics = list(work.get("topics") or [])
    merged_topics = deepcopy(current_topics)
    topic_ids = {
        topic.get("id") for topic in current_topics if isinstance(topic, dict)
    }
    for topic in raw_work.get("topics") or []:
        if not isinstance(topic, dict):
            continue
        topic_id = topic.get("id")
        if topic_id and topic_id not in topic_ids:
            merged_topics.append(deepcopy(topic))
            topic_ids.add(topic_id)
    if merged_topics != current_topics:
        fields["topics"] = merged_topics
    return fields, recovered_abstract, recovered_topic


def enrich_works_from_openalex(
    works,
    openalex_works,
    batch_size=1000,
    dry_run=False,
    verbose=0,
):
    """Recover missing metadata in bounded, idempotent batches."""
    if batch_size < 1:
        raise ValueError("openalex_enrichment_batch_size must be greater than zero")
    openalex_works.create_index("id", name="id_1")
    report = {
        "candidates": 0,
        "raw_matches": 0,
        "works_changed": 0,
        "abstracts_recovered": 0,
        "primary_topics_recovered": 0,
    }
    query = {
        "external_ids.source": "openalex",
        "$or": [
            {"abstracts.0": {"$exists": False}},
            {"primary_topic.id": {"$exists": False}},
            {"topics.0": {"$exists": False}},
        ],
    }
    projection = {
        "external_ids": 1, "abstracts": 1, "primary_topic": 1, "topics": 1,
    }
    cursor = works.find(query, projection, no_cursor_timeout=True).batch_size(
        batch_size
    )
    for batch in batched(cursor, batch_size):
        report["candidates"] += len(batch)
        identifiers = [
            identifier for identifier in map(openalex_id, batch) if identifier
        ]
        raw_by_id = {
            raw["id"]: raw
            for raw in openalex_works.find(
                {"id": {"$in": identifiers}},
                {
                    "id": 1, "abstract_inverted_index": 1, "language": 1,
                    "primary_topic": 1, "topics": 1,
                },
            ).hint("id_1")
        }
        operations = []
        for work in batch:
            raw = raw_by_id.get(openalex_id(work))
            if raw is None:
                continue
            report["raw_matches"] += 1
            fields, abstract_count, topic_count = build_openalex_enrichment(
                work, raw
            )
            if not fields:
                continue
            report["works_changed"] += 1
            report["abstracts_recovered"] += abstract_count
            report["primary_topics_recovered"] += topic_count
            if not dry_run:
                operations.append(UpdateOne(
                    {"_id": work["_id"]}, {"$set": fields}
                ))
        if operations:
            works.bulk_write(operations, ordered=False)
        if verbose and report["candidates"] % (batch_size * 10) == 0:
            print(
                "INFO: OpenAlex enrichment: "
                f"{report['candidates']} candidates, "
                f"{report['works_changed']} works changed"
            )
    return report
