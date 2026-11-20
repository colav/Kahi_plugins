"""Aggregate work topics into compact topic profiles for identified persons."""

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from copy import deepcopy
from datetime import datetime, timezone
from itertools import islice
from math import isfinite

from pymongo import ReplaceOne, UpdateOne


PERSON_TOPICS_VERSION = "person-topics-v1"
IDENTITY_DOCUMENT_SOURCES = frozenset({
    "Cédula de Ciudadanía",
    "Cédula de Extranjería",
    "Pasaporte",
    "Passport",
    "Tarjeta de Identidad",
})
ORCID_SOURCES = frozenset({"orcid", "ORCID"})


def utc_now():
    return datetime.now(timezone.utc)


def batched(iterable, size):
    iterator = iter(iterable)
    while batch := list(islice(iterator, size)):
        yield batch


def person_identity_evidence(person):
    """Return the accepted identity evidence labels for one person."""
    evidence = set()
    for external_id in person.get("external_ids") or []:
        if not isinstance(external_id, dict):
            continue
        identifier = external_id.get("id")
        if isinstance(identifier, dict):
            cod_rh = str(identifier.get("COD_RH") or "").strip()
            if cod_rh and cod_rh != "0000000000":
                evidence.add("COD_RH")
        if external_id.get("source") in IDENTITY_DOCUMENT_SOURCES:
            document = identifier.strip() if isinstance(identifier, str) else ""
            if document:
                evidence.add("identity_document")
        if external_id.get("source") in ORCID_SOURCES:
            orcid = identifier.strip() if isinstance(identifier, str) else ""
            if orcid:
                evidence.add("ORCID")
    return tuple(sorted(evidence))


def eligible_person_query():
    """Use indexed external identifiers to avoid scanning unrelated persons."""
    return {
        "$or": [
            {
                "external_ids": {
                    "$elemMatch": {
                        "id.COD_RH": {
                            "$exists": True,
                            "$nin": [None, "", "0000000000"],
                        }
                    }
                }
            },
            {
                "external_ids": {
                    "$elemMatch": {
                        "source": {"$in": sorted(IDENTITY_DOCUMENT_SOURCES)},
                        "id": {"$type": "string", "$nin": [None, ""]},
                    }
                }
            },
            {
                "external_ids": {
                    "$elemMatch": {
                        "source": {"$in": sorted(ORCID_SOURCES)},
                        "id": {"$type": "string", "$nin": [None, ""]},
                    }
                }
            },
        ]
    }


def canonical_topic(topic):
    """Return the exact Kahi topic shape, or None for incomplete metadata."""
    if not isinstance(topic, dict):
        return None
    topic_id = topic.get("id")
    display_name = topic.get("display_name")
    if not isinstance(topic_id, str) or not topic_id.strip():
        return None
    if not isinstance(display_name, str) or not display_name.strip():
        return None

    nested = {}
    for field in ("domain", "field", "subfield"):
        value = topic.get(field)
        if not isinstance(value, dict):
            return None
        value_id = value.get("id")
        value_name = value.get("display_name")
        if not isinstance(value_id, str) or not value_id.strip():
            return None
        if not isinstance(value_name, str) or not value_name.strip():
            return None
        nested[field] = {"id": value_id, "display_name": value_name}

    try:
        score = float(topic.get("score"))
    except (TypeError, ValueError):
        return None
    if not isfinite(score) or score < 0:
        return None
    return {
        "id": topic_id,
        "display_name": display_name,
        "domain": nested["domain"],
        "field": nested["field"],
        "subfield": nested["subfield"],
        "score": score,
    }


def build_person_topic_profile(
    topic_stats,
    *,
    min_works=2,
    limit=5,
    singleton_min_score=0.9,
):
    """Build primary_topic/topics using normalized accumulated confidence."""
    if min_works < 1:
        raise ValueError("person_topics_min_works must be greater than zero")
    if limit < 1:
        raise ValueError("person_topics_limit must be greater than zero")
    if not 0 <= singleton_min_score <= 1:
        raise ValueError(
            "person_topics_singleton_min_score must be between zero and one"
        )

    total_weight = sum(item["weight"] for item in topic_stats.values())
    total_works = sum(item["works_count"] for item in topic_stats.values())
    repeated = []
    singletons = []
    for topic_id, item in topic_stats.items():
        works_count = item["works_count"]
        average_score = item["weight"] / works_count
        is_repeated = works_count >= min_works
        is_accepted_singleton = (
            works_count == 1 and average_score >= singleton_min_score
        )
        if not is_repeated and not is_accepted_singleton:
            continue
        denominator = total_weight if total_weight > 0 else total_works
        numerator = item["weight"] if total_weight > 0 else item["works_count"]
        topic = deepcopy(item["topic"])
        topic["score"] = round(numerator / denominator, 6)
        ranked_item = (
            topic,
            works_count,
            average_score,
            topic_id,
        )
        (repeated if is_repeated else singletons).append(ranked_item)

    ranking_key = lambda item: (  # noqa: E731
        -item[0]["score"], -item[1], -item[2], item[3]
    )
    repeated.sort(key=ranking_key)
    singletons.sort(key=ranking_key)
    selected = repeated[:limit]
    selected.extend(singletons[:max(0, limit - len(selected))])
    selected.sort(key=ranking_key)
    topics = [item[0] for item in selected]
    overflow = round(sum(topic["score"] for topic in topics) - 1.0, 6)
    if topics and overflow > 0:
        topics[-1]["score"] = round(
            max(0.0, topics[-1]["score"] - overflow), 6
        )
    return {
        "primary_topic": deepcopy(topics[0]) if topics else {},
        "topics": topics,
    }


def _replace_many(collection, operations):
    if not operations:
        return 0
    try:
        return collection.bulk_write(operations, ordered=False).modified_count
    except TypeError:
        modified = 0
        for operation in operations:
            result = collection.replace_one(
                operation._filter, operation._doc, upsert=True
            )
            modified += result.modified_count + int(result.upserted_id is not None)
        return modified


def _update_many(collection, operations):
    if not operations:
        return type("BulkResult", (), {"matched_count": 0, "modified_count": 0})()
    try:
        return collection.bulk_write(operations, ordered=False)
    except TypeError:
        matched = 0
        modified = 0
        for operation in operations:
            result = collection.update_one(operation._filter, operation._doc)
            matched += result.matched_count
            modified += result.modified_count
        return type(
            "BulkResult", (),
            {"matched_count": matched, "modified_count": modified},
        )()


def _process_person_batch(
    works,
    people,
    min_works,
    limit,
    singleton_min_score,
    run_context,
):
    person_ids = [person["_id"] for person in people]
    person_id_set = set(person_ids)
    topic_stats = {person_id: {} for person_id in person_ids}
    batch_stats = {
        "work_batch_hits": 0,
        "person_work_edges": 0,
        "valid_topic_edges": 0,
        "invalid_topic_edges": 0,
    }
    cursor = works.find(
        {
            "authors.id": {"$in": person_ids},
            "primary_topic.id": {"$exists": True},
        },
        {"authors.id": 1, "primary_topic": 1},
    )
    for work in cursor:
        batch_stats["work_batch_hits"] += 1
        author_ids = set()
        for author in work.get("authors") or []:
            if not isinstance(author, dict):
                continue
            author_id = author.get("id")
            try:
                if author_id in person_id_set:
                    author_ids.add(author_id)
            except TypeError:
                continue
        if not author_ids:
            continue
        batch_stats["person_work_edges"] += len(author_ids)
        topic = canonical_topic(work.get("primary_topic"))
        if topic is None:
            batch_stats["invalid_topic_edges"] += len(author_ids)
            continue
        for person_id in author_ids:
            item = topic_stats[person_id].setdefault(topic["id"], {
                "topic": topic,
                "works_count": 0,
                "weight": 0.0,
            })
            item["works_count"] += 1
            item["weight"] += topic["score"]
            batch_stats["valid_topic_edges"] += 1

    documents = []
    for person in people:
        person_id = person["_id"]
        stats = topic_stats[person_id]
        profile = build_person_topic_profile(
            stats,
            min_works=min_works,
            limit=limit,
            singleton_min_score=singleton_min_score,
        )
        documents.append({
            "_id": person_id,
            "run_id": run_context["run_id"],
            "calculated_at": run_context["calculated_at"],
            "version": PERSON_TOPICS_VERSION,
            "provenance": "works.primary_topic",
            "eligibility": list(person["eligibility"]),
            "works_with_valid_topics": sum(
                item["works_count"] for item in stats.values()
            ),
            "primary_topic": profile["primary_topic"],
            "topics": profile["topics"],
        })
    batch_stats["persons_with_topics"] = sum(
        bool(document["topics"]) for document in documents
    )
    for person, document in zip(people, documents):
        stats = topic_stats[person["_id"]]
        singleton_ids = {
            topic_id for topic_id, item in stats.items()
            if item["works_count"] == 1
        }
        selected_ids = {topic["id"] for topic in document["topics"]}
        singleton_count = len(selected_ids & singleton_ids)
        batch_stats.setdefault("singleton_topics_selected", 0)
        batch_stats.setdefault("repeated_topics_selected", 0)
        batch_stats.setdefault("persons_with_singleton_topics", 0)
        batch_stats["singleton_topics_selected"] += singleton_count
        batch_stats["repeated_topics_selected"] += (
            len(selected_ids) - singleton_count
        )
        batch_stats["persons_with_singleton_topics"] += int(
            singleton_count > 0
        )
    return documents, batch_stats


def _write_staging_batch(staging, documents):
    return _replace_many(staging, [
        ReplaceOne({"_id": document["_id"]}, document, upsert=True)
        for document in documents
    ])


def _apply_staging(persons, staging, batch_size):
    matched = 0
    modified = 0
    cursor = staging.find({}, {"primary_topic": 1, "topics": 1}).batch_size(
        batch_size
    )
    for batch in batched(cursor, batch_size):
        operations = []
        for document in batch:
            topics = document.get("topics") or []
            update = (
                {"$set": {
                    "primary_topic": document["primary_topic"],
                    "topics": topics,
                }}
                if topics
                else {"$unset": {"primary_topic": "", "topics": ""}}
            )
            operations.append(UpdateOne({"_id": document["_id"]}, update))
        result = _update_many(persons, operations)
        matched += result.matched_count
        modified += result.modified_count
    return matched, modified


def aggregate_person_topics(
    persons,
    works,
    staging,
    runs,
    *,
    dry_run=True,
    jobs=16,
    batch_size=1000,
    min_works=2,
    limit=5,
    singleton_min_score=0.9,
    progress_interval=10000,
    verbose=0,
):
    """Stage and optionally apply work-topic profiles for identified persons."""
    if jobs < 1:
        raise ValueError("person_topics_jobs must be greater than zero")
    if batch_size < 1:
        raise ValueError("person_topics_batch_size must be greater than zero")
    if min_works < 1:
        raise ValueError("person_topics_min_works must be greater than zero")
    if limit < 1:
        raise ValueError("person_topics_limit must be greater than zero")
    if not 0 <= singleton_min_score <= 1:
        raise ValueError(
            "person_topics_singleton_min_score must be between zero and one"
        )
    if progress_interval < 1:
        raise ValueError(
            "person_topics_progress_interval must be greater than zero"
        )

    started_at = utc_now()
    runs.create_index("started_at")
    staging.create_index("run_id")
    staging.create_index("topics.id")
    run = {
        "started_at": started_at,
        "finished_at": None,
        "status": "RUNNING",
        "dry_run": dry_run,
        "version": PERSON_TOPICS_VERSION,
        "provenance": "works.primary_topic",
        "target": {
            "database": persons.database.name,
            "collection": persons.name,
        },
        "staging": {
            "database": staging.database.name,
            "collection": staging.name,
        },
        "parameters": {
            "jobs": jobs,
            "batch_size": batch_size,
            "min_works": min_works,
            "limit": limit,
            "singleton_min_score": singleton_min_score,
        },
    }
    run_id = runs.insert_one(run).inserted_id
    run_context = {"run_id": run_id, "calculated_at": started_at}
    stats = {
        "eligible_persons": 0,
        "eligible_by_cod_rh": 0,
        "eligible_by_document": 0,
        "eligible_by_orcid": 0,
        "eligible_by_multiple": 0,
        "work_batch_hits": 0,
        "person_work_edges": 0,
        "valid_topic_edges": 0,
        "invalid_topic_edges": 0,
        "persons_with_topics": 0,
        "persons_with_singleton_topics": 0,
        "singleton_topics_selected": 0,
        "repeated_topics_selected": 0,
        "persons_without_topics": 0,
        "staged_persons": 0,
        "persons_matched": 0,
        "persons_modified": 0,
    }

    try:
        staging.delete_many({})
        person_cursor = persons.find(
            eligible_person_query(), {"external_ids": 1}
        ).batch_size(batch_size)

        def eligible_batches():
            for raw_batch in batched(person_cursor, batch_size):
                people = []
                for person in raw_batch:
                    evidence = person_identity_evidence(person)
                    if not evidence:
                        continue
                    stats["eligible_persons"] += 1
                    has_cod_rh = "COD_RH" in evidence
                    has_document = "identity_document" in evidence
                    has_orcid = "ORCID" in evidence
                    stats["eligible_by_cod_rh"] += int(has_cod_rh)
                    stats["eligible_by_document"] += int(has_document)
                    stats["eligible_by_orcid"] += int(has_orcid)
                    stats["eligible_by_multiple"] += int(len(evidence) > 1)
                    people.append({
                        "_id": person["_id"], "eligibility": evidence,
                    })
                if people:
                    yield people

        def consume(future):
            documents, batch_stats = future.result()
            _write_staging_batch(staging, documents)
            stats["staged_persons"] += len(documents)
            for key, value in batch_stats.items():
                stats[key] += value
            if (
                verbose
                and stats["staged_persons"] % progress_interval < len(documents)
            ):
                print(
                    "INFO: person topics: "
                    f"{stats['staged_persons']} persons staged; "
                    f"{stats['persons_with_topics']} with topics",
                    flush=True,
                )

        pending = set()
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            for people in eligible_batches():
                pending.add(executor.submit(
                    _process_person_batch,
                    works,
                    people,
                    min_works,
                    limit,
                    singleton_min_score,
                    run_context,
                ))
                if len(pending) >= jobs * 2:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        consume(future)
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    consume(future)

        stats["persons_without_topics"] = (
            stats["staged_persons"] - stats["persons_with_topics"]
        )
        if not dry_run:
            stats["persons_matched"], stats["persons_modified"] = (
                _apply_staging(persons, staging, batch_size)
            )
        runs.update_one(
            {"_id": run_id},
            {"$set": {
                "finished_at": utc_now(),
                "status": "COMPLETED",
                "stats": stats,
            }},
        )
    except Exception as error:
        runs.update_one(
            {"_id": run_id},
            {"$set": {
                "finished_at": utc_now(),
                "status": "FAILED",
                "stats": stats,
                "error": repr(error),
            }},
        )
        raise

    stats["run_id"] = run_id
    return stats
