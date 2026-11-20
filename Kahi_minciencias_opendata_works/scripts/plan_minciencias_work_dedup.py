#!/usr/bin/env python3
"""Build a read-only plan for MinCiencias/rich-work deduplication.

The historical database supplies the DOI bridge, while the current database
must contain exactly one work for that DOI.  Every proposed merge is then
corroborated against the live documents by a long exact title or a very close
title plus a shared author.  This script never writes to MongoDB.
"""

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
import gzip
import hashlib
import json
from pathlib import Path
import random
import re
import sqlite3
import unicodedata

from bson import ObjectId
from pymongo import MongoClient


MIN_EXACT_TITLE_LENGTH = 50
MIN_FUZZY_TITLE_LENGTH = 30
FUZZY_TITLE_THRESHOLD = 0.95
DOI_PATTERN = re.compile(r"^10\.\d{4,9}/\S+$", re.IGNORECASE)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--database", default="kahi_dev_092026_2")
    parser.add_argument("--identity-sqlite", type=Path, required=True)
    parser.add_argument("--plan-file", type=Path, required=True)
    parser.add_argument("--report-file", type=Path, required=True)
    parser.add_argument("--sample-file", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--progress-interval", type=int, default=10000)
    return parser.parse_args()


def normalize_title(value):
    value = unicodedata.normalize("NFKD", str(value or ""))
    value = "".join(char for char in value if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value.casefold()).split())


def normalize_doi(value):
    value = str(value or "").strip().casefold()
    value = re.sub(r"^(?:https?://)?(?:dx\.)?doi\.org/", "", value)
    value = re.sub(r"^doi\s*:\s*", "", value)
    return value.rstrip(" .")


def valid_doi(value):
    value = normalize_doi(value)
    return bool(DOI_PATTERN.match(value)) and "(issn)" not in value


def titles(document):
    return {
        normalized
        for item in document.get("titles") or []
        if (normalized := normalize_title(item.get("title")))
    }


def author_ids(document):
    return {
        str(item.get("id"))
        for item in document.get("authors") or []
        if item.get("id") not in (None, "")
    }


def author_names(document):
    return {
        normalized
        for item in document.get("authors") or []
        if (normalized := normalize_title(item.get("full_name")))
    }


def shared_author(left, right):
    return bool(
        author_ids(left) & author_ids(right)
        or author_names(left) & author_names(right)
    )


def object_id_source(document):
    source_id = (document.get("source") or {}).get("id")
    return source_id if isinstance(source_id, ObjectId) else None


def nonempty_date(document):
    value = document.get("date_published")
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def identity_hash(document):
    identity = {
        "_id": str(document["_id"]),
        "titles": document.get("titles") or [],
        "external_ids": document.get("external_ids") or [],
        "authors": [
            {"id": item.get("id"), "full_name": item.get("full_name")}
            for item in document.get("authors") or []
        ],
        "year_published": document.get("year_published"),
        "date_published": document.get("date_published"),
        "source_id": str(object_id_source(document) or ""),
        "doi": document.get("doi"),
        "updated": document.get("updated") or [],
    }
    encoded = json.dumps(identity, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def evaluate_pair(loser, survivor, doi):
    if not valid_doi(doi):
        return None, "invalid_doi", 0.0

    loser_doi = normalize_doi(loser.get("doi"))
    if loser_doi:
        return None, "loser_already_has_doi", 0.0

    loser_year = loser.get("year_published")
    survivor_year = survivor.get("year_published")
    if (
        isinstance(loser_year, int)
        and isinstance(survivor_year, int)
        and loser_year != survivor_year
    ):
        return None, "year_conflict", 0.0

    loser_date = nonempty_date(loser)
    survivor_date = nonempty_date(survivor)
    if loser_date and survivor_date and loser_date != survivor_date:
        return None, "date_conflict", 0.0

    loser_source = object_id_source(loser)
    survivor_source = object_id_source(survivor)
    if loser_source and survivor_source and loser_source != survivor_source:
        return None, "source_conflict", 0.0

    left_titles = titles(loser)
    right_titles = titles(survivor)
    exact = left_titles & right_titles
    if any(len(value) >= MIN_EXACT_TITLE_LENGTH for value in exact):
        if shared_author(loser, survivor):
            return "exact_long_title_shared_author", None, 1.0
        return None, "exact_title_without_shared_author", 1.0

    best = max(
        (
            SequenceMatcher(None, left, right).ratio()
            for left in left_titles
            for right in right_titles
            if min(len(left), len(right)) >= MIN_FUZZY_TITLE_LENGTH
        ),
        default=0.0,
    )
    if best >= FUZZY_TITLE_THRESHOLD and shared_author(loser, survivor):
        return "fuzzy_title_shared_author", None, best
    return None, "insufficient_title_author", best


def candidate_rows(connection, database):
    query = """
        WITH doi_counts AS (
            SELECT doi, count(*) AS works
            FROM new_doi
            GROUP BY doi
        )
        SELECT old.pid, old.doi, current.work_id, rich.work_id
        FROM old_minc_doi AS old
        JOIN records AS current
          ON current.pid = old.pid AND current.db = ?
        JOIN doi_counts AS counts
          ON counts.doi = old.doi AND counts.works = 1
        JOIN new_doi AS rich ON rich.doi = old.doi
        WHERE current.work_id != rich.work_id
        ORDER BY current.work_id, rich.work_id, old.pid
    """
    return connection.execute(query, (database,))


def chunks(iterator, size):
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def compact_sample(document):
    return {
        "_id": str(document["_id"]),
        "titles": [item.get("title") for item in (document.get("titles") or [])[:5]],
        "year_published": document.get("year_published"),
        "date_published": document.get("date_published"),
        "source_id": str(object_id_source(document) or ""),
        "doi": document.get("doi"),
        "author_ids": sorted(author_ids(document))[:10],
    }


def reservoir_add(samples, key, value, count, rng, limit=10):
    bucket = samples[key]
    if len(bucket) < limit:
        bucket.append(value)
        return
    index = rng.randrange(count)
    if index < limit:
        bucket[index] = value


def current_counts(works):
    return {
        "total": works.estimated_document_count(),
        "year_numeric": works.count_documents(
            {"year_published": {"$type": "number"}}, hint="year_published_1"
        ),
        "source_objectid": works.count_documents(
            {"source.id": {"$type": "objectId"}}, hint="source.id_1"
        ),
    }


def projected_counts(baseline, groups, documents):
    result = dict(baseline)
    result["total"] -= sum(len(group["losers"]) for group in groups)
    for field, output_key, predicate in (
        ("year_published", "year_numeric", lambda value: isinstance(value, (int, float)) and not isinstance(value, bool)),
        ("source", "source_objectid", lambda value: isinstance((value or {}).get("id"), ObjectId)),
    ):
        reduction = 0
        for group in groups:
            members = [group["survivor_id"]] + [item["work_id"] for item in group["losers"]]
            before = sum(bool(predicate(documents[member].get(field))) for member in members)
            after = int(any(predicate(documents[member].get(field)) for member in members))
            reduction += before - after
        result[output_key] -= reduction
    return result


def main():
    args = parse_args()
    if args.batch_size < 1 or args.progress_interval < 1:
        raise ValueError("batch size and progress interval must be positive")
    if not args.identity_sqlite.is_file():
        raise FileNotFoundError(args.identity_sqlite)

    client = MongoClient(args.mongo_uri)
    client.admin.command("ping")
    works = client[args.database].works
    projection = {
        "titles": 1,
        "authors.id": 1,
        "authors.full_name": 1,
        "year_published": 1,
        "date_published": 1,
        "source.id": 1,
        "doi": 1,
        "external_ids": 1,
        "updated": 1,
    }
    counters = Counter()
    samples = defaultdict(list)
    rng = random.Random(20260929)
    edges = {}
    documents = {}

    connection = sqlite3.connect(args.identity_sqlite)
    try:
        for batch in chunks(candidate_rows(connection, args.database), args.batch_size):
            ids = {ObjectId(value) for row in batch for value in row[2:]}
            fetched = {
                str(item["_id"]): item
                for item in works.find({"_id": {"$in": list(ids)}}, projection)
            }
            documents.update(fetched)
            for pid, doi, loser_id, survivor_id in batch:
                counters["candidate_rows"] += 1
                loser = fetched.get(loser_id)
                survivor = fetched.get(survivor_id)
                if not loser or not survivor:
                    reason = "missing_document"
                    evidence = None
                    similarity = 0.0
                else:
                    evidence, reason, similarity = evaluate_pair(loser, survivor, doi)
                key = evidence or reason
                counters[key] += 1
                sample = {
                    "pid": pid,
                    "doi": doi,
                    "similarity": round(similarity, 6),
                    "loser": compact_sample(loser) if loser else None,
                    "survivor": compact_sample(survivor) if survivor else None,
                }
                reservoir_add(samples, key, sample, counters[key], rng)
                if not evidence:
                    continue
                edge = edges.setdefault(
                    (loser_id, survivor_id),
                    {"loser_id": loser_id, "survivor_id": survivor_id, "pids": set(), "dois": set(), "evidence": set()},
                )
                edge["pids"].add(pid)
                edge["dois"].add(normalize_doi(doi))
                edge["evidence"].add(evidence)
            if counters["candidate_rows"] % args.progress_interval < len(batch):
                print(
                    f"INFO: candidates={counters['candidate_rows']} accepted_edges={len(edges)}",
                    flush=True,
                )
    finally:
        connection.close()

    loser_targets = defaultdict(set)
    for loser_id, survivor_id in edges:
        loser_targets[loser_id].add(survivor_id)
    ambiguous_losers = {loser for loser, targets in loser_targets.items() if len(targets) != 1}
    survivor_ids = {survivor for _, survivor in edges}
    role_conflicts = set(loser_targets) & survivor_ids
    accepted_edges = []
    for edge in edges.values():
        if edge["loser_id"] in ambiguous_losers:
            counters["excluded_ambiguous_loser"] += 1
            continue
        if edge["loser_id"] in role_conflicts or edge["survivor_id"] in role_conflicts:
            counters["excluded_role_conflict"] += 1
            continue
        accepted_edges.append(edge)

    by_survivor = defaultdict(list)
    for edge in accepted_edges:
        by_survivor[edge["survivor_id"]].append(edge)
    groups = []
    for survivor_id, group_edges in sorted(by_survivor.items()):
        survivor = documents[survivor_id]
        groups.append(
            {
                "survivor_id": survivor_id,
                "survivor_identity_hash": identity_hash(survivor),
                "losers": [
                    {
                        "work_id": edge["loser_id"],
                        "identity_hash": identity_hash(documents[edge["loser_id"]]),
                        "product_ids": sorted(edge["pids"]),
                        "dois": sorted(edge["dois"]),
                        "evidence": sorted(edge["evidence"]),
                    }
                    for edge in sorted(group_edges, key=lambda value: value["loser_id"])
                ],
            }
        )

    args.plan_file.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(args.plan_file, "wt", encoding="utf-8") as stream:
        for group in groups:
            stream.write(json.dumps(group, ensure_ascii=False) + "\n")

    baseline = current_counts(works)
    projected = projected_counts(baseline, groups, documents)
    counters["unique_evidence_edges"] = len(edges)
    counters["planned_groups"] = len(groups)
    counters["planned_losers"] = sum(len(group["losers"]) for group in groups)
    report = {
        "mode": "dry-run",
        "database": args.database,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "plan_file": str(args.plan_file),
        "counters": dict(sorted(counters.items())),
        "baseline": baseline,
        "projected_after_merge": projected,
        "rules": {
            "current_doi_must_be_unique": True,
            "minimum_exact_title_length": MIN_EXACT_TITLE_LENGTH,
            "minimum_fuzzy_title_length": MIN_FUZZY_TITLE_LENGTH,
            "fuzzy_title_threshold": FUZZY_TITLE_THRESHOLD,
            "all_title_matches_require_shared_author": True,
            "reject_year_date_source_conflicts": True,
            "reject_graph_role_conflicts": True,
            "mongo_writes": False,
        },
    }
    args.report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    args.sample_file.write_text(json.dumps(samples, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
