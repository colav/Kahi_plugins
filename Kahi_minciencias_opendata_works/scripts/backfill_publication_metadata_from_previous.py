#!/usr/bin/env python3
"""Plan or apply a conservative publication metadata backfill.

The plan joins old and new works through the stable MinCiencias product ID.
It never overwrites populated values and excludes old works containing more
than one MinCiencias product ID.  Source ObjectIds are resolved again against
the target database through source-qualified external identifiers.
"""

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
import gzip
import json
from pathlib import Path
import re
import sqlite3
import unicodedata

from bson import ObjectId
from pymongo import MongoClient, UpdateOne


MIN_TITLE_LENGTH = 30
FUZZY_TITLE_THRESHOLD = 0.95


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--previous-db", default="kahi_dev_062026")
    parser.add_argument("--target-db", default="kahi_dev_092026_2")
    parser.add_argument("--identity-sqlite", type=Path, required=True)
    parser.add_argument("--plan-file", type=Path, required=True)
    parser.add_argument("--report-file", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--progress-interval", type=int, default=10000)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def normalized_title(value):
    value = unicodedata.normalize("NFKD", str(value or ""))
    value = "".join(char for char in value if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value.casefold()).split())


def comparable_id(value):
    if value in (None, "", {}, []):
        return None
    if isinstance(value, ObjectId):
        value = str(value)
    try:
        encoded = json.dumps(value, sort_keys=True, ensure_ascii=False)
    except TypeError:
        encoded = str(value)
    return encoded


def external_id_keys(document, exclude_minciencias_provenance=False):
    keys = set()
    for item in document.get("external_ids") or []:
        if exclude_minciencias_provenance and item.get("provenance") == "minciencias":
            continue
        source = str(item.get("source") or "")
        identifier = comparable_id(item.get("id"))
        if source and identifier:
            keys.add((source, identifier))
    return keys


def author_ids(document):
    return {
        str(author.get("id"))
        for author in document.get("authors") or []
        if author.get("id") not in (None, "")
    }


def strong_identity_evidence(previous, target):
    shared_ids = (
        external_id_keys(previous, exclude_minciencias_provenance=True)
        & external_id_keys(target, exclude_minciencias_provenance=True)
    )
    if shared_ids:
        return "shared_external_id"

    previous_titles = {
        normalized_title(item.get("title"))
        for item in previous.get("titles") or []
        if item.get("source") != "minciencias"
    }
    target_titles = {
        normalized_title(item.get("title"))
        for item in target.get("titles") or []
    }
    previous_titles.discard("")
    target_titles.discard("")
    exact = previous_titles & target_titles
    if any(len(title) >= MIN_TITLE_LENGTH for title in exact):
        return "exact_rich_title"

    if not (author_ids(previous) & author_ids(target)):
        return None
    for left in previous_titles:
        for right in target_titles:
            if min(len(left), len(right)) < MIN_TITLE_LENGTH:
                continue
            if SequenceMatcher(None, left, right).ratio() >= FUZZY_TITLE_THRESHOLD:
                return "fuzzy_title_and_author"
    return None


def source_index(collection):
    by_external_id = defaultdict(set)
    documents = {}
    projection = {"external_ids": 1, "names": 1}
    for source in collection.find({}, projection):
        source_id = source["_id"]
        documents[source_id] = source
        for key in external_id_keys(source):
            by_external_id[key].add(source_id)
    return by_external_id, documents


def preferred_source_name(source):
    names = source.get("names") or []
    if not names:
        return ""
    default = names[0].get("name", "")
    for language in ("es", "en"):
        name = next((
            item.get("name", "") for item in names
            if item.get("lang") == language and item.get("name")
        ), None)
        if name:
            return name
    return default


def build_source_crosswalk(previous_sources, target_sources):
    target_index, target_documents = source_index(target_sources)
    crosswalk = {}
    ambiguous = 0
    for source in previous_sources.find({}, {"external_ids": 1}):
        candidates = set()
        for key in external_id_keys(source):
            candidates.update(target_index.get(key, ()))
        if len(candidates) == 1:
            target_id = next(iter(candidates))
            target = target_documents[target_id]
            crosswalk[source["_id"]] = {
                "id": target_id,
                "name": preferred_source_name(target),
            }
        elif len(candidates) > 1:
            ambiguous += 1
    return crosswalk, ambiguous


def candidate_rows(connection, previous_db, target_db):
    query = """
        WITH previous_counts AS (
            SELECT work_id, count(*) AS products
            FROM records
            WHERE db = ?
            GROUP BY work_id
        )
        SELECT old.pid, old.work_id, new.work_id
        FROM records AS old
        JOIN records AS new USING(pid)
        JOIN previous_counts AS counts ON counts.work_id = old.work_id
        WHERE old.db = ?
          AND new.db = ?
          AND counts.products = 1
          AND (
            (old.has_year = 1 AND new.has_year = 0)
            OR
            (old.source_type = 'objectId' AND new.source_type != 'objectId')
          )
    """
    return connection.execute(
        query, (previous_db, previous_db, target_db))


def chunks(iterator, size):
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def valid_year(value):
    current_year = datetime.now(timezone.utc).year
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 1500 <= value <= current_year
    )


def valid_date(value):
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def target_source_is_missing(document):
    source = document.get("source") or {}
    return not isinstance(source.get("id"), ObjectId)


def propose_fields(previous, target, source_crosswalk):
    fields = {}
    if not valid_year(target.get("year_published")) and valid_year(
            previous.get("year_published")):
        fields["year_published"] = previous["year_published"]
        if (
            target.get("date_published") in (None, "", 0)
            and valid_date(previous.get("date_published"))
        ):
            fields["date_published"] = previous["date_published"]

    if target_source_is_missing(target):
        old_source = (previous.get("source") or {}).get("id")
        mapped_source = source_crosswalk.get(old_source)
        if mapped_source:
            fields["source"] = mapped_source
    return fields


def serializable_fields(fields):
    result = dict(fields)
    if "source" in result:
        result["source"] = dict(result["source"])
        result["source"]["id"] = str(result["source"]["id"])
    return result


def merge_proposal(proposals, target_id, pid, previous_id, fields, evidence,
                   counters):
    proposal = proposals.setdefault(target_id, {
        "work_id": target_id,
        "product_ids": [],
        "previous_work_ids": [],
        "evidence": [],
        "field_candidates": defaultdict(set),
    })
    proposal["product_ids"].append(pid)
    proposal["previous_work_ids"].append(previous_id)
    proposal["evidence"].append(evidence)
    for field, value in serializable_fields(fields).items():
        proposal["field_candidates"][field].add(
            json.dumps(value, sort_keys=True, ensure_ascii=False))
    counters["accepted_product_ids"] += 1


def finalize_proposals(proposals, counters):
    finalized = []
    for proposal in proposals.values():
        fields = {}
        conflict = False
        for field, values in proposal.pop("field_candidates").items():
            if len(values) != 1:
                counters[f"conflict_{field}"] += 1
                conflict = True
                continue
            fields[field] = json.loads(next(iter(values)))
        if conflict or not fields:
            counters["skipped_conflicting_works"] += 1
            continue
        proposal["set"] = fields
        proposal["product_ids"] = sorted(set(proposal["product_ids"]))
        proposal["previous_work_ids"] = sorted(
            set(proposal["previous_work_ids"]))
        proposal["evidence"] = sorted(set(proposal["evidence"]))
        finalized.append(proposal)
        counters["planned_works"] += 1
        for field in fields:
            counters[f"planned_{field}"] += 1
    return finalized


def write_plan(path, proposals):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        for proposal in proposals:
            stream.write(json.dumps(proposal, ensure_ascii=False) + "\n")


def create_plan(args, client):
    previous_db = client[args.previous_db]
    target_db = client[args.target_db]
    crosswalk, ambiguous_sources = build_source_crosswalk(
        previous_db.sources, target_db.sources)
    counters = Counter({
        "source_crosswalk": len(crosswalk),
        "ambiguous_previous_sources": ambiguous_sources,
    })
    proposals = {}
    projection = {
        "titles": 1, "external_ids": 1, "authors.id": 1,
        "year_published": 1, "date_published": 1, "source": 1,
    }
    connection = sqlite3.connect(args.identity_sqlite)
    try:
        rows = candidate_rows(connection, args.previous_db, args.target_db)
        for batch in chunks(rows, args.batch_size):
            previous_ids = [ObjectId(row[1]) for row in batch]
            target_ids = [ObjectId(row[2]) for row in batch]
            previous = {
                str(item["_id"]): item
                for item in previous_db.works.find(
                    {"_id": {"$in": previous_ids}}, projection)
            }
            target = {
                str(item["_id"]): item
                for item in target_db.works.find(
                    {"_id": {"$in": target_ids}}, projection)
            }
            for pid, previous_id, target_id in batch:
                counters["candidate_product_ids"] += 1
                old = previous.get(previous_id)
                new = target.get(target_id)
                if not old or not new:
                    counters["missing_documents"] += 1
                    continue
                evidence = strong_identity_evidence(old, new)
                if not evidence:
                    counters["insufficient_evidence"] += 1
                    continue
                fields = propose_fields(old, new, crosswalk)
                if not fields:
                    counters["no_safe_fields"] += 1
                    continue
                merge_proposal(
                    proposals, target_id, pid, previous_id, fields, evidence,
                    counters)
            if (
                counters["candidate_product_ids"]
                % args.progress_interval < len(batch)
            ):
                print(
                    "INFO: candidates={} accepted_ids={} planned_targets={}".format(
                        counters["candidate_product_ids"],
                        counters["accepted_product_ids"],
                        len(proposals),
                    ),
                    flush=True,
                )
    finally:
        connection.close()
    finalized = finalize_proposals(proposals, counters)
    write_plan(args.plan_file, finalized)
    return counters


def decoded_fields(fields):
    result = dict(fields)
    if "source" in result:
        result["source"] = dict(result["source"])
        result["source"]["id"] = ObjectId(result["source"]["id"])
    return result


def apply_plan(args, client):
    collection = client[args.target_db].works
    counters = Counter()
    operations = []
    with gzip.open(args.plan_file, "rt", encoding="utf-8") as stream:
        for line in stream:
            proposal = json.loads(line)
            counters["plan_rows"] += 1
            work_id = ObjectId(proposal["work_id"])
            current = collection.find_one(
                {"_id": work_id},
                {"year_published": 1, "date_published": 1, "source.id": 1},
            )
            if not current:
                counters["missing_targets"] += 1
                continue
            proposed = decoded_fields(proposal["set"])
            fields = {}
            if (
                "year_published" in proposed
                and not valid_year(current.get("year_published"))
            ):
                fields["year_published"] = proposed["year_published"]
            if (
                "date_published" in proposed
                and current.get("date_published") in (None, "", 0)
            ):
                fields["date_published"] = proposed["date_published"]
            if "source" in proposed and target_source_is_missing(current):
                fields["source"] = proposed["source"]
            if not fields:
                counters["already_complete"] += 1
                continue
            operations.append(UpdateOne({"_id": work_id}, {"$set": fields}))
            for field in fields:
                counters[f"applied_{field}"] += 1
            if len(operations) >= args.batch_size:
                result = collection.bulk_write(operations, ordered=False)
                counters["modified_works"] += result.modified_count
                operations = []
                print(
                    "INFO: plan_rows={} modified={}".format(
                        counters["plan_rows"], counters["modified_works"]),
                    flush=True,
                )
    if operations:
        result = collection.bulk_write(operations, ordered=False)
        counters["modified_works"] += result.modified_count
    return counters


def main():
    args = parse_args()
    if args.batch_size < 1 or args.progress_interval < 1:
        raise ValueError("batch size and progress interval must be positive")
    if not args.identity_sqlite.is_file():
        raise FileNotFoundError(args.identity_sqlite)
    if args.apply and not args.plan_file.is_file():
        raise FileNotFoundError(args.plan_file)

    client = MongoClient(args.mongo_uri)
    client.admin.command("ping")
    started = datetime.now(timezone.utc)
    counters = (
        apply_plan(args, client) if args.apply else create_plan(args, client)
    )
    report = {
        "mode": "apply" if args.apply else "dry-run",
        "previous_database": args.previous_db,
        "target_database": args.target_db,
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "plan_file": str(args.plan_file),
        "counters": dict(sorted(counters.items())),
        "rules": {
            "old_work_minciencias_ids": 1,
            "minimum_title_length": MIN_TITLE_LENGTH,
            "fuzzy_title_threshold": FUZZY_TITLE_THRESHOLD,
            "never_overwrite_populated_fields": True,
        },
    }
    args.report_file.parent.mkdir(parents=True, exist_ok=True)
    args.report_file.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
