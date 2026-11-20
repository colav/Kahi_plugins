#!/usr/bin/env python3
"""Simulate or apply an audited MinCiencias/rich-work deduplication plan."""

import argparse
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import gzip
import importlib.util
import json
from pathlib import Path

from bson import BSON, ObjectId
from pymongo import MongoClient, ReplaceOne

from kahi.KahiBase import KahiBase
from kahi_impactu_utils.Mongo import order_document


MAX_BSON_BYTES = 15_500_000
GENERIC_PRESERVED_LISTS = (
    "titles",
    "updated",
    "abstracts",
    "keywords",
    "types",
    "external_ids",
    "external_urls",
    "references",
    "citations",
    "ranking",
    "subjects",
    "rights",
)


def load_planner():
    path = Path(__file__).with_name("plan_minciencias_work_dedup.py")
    spec = importlib.util.spec_from_file_location("dedup_planner", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


planner = load_planner()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--database", default="kahi_dev_092026_2")
    parser.add_argument("--plan-file", type=Path, required=True)
    parser.add_argument("--report-file", type=Path, required=True)
    parser.add_argument("--sample-file", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--progress-interval", type=int, default=1000)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def typed_json(value):
    if isinstance(value, ObjectId):
        return {"$objectId": str(value)}
    raise TypeError(type(value).__name__)


def canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=typed_json,
    )


def is_empty(value):
    return value is None or value == "" or value == [] or value == {}


def list_identity(item, path):
    field = path[-1] if path else ""
    if not isinstance(item, dict):
        return ("value", canonical(item))
    if field == "authors":
        if item.get("id") not in (None, ""):
            return ("id", canonical(item["id"]))
        name = planner.normalize_title(item.get("full_name"))
        return ("name", name) if name else ("value", canonical(item))
    if field == "groups":
        if item.get("id") not in (None, ""):
            return ("id", canonical(item["id"]))
        name = planner.normalize_title(item.get("name"))
        return ("name", name) if name else ("value", canonical(item))
    if field == "citations_count":
        return ("source", item.get("source"), item.get("provenance"))
    if field == "citations_by_year":
        return (
            "year",
            item.get("year"),
            item.get("source"),
            item.get("provenance"),
        )
    if field == "topics" and item.get("id") not in (None, ""):
        return ("id", canonical(item["id"]))
    return ("value", canonical(item))


def prefer_maximum(path):
    return path[-1:] in (("count",), ("score",)) or path[-1:] in (
        ("references_count",),
        ("citations_count_openalex",),
    )


def merge_lists(existing, incoming, path, conflicts):
    result = deepcopy(existing)
    positions = {list_identity(item, path): index for index, item in enumerate(result)}
    special = path[-1:] in (
        ("authors",),
        ("groups",),
        ("citations_count",),
        ("citations_by_year",),
        ("topics",),
    )
    for item in incoming:
        identity = list_identity(item, path)
        if identity not in positions:
            positions[identity] = len(result)
            result.append(deepcopy(item))
        elif special and isinstance(item, dict) and isinstance(result[positions[identity]], dict):
            result[positions[identity]] = deep_merge(
                result[positions[identity]], item, path + ("[]",), conflicts
            )
    return result


def deep_merge(existing, incoming, path=(), conflicts=None):
    conflicts = conflicts if conflicts is not None else Counter()
    if is_empty(existing):
        return deepcopy(incoming)
    if is_empty(incoming) or existing == incoming:
        return deepcopy(existing)
    if isinstance(existing, dict) and isinstance(incoming, dict):
        result = deepcopy(existing)
        for key, value in incoming.items():
            if key not in result:
                result[key] = deepcopy(value)
            else:
                result[key] = deep_merge(
                    result[key], value, path + (str(key),), conflicts
                )
        return result
    if isinstance(existing, list) and isinstance(incoming, list):
        return merge_lists(existing, incoming, path, conflicts)
    if (
        prefer_maximum(path)
        and isinstance(existing, (int, float))
        and isinstance(incoming, (int, float))
    ):
        return max(existing, incoming)
    conflicts[".".join(path) or "<root>"] += 1
    return deepcopy(existing)


def ensure_primary_topic_in_topics(merged, document):
    topic = document.get("primary_topic") or {}
    if not topic.get("id"):
        return
    topics = merged.setdefault("topics", [])
    if not any(item.get("id") == topic.get("id") for item in topics if isinstance(item, dict)):
        topics.append(deepcopy(topic))


def merge_documents(survivor, losers, conflicts=None):
    conflicts = conflicts if conflicts is not None else Counter()
    merged = deepcopy(survivor)
    for loser in losers:
        merged = deep_merge(merged, loser, (), conflicts)
        ensure_primary_topic_in_topics(merged, loser)
    merged["_id"] = survivor["_id"]
    merged["author_count"] = len(merged.get("authors") or [])
    return order_document(merged, KahiBase().empty_work())


def assert_generic_lists_preserved(documents, merged):
    for field in GENERIC_PRESERVED_LISTS:
        expected = {
            canonical(item)
            for document in documents
            for item in document.get(field) or []
        }
        actual = {canonical(item) for item in merged.get(field) or []}
        if not expected <= actual:
            raise ValueError(f"items lost from {field}: {len(expected - actual)}")


def assert_entities_preserved(documents, merged, field):
    actual = {
        list_identity(item, (field,)): item for item in merged.get(field) or []
    }
    for document in documents:
        for item in document.get(field) or []:
            if list_identity(item, (field,)) not in actual:
                raise ValueError(f"entity lost from {field}")


def assert_plan_products_preserved(group, merged):
    identifiers = {
        (item.get("source"), canonical(item.get("id")))
        for item in merged.get("external_ids") or []
        if isinstance(item, dict)
    }
    for loser in group["losers"]:
        for product_id in loser["product_ids"]:
            if ("minciencias", canonical(product_id)) not in identifiers:
                raise ValueError(f"MinCiencias product ID lost: {product_id}")


def validate_merge(group, survivor, losers, merged):
    documents = [survivor, *losers]
    if merged["_id"] != survivor["_id"]:
        raise ValueError("survivor _id changed")
    assert_generic_lists_preserved(documents, merged)
    assert_entities_preserved(documents, merged, "authors")
    assert_entities_preserved(documents, merged, "groups")
    assert_entities_preserved(documents, merged, "topics")
    assert_plan_products_preserved(group, merged)
    survivor_doi = planner.normalize_doi(survivor.get("doi"))
    if planner.normalize_doi(merged.get("doi")) != survivor_doi:
        raise ValueError("survivor DOI changed")
    if merged.get("author_count") != len(merged.get("authors") or []):
        raise ValueError("author_count is inconsistent")
    size = len(BSON.encode(merged))
    if size > MAX_BSON_BYTES:
        raise ValueError(f"merged BSON exceeds safety limit: {size}")
    return size


def chunks(values, size):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def load_plan(path):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return [json.loads(line) for line in stream]


def compact_sample(group, survivor, losers, merged, size):
    return {
        "survivor_id": group["survivor_id"],
        "loser_ids": [item["work_id"] for item in group["losers"]],
        "product_ids": [
            product_id for item in group["losers"] for product_id in item["product_ids"]
        ],
        "doi": survivor.get("doi"),
        "titles_before": sum(len(item.get("titles") or []) for item in [survivor, *losers]),
        "titles_after": len(merged.get("titles") or []),
        "authors_before": sum(len(item.get("authors") or []) for item in [survivor, *losers]),
        "authors_after": len(merged.get("authors") or []),
        "external_ids_before": sum(len(item.get("external_ids") or []) for item in [survivor, *losers]),
        "external_ids_after": len(merged.get("external_ids") or []),
        "bson_bytes": size,
    }


def main():
    args = parse_args()
    if args.batch_size < 1 or args.progress_interval < 1:
        raise ValueError("batch size and progress interval must be positive")
    if not args.plan_file.is_file():
        raise FileNotFoundError(args.plan_file)

    client = MongoClient(args.mongo_uri)
    client.admin.command("ping")
    works = client[args.database].works
    plan = load_plan(args.plan_file)
    counters = Counter()
    conflicts = Counter()
    largest = []

    for batch in chunks(plan, args.batch_size):
        ids = {
            ObjectId(value)
            for group in batch
            for value in [group["survivor_id"], *[item["work_id"] for item in group["losers"]]]
        }
        documents = {
            str(item["_id"]): item for item in works.find({"_id": {"$in": list(ids)}})
        }
        replacements = []
        delete_ids = []
        for group in batch:
            counters["plan_groups"] += 1
            survivor = documents.get(group["survivor_id"])
            losers = [documents.get(item["work_id"]) for item in group["losers"]]
            if not survivor or any(item is None for item in losers):
                counters["missing_documents"] += 1
                continue
            valid = True
            for loser_plan, loser in zip(group["losers"], losers):
                evidence, rejection, _ = planner.evaluate_pair(
                    loser, survivor, loser_plan["dois"][0]
                )
                if rejection or evidence not in loser_plan["evidence"]:
                    valid = False
                    break
            if not valid:
                counters["evidence_recheck_failures"] += 1
                continue
            merged = merge_documents(survivor, losers, conflicts)
            try:
                size = validate_merge(group, survivor, losers, merged)
            except ValueError as error:
                counters[f"validation_error:{error}"] += 1
                continue
            counters["validated_groups"] += 1
            counters["validated_losers"] += len(losers)
            counters["merged_bson_bytes"] += size
            sample = compact_sample(group, survivor, losers, merged, size)
            largest.append(sample)
            largest.sort(key=lambda value: value["bson_bytes"], reverse=True)
            del largest[20:]
            if args.apply:
                replacements.append(ReplaceOne({"_id": survivor["_id"]}, merged))
                delete_ids.extend(item["_id"] for item in losers)

        if args.apply and replacements:
            result = works.bulk_write(replacements, ordered=False)
            if result.modified_count != len(replacements):
                raise RuntimeError(
                    f"replacement count mismatch: {result.modified_count} != {len(replacements)}"
                )
            deletion = works.delete_many({"_id": {"$in": delete_ids}})
            if deletion.deleted_count != len(delete_ids):
                raise RuntimeError(
                    f"deletion count mismatch: {deletion.deleted_count} != {len(delete_ids)}"
                )
            counters["applied_groups"] += len(replacements)
            counters["deleted_losers"] += len(delete_ids)

        if counters["plan_groups"] % args.progress_interval < len(batch):
            print(
                f"INFO: groups={counters['plan_groups']} validated={counters['validated_groups']} "
                f"losers={counters['validated_losers']}",
                flush=True,
            )

    report = {
        "mode": "apply" if args.apply else "dry-run",
        "database": args.database,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "plan_file": str(args.plan_file),
        "counters": dict(sorted(counters.items())),
        "scalar_conflicts_kept_from_survivor": dict(conflicts.most_common(100)),
        "maximum_bson_bytes": largest[0]["bson_bytes"] if largest else 0,
        "passed": (
            counters["validated_groups"] == len(plan)
            and not any(key.startswith("validation_error:") for key in counters)
            and counters["missing_documents"] == 0
            and counters["evidence_recheck_failures"] == 0
        ),
    }
    args.report_file.parent.mkdir(parents=True, exist_ok=True)
    args.report_file.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    args.sample_file.write_text(json.dumps(largest, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
