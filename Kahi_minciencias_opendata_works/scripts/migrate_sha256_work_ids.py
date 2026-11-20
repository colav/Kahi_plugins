#!/usr/bin/env python3
"""Migrate legacy SHA-256 work ids to deterministic BSON ObjectIds."""

import argparse
from hashlib import sha256
import re
from copy import deepcopy

from bson import ObjectId
from pymongo import MongoClient


SHA256_RE = re.compile(r"[0-9a-fA-F]{64}")


def target_id(value):
    if SHA256_RE.fullmatch(value):
        return ObjectId(bytes.fromhex(value)[:12])
    return ObjectId(sha256(value.encode("utf-8")).digest()[:12])


def same_document(left, right):
    left = deepcopy(left)
    right = deepcopy(right)
    left.pop("_id", None)
    right.pop("_id", None)
    return left == right


def migrate(collection, batch_size):
    query = {"_id": {"$type": "string"}}
    total = collection.count_documents(query)
    migrated = 0
    resumed = 0
    cursor = collection.find(query, no_cursor_timeout=True).batch_size(batch_size)
    try:
        batch = []
        for document in cursor:
            batch.append(document)
            if len(batch) >= batch_size:
                done, recovered = migrate_batch(collection, batch)
                migrated += done
                resumed += recovered
                print(f"INFO: migrated {migrated + resumed}/{total} works", flush=True)
                batch.clear()
        if batch:
            done, recovered = migrate_batch(collection, batch)
            migrated += done
            resumed += recovered
            print(f"INFO: migrated {migrated + resumed}/{total} works", flush=True)
    finally:
        cursor.close()
    return {"total": total, "migrated": migrated, "resumed": resumed}


def migrate_batch(collection, documents):
    replacements = []
    legacy_ids = []
    resumed = 0
    mapped = {}
    for document in documents:
        legacy_id = document["_id"]
        new_id = target_id(legacy_id)
        if new_id in mapped and mapped[new_id] != legacy_id:
            raise RuntimeError(f"ObjectId collision: {mapped[new_id]} and {legacy_id}")
        mapped[new_id] = legacy_id
    existing = {
        item["_id"]: item
        for item in collection.find({"_id": {"$in": list(mapped)}})
    }
    for document in documents:
        legacy_id = document["_id"]
        new_id = target_id(legacy_id)
        current = existing.get(new_id)
        replacement = deepcopy(document)
        replacement["_id"] = new_id
        if current:
            if not same_document(current, replacement):
                raise RuntimeError(f"occupied target ObjectId: {new_id}")
            resumed += 1
        else:
            replacements.append(replacement)
        legacy_ids.append(legacy_id)
    if replacements:
        collection.insert_many(replacements, ordered=False)
    result = collection.delete_many({"_id": {"$in": legacy_ids}})
    if result.deleted_count != len(legacy_ids):
        raise RuntimeError("not all legacy documents were removed")
    return len(replacements), resumed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default="mongodb://localhost:27017")
    parser.add_argument("--database-name", required=True)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    client = MongoClient(args.database_url)
    collection = client[args.database_name]["works"]
    summary = migrate(collection, max(1, args.batch_size))
    remaining = collection.count_documents({"_id": {"$type": "string"}})
    print(f"INFO: completed {summary}; remaining_string_ids={remaining}", flush=True)
    if remaining:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
