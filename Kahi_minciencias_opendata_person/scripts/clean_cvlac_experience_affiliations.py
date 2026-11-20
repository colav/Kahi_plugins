#!/usr/bin/env python3
"""Remove scraped CvLAC experiences and merge remaining affiliation history."""

import argparse
import gzip
import json
import unicodedata
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from bson import BSON
from pymongo import MongoClient, UpdateOne
from yuku.scienti_persons import experience_affiliation


def name_key(value):
    text = unicodedata.normalize("NFC", str(value or "")).casefold()
    return " ".join(text.split())


def affiliation_key(item):
    identifier = str(item.get("id") or "")
    identity = identifier or name_key(item.get("name"))
    return (
        identity,
        item.get("start_date"),
        item.get("end_date"),
        str(item.get("position") or ""),
    )


def years_from_item(item, current_year):
    years = {
        int(value) for value in item.get("years", [])
        if str(value).isdigit() and 1900 <= int(value) <= current_year
    }
    start = item.get("start_date")
    end = item.get("end_date")
    if isinstance(start, (int, float)) and start > 0:
        start_year = datetime.fromtimestamp(start, timezone.utc).year
        if end == -1:
            end_year = current_year
        elif isinstance(end, (int, float)) and end > 0:
            end_year = datetime.fromtimestamp(end, timezone.utc).year
        else:
            end_year = start_year
        if 1900 <= start_year <= end_year <= current_year:
            years.update(range(start_year, end_year + 1))
    return years


def merge_affiliations(values, current_year):
    grouped = defaultdict(list)
    order = []
    for item in values:
        key = str(item.get("id") or "") or "name:" + name_key(item.get("name"))
        if key not in grouped:
            order.append(key)
        grouped[key].append(item)
    output = []
    for key in order:
        items = grouped[key]
        base = deepcopy(max(items, key=lambda item: len(json.dumps(
            item, ensure_ascii=False, default=str))))
        if len(items) > 1:
            years = set()
            starts = []
            ends = []
            active = False
            for item in items:
                years.update(years_from_item(item, current_year))
                start = item.get("start_date")
                end = item.get("end_date")
                if isinstance(start, (int, float)) and start > 0:
                    starts.append(start)
                if end == -1:
                    active = True
                elif isinstance(end, (int, float)) and end > 0:
                    ends.append(end)
            if years:
                base["years"] = sorted(years)
            if starts:
                base["start_date"] = min(starts)
            if active:
                base["end_date"] = -1
            elif ends:
                base["end_date"] = max(ends)
        output.append(base)
    return output


def chunks(values, size=1000):
    values = list(values)
    for offset in range(0, len(values), size):
        yield values[offset:offset + size]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="localhost:27017")
    parser.add_argument("--database", default="kahi_dev_092026")
    parser.add_argument("--source-database", default="dam")
    parser.add_argument(
        "--cvlac-collection", default="cvlac_related_works_kahi_v12_20260908")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup-path")
    parser.add_argument("--progress-every", type=int, default=10000)
    parser.add_argument("--person-id", action="append", default=[])
    args = parser.parse_args()

    client = MongoClient(args.url)
    target = client[args.database]
    cvlac = client[args.source_database][args.cvlac_collection]
    current_year = datetime.now(timezone.utc).year

    names = defaultdict(set)
    preferred = {}
    for affiliation in target.affiliations.find({}, {"names": 1}):
        identifier = str(affiliation["_id"])
        for item in affiliation.get("names", []):
            if item.get("name"):
                names[name_key(item["name"])].add(identifier)
                if identifier not in preferred or item.get("lang") == "es":
                    preferred[identifier] = item["name"]

    stats = defaultdict(int)
    backup = None
    if args.apply and args.backup_path:
        path = Path(args.backup_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        backup = gzip.open(path, "wb")
    operations = []
    source_query = {"profile.experiences.0": {"$exists": True}}
    if args.person_id:
        source_query["id_persona_pr"] = {"$in": args.person_id}
    cursor = cvlac.find(
        source_query,
        {"id_persona_pr": 1, "profile.experiences": 1},
        no_cursor_timeout=True,
    )
    try:
        for source in cursor:
            stats["profiles_with_experiences"] += 1
            person_id = str(source.get("id_persona_pr") or source.get("_id") or "")
            people = list(target.person.find({
                "external_ids": {"$elemMatch": {
                    "source": "scienti", "id.COD_RH": person_id,
                }}
            }).limit(2))
            if not people:
                stats["person_not_found"] += 1
                continue
            if len(people) != 1:
                stats["ambiguous_person"] += 1
                continue
            person = people[0]
            fingerprints = set()
            for raw in (source.get("profile") or {}).get("experiences", []):
                item = experience_affiliation(raw)
                if not item:
                    continue
                # Depending on ingestion order, the same scraped experience
                # may still have an empty id or may have been resolved to ROR.
                fingerprints.add(affiliation_key(item))
                matches = names.get(name_key(item.get("name")), set())
                if len(matches) == 1:
                    item["id"] = next(iter(matches))
                fingerprints.add(affiliation_key(item))
            before = person.get("affiliations", [])
            retained = []
            removed = 0
            for item in before:
                if (not item.get("years")
                        and affiliation_key(item) in fingerprints):
                    removed += 1
                else:
                    retained.append(item)
            merged = merge_affiliations(retained, current_year)
            if removed:
                stats["experience_entries_removed"] += removed
                stats["people_with_experience_removed"] += 1
            stats["entries_merged"] += len(retained) - len(merged)
            if merged == before:
                continue
            stats["people_changed"] += 1
            stats["affiliations_before"] += len(before)
            stats["affiliations_after"] += len(merged)
            if backup:
                backup.write(BSON.encode(person))
            operations.append(UpdateOne(
                {"_id": person["_id"]}, {"$set": {"affiliations": merged}}))
            if len(operations) >= 1000:
                if args.apply:
                    result = target.person.bulk_write(operations, ordered=False)
                    stats["modified"] += result.modified_count
                operations = []
            if stats["profiles_with_experiences"] % args.progress_every == 0:
                print(json.dumps(dict(stats), ensure_ascii=False), flush=True)
        if args.apply and operations:
            result = target.person.bulk_write(operations, ordered=False)
            stats["modified"] += result.modified_count
    finally:
        cursor.close()
        if backup:
            backup.close()
    print(json.dumps({
        "mode": "apply" if args.apply else "dry-run",
        **dict(stats),
        "backup_path": args.backup_path if args.apply else None,
    }, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
