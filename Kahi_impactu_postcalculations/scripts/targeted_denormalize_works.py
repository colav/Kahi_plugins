#!/usr/bin/env python3
"""Refresh embedded author data for a BSON-selected set of works."""

import argparse
import gzip
import json
from pathlib import Path

from bson import BSON, decode_file_iter
from pymongo import MongoClient, UpdateOne


AUTHOR_FIELDS = (
    "sex", "full_name", "first_names", "last_names", "ranking",
    "external_ids",
)


def chunks(values, size=1000):
    for offset in range(0, len(values), size):
        yield values[offset:offset + size]


def selected_ids(path):
    with gzip.open(path, "rb") as source:
        return [document["_id"] for document in decode_file_iter(source)]


def enrich_authors(authors, people, affiliations):
    enriched = []
    for original in authors or []:
        author = dict(original)
        person = people.get(author.get("id"))
        if person:
            for field in AUTHOR_FIELDS:
                if field in person:
                    author[field] = person[field]
        person_affiliations = {
            item.get("id"): item
            for item in (person or {}).get("affiliations", [])
        }
        embedded = []
        for original_affiliation in author.get("affiliations", []) or []:
            affiliation = dict(original_affiliation)
            person_affiliation = person_affiliations.get(
                affiliation.get("id"), {})
            for field in ("start_date", "end_date"):
                if person_affiliation.get(field) is not None:
                    affiliation[field] = person_affiliation[field]
            canonical = affiliations.get(affiliation.get("id"))
            if canonical:
                if "external_ids" in canonical:
                    affiliation["external_ids"] = canonical["external_ids"]
                affiliation["addresses"] = [
                    {
                        "latitude": address.get("lat", ""),
                        "longitude": address.get("lng", ""),
                        "city": address.get("city", ""),
                        "country": address.get("country", ""),
                        "country_code": address.get("country_code", ""),
                    }
                    for address in canonical.get("addresses", [])
                ]
            embedded.append(affiliation)
        author["affiliations"] = embedded
        enriched.append(author)
    return enriched


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="localhost:27017")
    parser.add_argument("--database", default="kahi_dev_092026")
    parser.add_argument("--ids-from-bson-gz", required=True)
    parser.add_argument("--backup-path")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    client = MongoClient(args.url)
    db = client[args.database]
    work_ids = selected_ids(args.ids_from_bson_gz)
    person_ids = set()
    affiliation_ids = set()
    if args.apply and args.backup_path:
        Path(args.backup_path).parent.mkdir(parents=True, exist_ok=True)
    backup = gzip.open(args.backup_path, "wb") if (
        args.apply and args.backup_path) else None
    try:
        for batch in chunks(work_ids):
            for work in db.works.find({"_id": {"$in": batch}}):
                if backup:
                    backup.write(BSON.encode(work))
                for author in work.get("authors", []) or []:
                    if author.get("id"):
                        person_ids.add(author["id"])
                    for affiliation in author.get("affiliations", []) or []:
                        if affiliation.get("id"):
                            affiliation_ids.add(affiliation["id"])
    finally:
        if backup:
            backup.close()
    print(
        f"INFO: selected {len(work_ids)} works; "
        f"people={len(person_ids)} affiliations={len(affiliation_ids)}",
        flush=True,
    )

    people = {}
    for batch in chunks(list(person_ids)):
        for person in db.person.find({"_id": {"$in": batch}}, {
                "sex": 1, "full_name": 1, "first_names": 1,
                "last_names": 1, "ranking": 1, "external_ids": 1,
                "affiliations.id": 1, "affiliations.start_date": 1,
                "affiliations.end_date": 1}):
            people[person["_id"]] = person
    affiliations = {}
    for batch in chunks(list(affiliation_ids)):
        for affiliation in db.affiliations.find(
                {"_id": {"$in": batch}}, {"external_ids": 1, "addresses": 1}):
            affiliations[affiliation["_id"]] = affiliation

    scanned = changed = modified = 0
    for batch in chunks(work_ids):
        operations = []
        for work in db.works.find(
                {"_id": {"$in": batch}}, {"authors": 1}):
            scanned += 1
            authors = enrich_authors(
                work.get("authors", []), people, affiliations)
            if authors != work.get("authors", []):
                changed += 1
                operations.append(UpdateOne(
                    {"_id": work["_id"]}, {"$set": {"authors": authors}}))
        if args.apply and operations:
            modified += db.works.bulk_write(
                operations, ordered=False).modified_count
        if scanned % 5000 == 0 or scanned == len(work_ids):
            print(
                f"INFO: scanned={scanned}/{len(work_ids)} "
                f"changed={changed} modified={modified}",
                flush=True,
            )
    print(json.dumps({
        "mode": "apply" if args.apply else "dry-run",
        "scanned": scanned,
        "changed": changed,
        "modified": modified,
        "people_loaded": len(people),
        "affiliations_loaded": len(affiliations),
        "backup_path": args.backup_path if args.apply else None,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
