#!/usr/bin/env python3
"""Backfill parent institutions omitted when staff people already existed."""

import argparse
import json
from collections import defaultdict

from pymongo import MongoClient, UpdateOne

from kahi_staff_person.Kahi_staff_person import (
    append_staff_external_ids,
    staff_affiliation_interval,
)


def identifier_key(source, value):
    return source, json.dumps(value, sort_keys=True, ensure_ascii=False)


def preferred_name(affiliation):
    names = affiliation.get("names", [])
    for language in ("es", "en"):
        for name in names:
            if name.get("lang") == language and name.get("name"):
                return name["name"]
    return names[0].get("name", "") if names else ""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="localhost:27017")
    parser.add_argument("--database", default="kahi_dev_092026")
    parser.add_argument("--source-database", default="institutional_data")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    client = MongoClient(args.url)
    target = client[args.database]
    source = client[args.source_database]
    grouped = defaultdict(list)
    for record in source.staff.find({}):
        institution_id = str(record.get("institution_id") or "")
        person_id = str(record.get("identificación") or "").strip()
        if institution_id and person_id:
            grouped[(institution_id, person_id)].append(record)

    external_ids_by_group = {}
    scalar_ids = set()
    cod_rh_ids = set()
    for key, records in grouped.items():
        external_ids = []
        for record in records:
            append_staff_external_ids(external_ids, record, key[1])
        external_ids_by_group[key] = external_ids
        for external_id in external_ids:
            value = external_id.get("id")
            if isinstance(value, dict) and value.get("COD_RH"):
                cod_rh_ids.add(value["COD_RH"])
            elif value:
                scalar_ids.add(value)
    people = list(target.person.find({"$or": [
        {"external_ids.id": {"$in": list(scalar_ids)}},
        {"external_ids.id.COD_RH": {"$in": list(cod_rh_ids)}},
    ]}, {"external_ids": 1, "affiliations.id": 1}))
    people_by_external_id = defaultdict(dict)
    for person in people:
        for external_id in person.get("external_ids", []):
            key = identifier_key(
                external_id.get("source"), external_id.get("id"))
            people_by_external_id[key][person["_id"]] = person

    affiliations = {
        str(item["_id"]): item
        for item in target.affiliations.find({}, {
            "names": 1, "types": 1,
        })
    }
    stats = defaultdict(lambda: {
        "staff_people": 0, "resolved": 0, "missing_parent": 0,
        "ambiguous": 0, "unresolved": 0,
    })
    operations = []
    for (institution_id, person_id), records in grouped.items():
        institution_stats = stats[institution_id]
        institution_stats["staff_people"] += 1
        external_ids = external_ids_by_group[(institution_id, person_id)]
        matches = {}
        for external_id in external_ids:
            key = identifier_key(
                external_id.get("source"), external_id.get("id"))
            matches.update(people_by_external_id.get(key, {}))
        if not matches:
            institution_stats["unresolved"] += 1
            continue
        if len(matches) != 1:
            institution_stats["ambiguous"] += 1
            continue
        institution_stats["resolved"] += 1
        person = next(iter(matches.values()))
        if any(str(item.get("id")) == institution_id
               for item in person.get("affiliations", [])):
            continue
        institution_stats["missing_parent"] += 1
        institution = affiliations.get(institution_id)
        if not institution:
            continue
        start_date, end_date, years = staff_affiliation_interval(records)
        parent = {
            "id": institution["_id"],
            "name": preferred_name(institution),
            "types": institution.get("types", []),
            "start_date": start_date,
            "end_date": end_date,
            "years": years,
            "relations": [],
        }
        operations.append(UpdateOne(
            {"_id": person["_id"], "affiliations.id": {"$ne": institution["_id"]}},
            {"$push": {"affiliations": parent}},
        ))

    print({
        "mode": "apply" if args.apply else "dry-run",
        "institutions": dict(sorted(stats.items())),
        "repairs": len(operations),
    })
    if args.apply and operations:
        result = target.person.bulk_write(operations, ordered=False)
        print({"matched": result.matched_count, "modified": result.modified_count})


if __name__ == "__main__":
    main()
