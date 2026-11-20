#!/usr/bin/env python3
"""Repair authors captured by an unsafe migration from an empty person id."""

import argparse
from collections import defaultdict

from pymongo import MongoClient, UpdateOne


def preferred_name(affiliation):
    names = affiliation.get("names", [])
    for lang in ("es", "en"):
        for name in names:
            if name.get("lang") == lang and name.get("name"):
                return name["name"]
    return names[0].get("name", "") if names else ""


def recalculate_entity(target, collection, entity_id, field):
    """Refresh counts affected by repaired work-author references."""
    path = "authors.id" if field == "person" else "authors.affiliations.id"
    match = {path: entity_id}
    products_count = target.works.count_documents(match)
    citations = list(target.works.aggregate([
        {"$match": match},
        {"$unwind": "$citations_count"},
        {"$match": {"citations_count.source": {"$nin": [None, ""]}}},
        {"$group": {
            "_id": "$citations_count.source",
            "count": {"$sum": {"$convert": {
                "input": "$citations_count.count", "to": "long",
                "onError": 0, "onNull": 0,
            }}},
        }},
    ]))
    collection.update_one(
        {"_id": entity_id},
        {"$set": {
            "products_count": products_count,
            "citations_count": [
                {"source": item["_id"], "count": item["count"]}
                for item in citations
            ],
        }},
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="localhost:27017")
    parser.add_argument("--database", default="kahi_dev_092026")
    parser.add_argument("--source-database", default="institutional_data")
    parser.add_argument("--bad-person-id", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    client = MongoClient(args.url)
    target = client[args.database]
    source = client[args.source_database]
    works = list(target.works.find(
        {"authors.id": args.bad_person_id},
        {"authors": 1, "external_ids": 1},
    ))
    record_ids = {
        str(ext["id"])
        for work in works
        for ext in work.get("external_ids", [])
        if ext.get("source") == "ciarp_record" and ext.get("id")
    }
    records = {
        str(record["_id"]): record
        for record in source.ciarp.find(
            {"_id": {"$in": list(record_ids)}},
            {"identificación": 1, "tipo_documento": 1,
             "institution_id": 1},
        )
    }

    person_cache = {}
    affiliation_cache = {}
    matched_identifications = set()
    unresolved_identifications = set()
    ambiguous_identifications = set()
    operations = []
    authorless = 0
    unresolved_author_entries = 0
    repaired_person_ids = {args.bad_person_id}
    repaired_affiliation_ids = set()

    def resolve_person(identifier):
        if identifier not in person_cache:
            matches = list(target.person.find({"$or": [
                {"external_ids.id": identifier},
                {"external_ids.id.COD_RH": identifier},
            ]}, {
                "full_name": 1, "first_names": 1, "last_names": 1,
                "ranking": 1, "external_ids": 1, "sex": 1,
            }).limit(2))
            person_cache[identifier] = matches
        matches = person_cache[identifier]
        if len(matches) == 1:
            matched_identifications.add(identifier)
            return matches[0]
        if len(matches) > 1:
            ambiguous_identifications.add(identifier)
        else:
            unresolved_identifications.add(identifier)
        return None

    def compact_affiliation(institution_id):
        if institution_id not in affiliation_cache:
            affiliation = target.affiliations.find_one(
                {"_id": institution_id},
                {"names": 1, "types": 1},
            )
            affiliation_cache[institution_id] = ({
                "id": affiliation["_id"],
                "name": preferred_name(affiliation),
                "types": affiliation.get("types", []),
            } if affiliation else None)
        return affiliation_cache[institution_id]

    for work in works:
        resolved = defaultdict(set)
        unresolved = defaultdict(set)
        for ext in work.get("external_ids", []):
            if ext.get("source") != "ciarp_record":
                continue
            record = records.get(str(ext.get("id")))
            if not record:
                continue
            identifier = str(record.get("identificación") or "").strip()
            if not identifier:
                continue
            person = resolve_person(identifier)
            if person and record.get("institution_id"):
                resolved[str(person["_id"])].add(record["institution_id"])
            elif not person:
                key = (str(record.get("tipo_documento") or "ciarp"),
                       identifier)
                if record.get("institution_id"):
                    unresolved[key].add(record["institution_id"])

        authors = [
            author for author in work.get("authors", [])
            if author.get("id") != args.bad_person_id
        ]
        existing = {str(author.get("id")) for author in authors if author.get("id")}
        for person_id, institution_ids in resolved.items():
            if person_id in existing:
                continue
            person = next(
                match for matches in person_cache.values() for match in matches
                if str(match["_id"]) == person_id
            )
            affiliations = [
                compact_affiliation(institution_id)
                for institution_id in sorted(institution_ids)
            ]
            author = {
                "id": person["_id"],
                "full_name": person.get("full_name", ""),
                "affiliations": [item for item in affiliations if item],
            }
            for field in ("first_names", "last_names", "ranking",
                          "external_ids", "sex"):
                if field in person:
                    author[field] = person[field]
            authors.append(author)
            existing.add(person_id)
            repaired_person_ids.add(person["_id"])
            repaired_affiliation_ids.update(institution_ids)
        for (document_type, identifier), institution_ids in unresolved.items():
            affiliations = [
                compact_affiliation(institution_id)
                for institution_id in sorted(institution_ids)
            ]
            authors.append({
                "id": "",
                "full_name": "",
                "affiliations": [item for item in affiliations if item],
                "external_ids": [{
                    "provenance": "ciarp",
                    "source": document_type,
                    "id": identifier,
                }],
            })
            unresolved_author_entries += 1
            repaired_affiliation_ids.update(institution_ids)
        if not authors:
            authorless += 1
        operations.append(UpdateOne(
            {"_id": work["_id"], "authors.id": args.bad_person_id},
            {"$set": {"authors": authors, "author_count": len(authors)}},
        ))

    print({
        "mode": "apply" if args.apply else "dry-run",
        "affected_works": len(works),
        "source_records_found": len(records),
        "matched_identifications": len(matched_identifications),
        "ambiguous_identifications": len(ambiguous_identifications),
        "unresolved_identifications": len(unresolved_identifications),
        "unresolved_author_entries": unresolved_author_entries,
        "works_left_without_authors": authorless,
    })
    if args.apply and operations:
        result = target.works.bulk_write(operations, ordered=False)
        print({"matched": result.matched_count, "modified": result.modified_count})
        for person_id in repaired_person_ids:
            recalculate_entity(
                target, target.person, person_id, "person")
        for affiliation_id in repaired_affiliation_ids:
            recalculate_entity(
                target, target.affiliations, affiliation_id, "affiliation")
        print({
            "recalculated_people": len(repaired_person_ids),
            "recalculated_affiliations": len(repaired_affiliation_ids),
        })


if __name__ == "__main__":
    main()
