#!/usr/bin/env python3
"""Audit or backfill CIARP authors and temporally valid affiliations."""

import argparse
import gzip
import json
import re
import unicodedata
from collections import defaultdict
from copy import deepcopy
from pathlib import Path

from bson import BSON
from pymongo import MongoClient, UpdateOne

from kahi_ciarp_works.process_one import (
    publication_year_in_parent_affiliation,
)
from kahi_impactu_utils.Mapping import ciarp_mapping


DOCUMENT_TYPES = {
    "cédula de ciudadanía": "Cédula de Ciudadanía",
    "cédula de extranjería": "Cédula de Extranjería",
    "pasaporte": "Pasaporte",
    "COD_RH": "scienti",
}


def identity_key(document_type, identifier):
    source = DOCUMENT_TYPES.get(document_type)
    value = str(identifier or "").strip()
    if not source or not value:
        return None
    if document_type == "COD_RH":
        value = value.zfill(10)
    return source, value


def normalized_name(value):
    """Normalize a name only for strict, accent-insensitive equality."""
    text = unicodedata.normalize("NFKD", str(value or "")).casefold()
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^\w]+", " ", text).split())


def staff_full_name(record):
    return " ".join(
        str(record.get(field) or "").strip()
        for field in ("nombres", "primer_apellido", "segundo_apellido")
        if str(record.get(field) or "").strip()
    )


def external_id_from_identity(key):
    source_name, value = key
    identifier = {"COD_RH": value} if source_name == "scienti" else value
    return {"provenance": "staff", "source": source_name, "id": identifier}


def preferred_name(affiliation):
    names = affiliation.get("names", [])
    for language in ("es", "en"):
        for name in names:
            if name.get("lang") == language and name.get("name"):
                return name["name"]
    return names[0].get("name", "") if names else ""


def compact_affiliation(affiliation):
    return {
        "id": affiliation["_id"],
        "name": preferred_name(affiliation),
        "types": affiliation.get("types", []),
    }


def chunks(values, size=2000):
    values = list(values)
    for offset in range(0, len(values), size):
        yield values[offset:offset + size]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="localhost:27017")
    parser.add_argument("--database", default="kahi_dev_092026")
    parser.add_argument("--source-database", default="institutional_data")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--identities-only", action="store_true",
        help="Apply only identity links proven by staff name and work author",
    )
    parser.add_argument(
        "--backup-path",
        help="Write changed works as concatenated BSON in a gzip file",
    )
    args = parser.parse_args()

    client = MongoClient(args.url)
    target = client[args.database]
    source = client[args.source_database]
    institution_ids = sorted(source.ciarp.distinct("institution_id"))
    records = []
    stats = {}
    for institution_id in institution_ids:
        try:
            categories = ciarp_mapping(
                "https://ror.org/" + institution_id, "works")
        except ValueError:
            categories = []
        institution_records = list(source.ciarp.find({
            "institution_id": institution_id,
            "ranking": {"$in": categories},
        }, {
            "identificación": 1, "tipo_documento": 1, "año": 1,
            "institution_id": 1, "código_unidad_académica": 1,
            "código_subunidad_académica": 1,
        }))
        records.extend(institution_records)
        stats[institution_id] = defaultdict(int)
        stats[institution_id]["source_records"] = len(institution_records)

    record_ids = {str(record["_id"]) for record in records}
    works_by_record = {}
    works = {}
    for batch in chunks(record_ids):
        cursor = target.works.find({
            "external_ids": {"$elemMatch": {
                "source": "ciarp_record", "id": {"$in": batch}}},
        }, {"authors": 1, "external_ids": 1})
        for work in cursor:
            works[work["_id"]] = work
            for external_id in work.get("external_ids", []):
                if (external_id.get("source") == "ciarp_record"
                        and str(external_id.get("id")) in record_ids):
                    works_by_record[str(external_id["id"])] = work["_id"]

    scalar_ids = defaultdict(set)
    cod_rh_ids = set()
    for record in records:
        key = identity_key(
            record.get("tipo_documento"), record.get("identificación"))
        if not key:
            continue
        if key[0] == "scienti":
            cod_rh_ids.add(key[1])
        else:
            scalar_ids[key[0]].add(key[1])
    people = {}
    queries = []
    for source_name, identifiers in scalar_ids.items():
        for batch in chunks(identifiers):
            queries.append({"external_ids": {"$elemMatch": {
                "source": source_name, "id": {"$in": batch}}}})
    for batch in chunks(cod_rh_ids):
        queries.append({"external_ids": {"$elemMatch": {
            "source": "scienti", "id.COD_RH": {"$in": batch}}}})
    for query in queries:
        for person in target.person.find(query, {
                "full_name": 1, "affiliations": 1, "external_ids": 1}):
            people[person["_id"]] = person
    people_by_identity = defaultdict(dict)
    for person in people.values():
        for external_id in person.get("external_ids", []):
            value = external_id.get("id")
            if isinstance(value, dict):
                value = value.get("COD_RH")
            key = (external_id.get("source"), str(value or ""))
            people_by_identity[key][person["_id"]] = person

    # A staff identity may have been lost after person unification. Recover it
    # only when the staff name matches an author on the linked CIARP works and
    # every matching occurrence points to one canonical person.
    staff_names = defaultdict(set)
    for item in source.staff.find({}, {
            "institution_id": 1, "tipo_documento": 1, "identificación": 1,
            "nombres": 1, "primer_apellido": 1, "segundo_apellido": 1}):
        key = identity_key(
            item.get("tipo_documento"), item.get("identificación"))
        name = normalized_name(staff_full_name(item))
        if key and name:
            staff_names[(str(item.get("institution_id") or ""), key)].add(name)

    records_by_identity = defaultdict(list)
    for record in records:
        key = identity_key(
            record.get("tipo_documento"), record.get("identificación"))
        if key:
            records_by_identity[(record["institution_id"], key)].append(record)

    identity_repairs = defaultdict(list)
    for (institution_id, key), identity_records in records_by_identity.items():
        if people_by_identity.get(key) or len(
                staff_names.get((institution_id, key), set())) != 1:
            continue
        expected_name = next(iter(staff_names[(institution_id, key)]))
        candidate_ids = set()
        for record in identity_records:
            work_id = works_by_record.get(str(record["_id"]))
            if work_id is None:
                continue
            for author in works[work_id].get("authors", []):
                if (author.get("id")
                        and normalized_name(author.get("full_name"))
                        == expected_name):
                    candidate_ids.add(author["id"])
        if len(candidate_ids) != 1:
            continue
        person = target.person.find_one(
            {"_id": next(iter(candidate_ids))},
            {"full_name": 1, "affiliations": 1, "external_ids": 1},
        )
        if not person or normalized_name(person.get("full_name")) != expected_name:
            continue
        people_by_identity[key][person["_id"]] = person
        identity_repairs[person["_id"]].append(external_id_from_identity(key))
        stats[institution_id]["resolved_by_staff_work_name"] += 1

    person_operations = [
        UpdateOne(
            {"_id": person_id},
            {"$addToSet": {"external_ids": {"$each": external_ids}}},
        )
        for person_id, external_ids in identity_repairs.items()
    ]
    if args.identities_only:
        print(json.dumps({
            "mode": "apply" if args.apply else "dry-run",
            "identities_to_repair": len(identity_repairs),
            "people": [str(person_id) for person_id in identity_repairs],
        }, ensure_ascii=False, sort_keys=True))
        if args.apply and person_operations:
            result = target.person.bulk_write(person_operations, ordered=False)
            print(json.dumps({
                "person_identities_matched": result.matched_count,
                "person_identities_modified": result.modified_count,
            }))
        return

    affiliation_ids = set(institution_ids)
    for record in records:
        institution_id = record["institution_id"]
        faculty = str(record.get("código_unidad_académica") or "").strip()
        department = str(
            record.get("código_subunidad_académica") or "").strip()
        if faculty:
            affiliation_ids.add(f"{institution_id}_{faculty}")
        if faculty and department:
            affiliation_ids.add(f"{institution_id}_{faculty}_{department}")
    affiliations = {
        str(item["_id"]): compact_affiliation(item)
        for item in target.affiliations.find(
            {"_id": {"$in": list(affiliation_ids)}},
            {"names": 1, "types": 1})
    }

    planned = {work_id: deepcopy(work) for work_id, work in works.items()}
    changed_works = set()
    touched_works = defaultdict(set)
    for record in records:
        institution_id = record["institution_id"]
        institution_stats = stats[institution_id]
        work_id = works_by_record.get(str(record["_id"]))
        if work_id is None:
            institution_stats["work_not_found"] += 1
            continue
        touched_works[institution_id].add(work_id)
        key = identity_key(
            record.get("tipo_documento"), record.get("identificación"))
        if not key:
            institution_stats["invalid_identity"] += 1
            continue
        matches = people_by_identity.get(key, {})
        if not matches:
            institution_stats["person_not_found"] += 1
            continue
        if len(matches) != 1:
            institution_stats["ambiguous_person"] += 1
            continue
        person = next(iter(matches.values()))
        institution_stats["person_resolved"] += 1
        parents = [
            item for item in person.get("affiliations", [])
            if str(item.get("id")) == institution_id
        ]
        if not any(item.get("start_date") not in (None, "", -1)
                   for item in parents):
            institution_stats["missing_start_date"] += 1
            eligible = False
        else:
            eligible = publication_year_in_parent_affiliation(
                person, institution_id, record.get("año"))
            if not eligible:
                institution_stats["outside_interval"] += 1
        expected_ids = []
        if eligible:
            expected_ids.append(institution_id)
            faculty = str(
                record.get("código_unidad_académica") or "").strip()
            department = str(
                record.get("código_subunidad_académica") or "").strip()
            if faculty:
                expected_ids.append(f"{institution_id}_{faculty}")
            if faculty and department:
                expected_ids.append(
                    f"{institution_id}_{faculty}_{department}")
            institution_stats["temporally_eligible"] += 1

        work = planned[work_id]
        author = next((item for item in work.get("authors", [])
                       if item.get("id") == person["_id"]), None)
        if author is None:
            author = {
                "id": person["_id"],
                "full_name": person.get("full_name", ""),
                "affiliations": [],
            }
            work.setdefault("authors", []).append(author)
            changed_works.add(work_id)
            institution_stats["authors_to_add"] += 1
        current_ids = {
            str(item.get("id")) for item in author.get("affiliations", [])
        }
        for expected_id in expected_ids:
            affiliation = affiliations.get(expected_id)
            if affiliation and expected_id not in current_ids:
                author.setdefault("affiliations", []).append(affiliation)
                current_ids.add(expected_id)
                changed_works.add(work_id)
                kind = next((item.get("type")
                             for item in affiliation.get("types", [])
                             if item.get("type") in {
                                 "education", "faculty", "department"}),
                            "affiliation")
                institution_stats[f"{kind}_edges_to_add"] += 1

    operations = [
        UpdateOne(
            {"_id": work_id},
            {"$set": {
                "authors": planned[work_id].get("authors", []),
                "author_count": len(planned[work_id].get("authors", [])),
            }},
        )
        for work_id in changed_works
    ]
    report = {}
    for institution_id, institution_stats in stats.items():
        institution_stats["unique_works_found"] = len(
            touched_works[institution_id])
        institution_stats["works_to_change"] = len({
            work_id for work_id in touched_works[institution_id]
            if work_id in changed_works
        })
        report[institution_id] = dict(institution_stats)
    print(json.dumps({
        "mode": "apply" if args.apply else "dry-run",
        "works_to_change": len(changed_works),
        "identities_to_repair": len(identity_repairs),
        "institutions": report,
    }, ensure_ascii=False, sort_keys=True))
    if args.apply and args.backup_path:
        backup_path = Path(args.backup_path)
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(backup_path, "wb") as backup_file:
            for work in target.works.find({
                    "_id": {"$in": list(changed_works)}}):
                backup_file.write(BSON.encode(work))
        print(json.dumps({
            "backup_path": str(backup_path),
            "backup_documents": len(changed_works),
        }))
    if args.apply and person_operations:
        result = target.person.bulk_write(person_operations, ordered=False)
        print(json.dumps({
            "person_identities_matched": result.matched_count,
            "person_identities_modified": result.modified_count,
        }))
    if args.apply and operations and not args.identities_only:
        result = target.works.bulk_write(operations, ordered=False)
        print(json.dumps({
            "matched": result.matched_count,
            "modified": result.modified_count,
        }))


if __name__ == "__main__":
    main()
