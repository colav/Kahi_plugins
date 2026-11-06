from copy import deepcopy
import re
import unicodedata

from pymongo import UpdateOne


def normalized_author_name(value):
    """Normalize a name strictly enough for comparisons inside one product."""
    if not isinstance(value, str):
        return ""
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode().lower()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value).split())


def _merge_author_metadata(target, source):
    """Fill and combine metadata while retaining the identified author."""
    for key, value in source.items():
        if key == "id":
            continue
        current = target.get(key)
        if isinstance(value, list):
            if not isinstance(current, list):
                target[key] = deepcopy(value)
            else:
                for item in value:
                    if item not in current:
                        current.append(deepcopy(item))
        elif isinstance(value, dict):
            if not isinstance(current, dict):
                target[key] = deepcopy(value)
            else:
                for subkey, subvalue in value.items():
                    if current.get(subkey) in (None, "", [], {}):
                        current[subkey] = deepcopy(subvalue)
        elif key == "full_name":
            if value and len(value.strip()) > len(str(current or "").strip()):
                target[key] = value
        elif current in (None, "") and value not in (None, ""):
            target[key] = value


def reconcile_document_authors(authors, skip_affiliation_conflicts=True):
    """Merge blank-ID authors into a unique same-name identified author."""
    authors = authors if isinstance(authors, list) else []
    groups = {}
    for position, author in enumerate(authors):
        if not isinstance(author, dict):
            continue
        name = normalized_author_name(author.get("full_name"))
        if name:
            groups.setdefault(name, []).append(position)

    replacements = {}
    skipped = set()
    resolved = 0
    ambiguous = 0
    affiliation_conflicts = 0
    for positions in groups.values():
        blank = [
            position for position in positions
            if authors[position].get("id") in (None, "")
        ]
        if not blank:
            continue
        identified_ids = {
            authors[position].get("id") for position in positions
            if authors[position].get("id") not in (None, "")
        }
        if len(identified_ids) != 1:
            if len(identified_ids) > 1:
                ambiguous += len(blank)
            continue

        persistent_id = next(iter(identified_ids))
        target_position = next(
            position for position in positions
            if authors[position].get("id") == persistent_id
        )
        target_affiliations = {
            affiliation.get("id")
            for affiliation in authors[target_position].get("affiliations") or []
            if isinstance(affiliation, dict)
            and affiliation.get("id") not in (None, "")
        }
        safe_blank = []
        for position in blank:
            blank_affiliations = {
                affiliation.get("id")
                for affiliation in authors[position].get("affiliations") or []
                if isinstance(affiliation, dict)
                and affiliation.get("id") not in (None, "")
            }
            if (
                skip_affiliation_conflicts
                and target_affiliations
                and blank_affiliations
                and not target_affiliations.intersection(blank_affiliations)
            ):
                affiliation_conflicts += 1
            else:
                safe_blank.append(position)
        if not safe_blank:
            continue

        group_positions = [target_position] + safe_blank
        target = deepcopy(authors[target_position])
        for position in group_positions:
            if position != target_position:
                _merge_author_metadata(target, authors[position])
        first_position = min(group_positions)
        replacements[first_position] = target
        skipped.update(set(group_positions) - {first_position})
        resolved += len(safe_blank)

    if not replacements:
        return deepcopy(authors), 0, ambiguous, affiliation_conflicts
    result = []
    for position, author in enumerate(authors):
        if position in replacements:
            result.append(replacements[position])
        elif position not in skipped:
            result.append(deepcopy(author))
    return result, resolved, ambiguous, affiliation_conflicts


def _bulk_write(collection, operations):
    try:
        collection.bulk_write(operations, ordered=False)
    except TypeError:
        # Compatibility with mongomock's reduced bulk API.
        for operation in operations:
            collection.update_one(operation._filter, operation._doc)


def reconcile_collection_authors(
    collection,
    dry_run=True,
    batch_size=1000,
    progress_interval=10000,
    skip_affiliation_conflicts=True,
):
    """Reconcile safe same-document author matches without adding fields."""
    report = {
        "documents_scanned": 0,
        "documents_changed": 0,
        "authors_resolved": 0,
        "ambiguous_authors": 0,
        "affiliation_conflicts": 0,
    }
    operations = []
    query = {"$or": [{"authors.id": ""}, {"authors.id": None}]}
    cursor = collection.find(query, {"authors": 1, "author_count": 1})
    for document in cursor:
        report["documents_scanned"] += 1
        authors, resolved, ambiguous, affiliation_conflicts = reconcile_document_authors(
            document.get("authors"),
            skip_affiliation_conflicts=skip_affiliation_conflicts,
        )
        report["ambiguous_authors"] += ambiguous
        report["affiliation_conflicts"] += affiliation_conflicts
        if resolved:
            report["documents_changed"] += 1
            report["authors_resolved"] += resolved
            if not dry_run:
                values = {"authors": authors}
                if "author_count" in document:
                    values["author_count"] = len(authors)
                operations.append(UpdateOne(
                    {"_id": document["_id"]}, {"$set": values}
                ))
        if len(operations) >= batch_size:
            _bulk_write(collection, operations)
            operations = []
        if (
            progress_interval
            and report["documents_scanned"] % progress_interval == 0
        ):
            action = "would_change" if dry_run else "changed"
            print(
                "INFO: author reconciliation scanned "
                f"{report['documents_scanned']} documents; "
                f"{action}={report['documents_changed']}; "
                f"resolved={report['authors_resolved']}"
            )
    if operations:
        _bulk_write(collection, operations)
    return report
