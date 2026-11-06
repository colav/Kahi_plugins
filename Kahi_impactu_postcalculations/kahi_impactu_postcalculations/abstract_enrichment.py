"""Enrich missing Kahi abstracts from the local abstract resolver."""

from copy import deepcopy
from datetime import datetime, timezone
from difflib import SequenceMatcher
from itertools import islice
import re
import unicodedata
from urllib.parse import unquote

from pymongo import UpdateOne


DOI_PREFIX = re.compile(
    r"^(?:doi:\s*|https?://(?:dx\.)?doi\.org/)", re.IGNORECASE
)


def utc_now():
    return datetime.now(timezone.utc)


def batched(iterable, size):
    iterator = iter(iterable)
    while batch := list(islice(iterator, size)):
        yield batch


def normalize_doi(value):
    """Return a comparable bare DOI while preserving meaningful suffix text."""
    if not isinstance(value, str):
        return None
    value = unquote(value).strip()
    value = DOI_PREFIX.sub("", value).strip().casefold()
    return value if value.startswith("10.") and "/" in value else None


def doi_variants(doi):
    """Return common stored representations for one normalized DOI."""
    return (
        doi,
        f"https://doi.org/{doi}",
        f"http://doi.org/{doi}",
        f"https://dx.doi.org/{doi}",
        f"doi:{doi}",
    )


def work_doi(work):
    normalized = normalize_doi(work.get("doi"))
    if normalized:
        return normalized
    for external_id in work.get("external_ids", []):
        if not isinstance(external_id, dict):
            continue
        if str(external_id.get("source", "")).casefold() != "doi":
            continue
        normalized = normalize_doi(external_id.get("id"))
        if normalized:
            return normalized
    return None


def normalized_title(value):
    if not isinstance(value, str):
        return ""
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode().casefold()
    return " ".join(re.findall(r"[a-z0-9]+", value))


def _ratio(left, right):
    return 100.0 * SequenceMatcher(None, left, right).ratio()


def title_similarity(left, right):
    """Return a fuzzy 0-100 score robust to punctuation and word order."""
    left = normalized_title(left)
    right = normalized_title(right)
    if not left or not right:
        return 0.0
    if left == right:
        return 100.0

    left_tokens = set(left.split())
    right_tokens = set(right.split())
    intersection = left_tokens & right_tokens
    left_only = left_tokens - intersection
    right_only = right_tokens - intersection
    common = " ".join(sorted(intersection))
    combined_left = " ".join(sorted(intersection | left_only))
    combined_right = " ".join(sorted(intersection | right_only))
    scores = [
        _ratio(left, right),
        _ratio(" ".join(sorted(left_tokens)), " ".join(sorted(right_tokens))),
    ]
    if common:
        scores.extend([
            _ratio(common, combined_left),
            _ratio(common, combined_right),
            _ratio(combined_left, combined_right),
        ])
    return round(max(scores), 2)


def work_titles(work):
    return [
        item.get("title")
        for item in work.get("titles", [])
        if isinstance(item, dict) and normalized_title(item.get("title"))
    ]


def resolver_titles(resolved):
    titles = [
        item.get("title")
        for item in resolved.get("source_titles", [])
        if isinstance(item, dict) and normalized_title(item.get("title"))
    ]
    match = resolved.get("title_match") or {}
    candidate_title = match.get("candidate_title")
    if normalized_title(candidate_title):
        titles.append(candidate_title)
    if not titles:
        titles.extend(
            item.get("title")
            for item in resolved.get("titles", [])
            if isinstance(item, dict) and normalized_title(item.get("title"))
        )
    return list(dict.fromkeys(titles))


def best_title_match(work, resolved):
    best = {"score": 0.0, "work_title": None, "resolver_title": None}
    for left in work_titles(work):
        for right in resolver_titles(resolved):
            score = title_similarity(left, right)
            if score > best["score"]:
                best = {
                    "score": score,
                    "work_title": left,
                    "resolver_title": right,
                }
    return best


def abstract_value(resolved):
    """Return only the resolver's canonical inverted-index representation."""
    inverted = resolved.get("abstract_inverted_index")
    if isinstance(inverted, dict) and inverted:
        return deepcopy(inverted)
    return None


def abstract_entry(resolved):
    value = abstract_value(resolved)
    if value is None:
        return None
    return {
        "abstract": value,
        "lang": resolved.get("abstract_lang") or resolved.get("language") or "",
        "source": resolved.get("source") or "impactu_abstract_resolver",
        "provenance": "impactu_abstract_resolver",
    }


def _bulk_write(collection, operations):
    try:
        return collection.bulk_write(operations, ordered=False)
    except TypeError:
        # Compatibility with mongomock's reduced bulk API.
        modified = 0
        for operation in operations:
            result = collection.update_one(operation._filter, operation._doc)
            modified += result.modified_count
        return type("BulkResult", (), {"modified_count": modified})()


def enrich_missing_abstracts(
    works,
    resolved,
    runs,
    *,
    dry_run=True,
    batch_size=1000,
    title_threshold=90.0,
    verbose=0,
):
    """Resolve DOI/title matches and persist one summary document per run."""
    if batch_size < 1:
        raise ValueError("abstracts_batch_size must be greater than zero")
    if not 0 <= title_threshold <= 100:
        raise ValueError("abstracts_title_threshold must be between 0 and 100")

    resolved.create_index("doi", name="doi_1")
    runs.create_index("started_at")
    run = {
        "started_at": utc_now(),
        "finished_at": None,
        "status": "RUNNING",
        "dry_run": dry_run,
        "target": {
            "database": works.database.name,
            "collection": works.name,
        },
        "resolver": {
            "database": resolved.database.name,
            "collection": resolved.name,
        },
        "parameters": {
            "batch_size": batch_size,
            "title_threshold": title_threshold,
        },
    }
    run_id = runs.insert_one(run).inserted_id
    stats = {
        "works_scanned": 0,
        "unique_dois_queried": 0,
        "resolver_queries": 0,
        "resolver_documents_examined": 0,
        "doi_matches": 0,
        "doi_not_found": 0,
        "resolver_without_inverted_abstract": 0,
        "missing_titles": 0,
        "title_matches": 0,
        "title_rejections": 0,
        "works_changed": 0,
        "abstracts_extracted": 0,
        "source_counts": {},
        "publication_status_counts": {},
        "rights_status_counts": {},
        "display_permission_counts": {"allowed": 0, "not_allowed": 0},
    }
    seen_dois = set()
    query = {
        "$and": [
            {"$or": [
                {"doi": {"$type": "string", "$ne": ""}},
                {"external_ids": {"$elemMatch": {
                    "source": "doi", "id": {"$type": "string", "$ne": ""},
                }}},
            ]},
            {"$or": [
                {"abstracts": {"$exists": False}},
                {"abstracts": None},
                {"abstracts": {"$size": 0}},
            ]},
        ]
    }
    projection = {"doi": 1, "external_ids": 1, "titles": 1, "abstracts": 1}

    try:
        cursor = works.find(query, projection, no_cursor_timeout=True).batch_size(
            batch_size
        )
        for batch in batched(cursor, batch_size):
            stats["works_scanned"] += len(batch)
            dois = {doi for doi in map(work_doi, batch) if doi}
            new_dois = dois - seen_dois
            seen_dois.update(new_dois)
            stats["unique_dois_queried"] += len(new_dois)
            variants = [variant for doi in dois for variant in doi_variants(doi)]
            stats["resolver_queries"] += 1
            raw_documents = list(resolved.find(
                {
                    "doi": {"$in": variants},
                    "rights.can_store": True,
                },
                {
                    "doi": 1, "abstract_inverted_index": 1,
                    "abstract_lang": 1, "language": 1, "source": 1,
                    "source_titles": 1, "titles": 1, "title_match": 1,
                    "publication_status": 1, "rights": 1,
                },
            ).hint("doi_1"))
            stats["resolver_documents_examined"] += len(raw_documents)
            by_doi = {}
            for document in raw_documents:
                normalized = normalize_doi(document.get("doi"))
                if normalized:
                    by_doi.setdefault(normalized, []).append(document)

            operations = []
            pending_changes = 0
            for work in batch:
                doi = work_doi(work)
                candidates = by_doi.get(doi, [])
                if not candidates:
                    stats["doi_not_found"] += 1
                    continue
                stats["doi_matches"] += 1
                usable = [item for item in candidates if abstract_value(item) is not None]
                if not usable:
                    stats["resolver_without_inverted_abstract"] += 1
                    continue
                if not work_titles(work):
                    stats["missing_titles"] += 1
                    continue
                ranked = sorted(
                    ((best_title_match(work, item), item) for item in usable),
                    key=lambda pair: pair[0]["score"],
                    reverse=True,
                )
                title_match, selected = ranked[0]
                if title_match["score"] < title_threshold:
                    stats["title_rejections"] += 1
                    continue
                entry = abstract_entry(selected)
                if entry is None:
                    stats["resolver_without_inverted_abstract"] += 1
                    continue
                stats["title_matches"] += 1
                stats["abstracts_extracted"] += 1
                source = entry["source"]
                stats["source_counts"][source] = (
                    stats["source_counts"].get(source, 0) + 1
                )
                publication_status = selected.get("publication_status") or "unknown"
                stats["publication_status_counts"][publication_status] = (
                    stats["publication_status_counts"].get(
                        publication_status, 0
                    ) + 1
                )
                rights_status = (selected.get("rights") or {}).get("status") or "unknown"
                stats["rights_status_counts"][rights_status] = (
                    stats["rights_status_counts"].get(rights_status, 0) + 1
                )
                display_key = (
                    "allowed"
                    if (selected.get("rights") or {}).get("can_display") is True
                    else "not_allowed"
                )
                stats["display_permission_counts"][display_key] += 1
                pending_changes += 1
                if not dry_run:
                    operations.append(UpdateOne(
                        {
                            "_id": work["_id"],
                            "$or": [
                                {"abstracts": {"$exists": False}},
                                {"abstracts": None},
                                {"abstracts": {"$size": 0}},
                            ],
                        },
                        {"$set": {"abstracts": [entry]}},
                    ))
            if dry_run:
                stats["works_changed"] += pending_changes
            elif operations:
                stats["works_changed"] += _bulk_write(
                    works, operations
                ).modified_count
            if verbose and stats["works_scanned"] % (batch_size * 10) == 0:
                print(
                    "INFO: abstract enrichment: "
                    f"{stats['works_scanned']} works scanned; "
                    f"{stats['works_changed']} changed"
                )
        runs.update_one(
            {"_id": run_id},
            {"$set": {
                "finished_at": utc_now(),
                "status": "COMPLETED",
                "stats": stats,
            }},
        )
    except Exception as error:
        runs.update_one(
            {"_id": run_id},
            {"$set": {
                "finished_at": utc_now(),
                "status": "FAILED",
                "stats": stats,
                "error": repr(error),
            }},
        )
        raise
    stats["run_id"] = run_id
    return stats
