"""Find the identifiers that Kahi holds on more than one entity."""

import re
from collections import Counter
from urllib.parse import unquote

DOI_PREFIX = re.compile(r"^(?:doi:\s*|https?://(?:dx\.)?doi\.org/)", re.IGNORECASE)
ORCID_RE = re.compile(r"\d{4}-\d{4}-\d{4}-\d{3}[\dXx]")


def normalize_doi(value):
    """Bare DOI in lower case, the key of the resolver ``doi`` collection."""
    if not isinstance(value, str):
        return None
    doi = DOI_PREFIX.sub("", unquote(value).strip()).strip().lower()
    return doi if doi.startswith("10.") and "/" in doi else None


def normalize_orcid(value):
    """ORCID iD without the URL, the key of the resolver ``orcid`` collection."""
    match = ORCID_RE.search(value) if isinstance(value, str) else None
    return match.group(0).upper() if match else None


def shared_dois(works):
    """Return ``{doi: works}`` for every DOI that is the ``doi`` of two or more works."""
    counts = Counter()
    pipeline = [
        {"$match": {"doi": {"$type": "string", "$ne": ""}}},
        {"$group": {"_id": "$doi", "count": {"$sum": 1}}},
    ]
    for group in works.aggregate(pipeline, allowDiskUse=True):
        doi = normalize_doi(group["_id"])
        if doi:
            counts[doi] += group["count"]
    return {doi: count for doi, count in counts.items() if count > 1}


def shared_orcids(person):
    """Return ``{orcid: people}`` for every ORCID iD held by two or more people."""
    counts = Counter()
    cursor = person.find({"external_ids.source": "orcid"}, {"external_ids": 1})
    for document in cursor:
        orcids = {
            normalize_orcid(external.get("id"))
            for external in document.get("external_ids") or []
            if external.get("source") == "orcid"
        }
        counts.update(orcid for orcid in orcids if orcid)
    return {orcid: count for orcid, count in counts.items() if count > 1}
