"""Resolve OpenAlex identities and merge institution years without lost updates."""
from collections import OrderedDict
from datetime import datetime, timezone
import json
import logging
import re
from threading import Lock

from kahi_impactu_utils.Utils import normalize_publication_year, parse_orcid_id_from_url


LOGGER = logging.getLogger("kahi_openalex_person")


def report_conflict(reason, author, candidates=(), institution=None):
    """Warnings reach the ETL's persistent stdout/stderr log; no model fields."""
    LOGGER.warning("openalex_person.identity_conflict %s", json.dumps({
        "reason": reason,
        "openalex_id": author.get("id"),
        "orcid": author.get("ids", {}).get("orcid") or author.get("orcid"),
        "candidate_ids": [str(item["_id"]) for item in candidates],
        "institution": institution,
    }, ensure_ascii=False, default=str))


def canonical_openalex_id(value, kind="A"):
    if not isinstance(value, str):
        return None
    match = re.fullmatch(
        rf"(?:https?://(?:www\.)?openalex\.org/)?({kind}\d+)/?", value.strip(), re.I
    )
    return match.group(1).upper() if match else None


def canonical_orcid(value):
    """Normalize with Utils and validate the ISO 7064 MOD 11-2 check digit."""
    if not isinstance(value, str):
        return None
    url = parse_orcid_id_from_url(value)
    if not url or not re.fullmatch(r"https://orcid\.org/\d{4}-\d{4}-\d{4}-\d{3}[\dX]", url):
        return None
    digits = url.rsplit("/", 1)[-1].replace("-", "")
    total = 0
    for digit in digits[:15]:
        total = (total + int(digit)) * 2
    check = (12 - total % 11) % 11
    return url.rsplit("/", 1)[-1] if digits[-1] == ("X" if check == 10 else str(check)) else None


def identifier_variants(identifier, domain):
    variants = [identifier, f"https://{domain}/{identifier}", f"http://{domain}/{identifier}"]
    if identifier.endswith("X"):
        variants.extend(item[:-1] + "x" for item in variants.copy())
    return variants


def institution_key(institution):
    openalex_id = canonical_openalex_id(institution.get("id"), "I")
    ror = institution.get("ror")
    if isinstance(ror, str):
        ror = re.sub(r"^https?://(?:www\.)?ror\.org/", "", ror.strip()).rstrip("/").lower()
        if not re.fullmatch(r"0[0-9a-z]{8}", ror):
            ror = None
    else:
        ror = None
    return ror, openalex_id


def author_institutions(author):
    last_known = author.get("last_known_institutions") or []
    if last_known:
        yield from last_known
        yield from (record.get("institution") or {} for record in author.get("affiliations") or [])


def resolve_person(collection, author):
    """Use a unique OpenAlex ID, then a unique valid ORCID; never pick arbitrarily."""
    openalex_id = canonical_openalex_id(author.get("ids", {}).get("openalex") or author.get("id"))
    orcid = canonical_orcid(author.get("ids", {}).get("orcid") or author.get("orcid"))
    for source, identifier, domain in (
        ("openalex", openalex_id, "openalex.org"), ("orcid", orcid, "orcid.org")
    ):
        if not identifier:
            continue
        candidates = list(collection.find({"external_ids": {"$elemMatch": {
            "source": source, "id": {"$in": identifier_variants(identifier, domain)}
        }}}).limit(2))
        if len(candidates) > 1:
            report_conflict(f"ambiguous_{source}", author, candidates)
            return None, False
        if candidates:
            person = candidates[0]
            existing_openalex = {
                canonical_openalex_id(item.get("id"))
                for item in person.get("external_ids", []) if item.get("source") == "openalex"
            } - {None}
            existing_orcids = {
                canonical_orcid(item.get("id"))
                for item in person.get("external_ids", []) if item.get("source") == "orcid"
            } - {None}
            if (source == "orcid" and existing_openalex and openalex_id not in existing_openalex):
                report_conflict("orcid_has_other_openalex_id", author, candidates)
                return None, False
            if orcid and existing_orcids and orcid not in existing_orcids:
                report_conflict("conflicting_orcid", author, candidates)
                return None, False
            return person, True
    return None, True


class InstitutionResolver:
    """One indexed query per cache miss, shared safely by the plugin's threads."""

    def __init__(self, collection, cache_size=32768):
        self.collection = collection
        self.cache_size = cache_size
        self.cache = OrderedDict()
        self.lock = Lock()
        self.current_year = datetime.now(timezone.utc).year

    def prefetch(self, authors):
        """Resolve a bounded author batch with a single indexed $in query."""
        keys = set(institution_key(institution) for author in authors for institution in author_institutions(author))
        with self.lock:
            keys.difference_update(self.cache)
        if not keys:
            return
        ror_ids = {value for ror, _oa in keys if ror for value in identifier_variants(ror, "ror.org")}
        openalex_ids = {value for _ror, oa in keys if oa for value in identifier_variants(oa, "openalex.org")}
        clauses = [{"external_ids": {"$elemMatch": {"source": source, "id": {"$in": list(ids)}}}}
                   for source, ids in (("ror", ror_ids), ("openalex", openalex_ids)) if ids]
        by_identifier = {}
        if clauses:
            for record in self.collection.find({"$or": clauses}, {"names": 1, "types": 1, "external_ids": 1}):
                identifiers = record.pop("external_ids", [])
                for identifier in identifiers:
                    source = identifier.get("source")
                    if source == "ror":
                        normalized = institution_key({"ror": identifier.get("id")})[0]
                    elif source == "openalex":
                        normalized = canonical_openalex_id(identifier.get("id"), "I")
                    else:
                        continue
                    if normalized:
                        by_identifier.setdefault((source, normalized), {})[record["_id"]] = record
        resolved = {}
        for ror, openalex_id in keys:
            matches = dict(by_identifier.get(("ror", ror), {}))
            matches.update(by_identifier.get(("openalex", openalex_id), {}))
            candidates = list(matches.values())[:2]
            resolved[(ror, openalex_id)] = (candidates[0] if len(candidates) == 1 else None, candidates)
        with self.lock:
            self.cache.update(resolved)
            while len(self.cache) > self.cache_size:
                self.cache.popitem(last=False)

    def resolve(self, institution, author):
        key = institution_key(institution)
        ror, openalex_id = key
        with self.lock:
            cached = self.cache.get(key)
            if cached is not None:
                self.cache.move_to_end(key)
        if cached is None:
            clauses = [{"external_ids": {"$elemMatch": {
                "source": source, "id": {"$in": identifier_variants(value, domain)}
            }}} for source, value, domain in (
                ("ror", ror, "ror.org"), ("openalex", openalex_id, "openalex.org")
            ) if value]
            candidates = list(self.collection.find(
                {"$or": clauses}, {"names": 1, "types": 1}
            ).limit(2)) if clauses else []
            cached = (candidates[0] if len(candidates) == 1 else None, candidates)
            with self.lock:
                self.cache[key] = cached
                self.cache.move_to_end(key)
                if len(self.cache) > self.cache_size:
                    self.cache.popitem(last=False)
        record, candidates = cached
        if record is None:
            report_conflict("ambiguous_institution" if candidates else "unresolved_institution",
                            author, candidates, institution)
        return record


def collect_affiliations(author, resolver):
    """Select last-known institutions and enrich only them with historical years."""
    affiliations = {}
    for institution in author.get("last_known_institutions") or []:
        record = resolver.resolve(institution, author)
        if not record or record["_id"] in affiliations:
            continue
        names = record.get("names") or []
        name = next((item["name"] for item in names if item.get("lang") == "es"), None)
        if name is None:
            name = next((item["name"] for item in reversed(names) if item.get("lang") == "en"),
                        names[0]["name"] if names else "")
        affiliations[record["_id"]] = {
            "id": record["_id"], "name": name, "types": record.get("types", []),
            "start_date": -1, "end_date": -1, "years": [],
        }

    if not affiliations:
        return []
    for evidence in author.get("affiliations") or []:
        record = resolver.resolve(evidence.get("institution") or {}, author)
        if not record or record["_id"] not in affiliations:
            continue
        values = evidence.get("years") or []
        if not isinstance(values, (list, tuple)):
            LOGGER.warning("openalex_person.invalid_years openalex_id=%s institution_id=%s",
                           author.get("id"), record["_id"])
            continue
        years = {year for value in values
                 if (year := normalize_publication_year(value, resolver.current_year)) is not None}
        affiliation = affiliations[record["_id"]]
        affiliation["years"] = sorted(set(affiliation["years"]) | years)
    return list(affiliations.values())


def affiliation_merge_expression(incoming):
    """Merge on the server's current array; retain all existing metadata and years."""
    return {"$let": {"vars": {
        "existing": {"$ifNull": ["$affiliations", []]},
        "incoming": {"$literal": incoming},
    }, "in": {"$concatArrays": [
        {"$map": {"input": "$$existing", "as": "aff", "in": {"$let": {
            "vars": {"matches": {"$filter": {"input": "$$incoming", "as": "new",
                         "cond": {"$eq": ["$$new.id", "$$aff.id"]}}}},
            "in": {"$cond": [{"$and": [
                {"$gt": [{"$size": "$$matches"}, 0]},
                {"$or": [
                    {"$gt": [{"$size": {"$ifNull": [
                        {"$getField": {"field": "years", "input": {"$first": "$$matches"}}}, []
                    ]}}, 0]},
                    {"$ne": [{"$type": "$$aff.years"}, "missing"]},
                ]},
            ]},
                {"$mergeObjects": ["$$aff", {"years": {"$sortArray": {
                    "input": {"$setUnion": [{"$ifNull": ["$$aff.years", []]},
                              {"$getField": {"field": "years", "input": {"$first": "$$matches"}}}]},
                    "sortBy": 1,
                }}}]}, "$$aff"]},
        }}}},
        {"$filter": {"input": "$$incoming", "as": "new", "cond": {
            "$not": [{"$in": ["$$new.id", "$$existing.id"]}]
        }}},
    ]}}}


def append_unique_expression(field, incoming):
    """Preserve array order while providing the old $addToSet semantics atomically."""
    return {"$let": {"vars": {
        "existing": {"$ifNull": [f"${field}", []]}, "incoming": {"$literal": incoming},
    }, "in": {"$concatArrays": ["$$existing", {"$filter": {
        "input": "$$incoming", "as": "new", "cond": {"$not": [{"$in": ["$$new", "$$existing"]}]}
    }}]}}}


def update_person(collection, person, author, affiliations, additions=None):
    """One atomic update per author, including when an already-processed author resumes."""
    fields = {field: append_unique_expression(field, values)
              for field, values in (additions or {}).items() if values}
    if affiliations:
        fields["affiliations"] = affiliation_merge_expression(affiliations)
    if not fields:
        return
    query = {"_id": person["_id"]}
    openalex_id = canonical_openalex_id(author.get("ids", {}).get("openalex") or author.get("id"))
    # Protect an ORCID-to-person association from another thread attaching a different OA ID.
    if openalex_id and not any(
        item.get("source") == "openalex" and canonical_openalex_id(item.get("id")) == openalex_id
        for item in person.get("external_ids", [])
    ):
        query["external_ids"] = {"$not": {"$elemMatch": {
            "source": "openalex", "id": {"$nin": identifier_variants(openalex_id, "openalex.org")}
        }}}
    result = collection.update_one(query, [{"$set": fields}])
    if not result.matched_count:
        report_conflict("person_identity_changed_during_update", author, [person])
