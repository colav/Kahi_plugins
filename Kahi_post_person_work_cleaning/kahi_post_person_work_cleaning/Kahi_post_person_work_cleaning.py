from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from joblib import Parallel, delayed
from functools import lru_cache
from copy import deepcopy
import re
import unicodedata
from pymongo import UpdateOne


class Kahi_post_person_work_cleaning(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config
        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.works = self.db["works"]
        self.projects = self.db["projects"]
        self.person = self.db["person"]
        self.affiliations = self.db["affiliations"]
        plugin_config = config.get("post_person_work_cleaning", {})
        self.dry_run = plugin_config.get("dry_run", True)
        self.n_jobs = plugin_config.get("num_jobs", 1)
        self.verbose = plugin_config.get("verbose", 0)
        self.progress_interval = plugin_config.get("progress_interval", 1000)

        self.works.create_index("authors.id")
        self.projects.create_index("authors.id")
        self.person.create_index(
            [("full_name", 1), ("affiliations.id", 1)],
            name="full_name_affiliation_es",
            collation={"locale": "es", "strength": 1},
        )

    @lru_cache(maxsize=200_000)
    def affiliations_compatible(self, left, right):
        """Return True for equal affiliations or a direct parent/child relation."""
        left_ids = set(left)
        right_ids = set(right)
        if not left_ids or not right_ids:
            return False
        if left_ids.intersection(right_ids):
            return True
        return bool(
            self.affiliations.find_one(
                {"_id": {"$in": list(left_ids)}, "relations.id": {"$in": list(right_ids)}},
                {"_id": 1},
            )
            or self.affiliations.find_one(
                {"_id": {"$in": list(right_ids)}, "relations.id": {"$in": list(left_ids)}},
                {"_id": 1},
            )
        )

    @staticmethod
    def authoritative_ids(person):
        """Return normalized identifiers that can distinguish natural persons."""
        identifiers = {"cod_rh": set(), "national_id": set(), "orcid": set()}
        for external_id in person.get("external_ids", []):
            source = unicodedata.normalize(
                "NFKD", str(external_id.get("source", ""))
            ).encode("ascii", "ignore").decode().lower()
            value = external_id.get("id")
            if isinstance(value, dict):
                cod_rh = value.get("COD_RH")
                if cod_rh not in (None, ""):
                    normalized = re.sub(
                        r"\D", "", str(cod_rh)).lstrip("0") or "0"
                    identifiers["cod_rh"].add(normalized)
                continue
            if value in (None, ""):
                continue
            if source == "orcid":
                normalized = re.sub(r"[^0-9X]", "", str(value).upper())
                if normalized:
                    identifiers["orcid"].add(normalized)
            elif "cedula" in source or source in {"national_id", "documento de identidad"}:
                normalized = re.sub(r"\D", "", str(value)).lstrip("0") or "0"
                identifiers["national_id"].add(normalized)
        return identifiers

    @classmethod
    def identities_are_distinct(cls, current, candidate):
        """Require conflicting IDs and reject conflicts when another ID is shared."""
        current_ids = cls.authoritative_ids(current)
        candidate_ids = cls.authoritative_ids(candidate)
        if any(
            current_ids[kind].intersection(candidate_ids[kind])
            for kind in current_ids
        ):
            return False
        return any(
            current_ids[kind] and candidate_ids[kind]
            for kind in current_ids
        )

    def find_better_candidate(self, author, full_name, work_affiliations):
        """Find a same-name candidate with matching affiliation and distinct identity."""
        if not full_name or not work_affiliations:
            return None
        candidates = self.person.find(
            {"_id": {"$ne": author["_id"]}, "full_name": full_name},
            {"_id": 1, "full_name": 1, "external_ids": 1, "affiliations.id": 1},
            collation={"locale": "es", "strength": 1},
        )
        work_ids = tuple(sorted(work_affiliations, key=str))
        for candidate in candidates:
            candidate_ids = tuple(sorted({
                affiliation.get("id")
                for affiliation in candidate.get("affiliations", [])
                if affiliation.get("id") not in (None, "")
            }, key=str))
            if (
                self.identities_are_distinct(author, candidate)
                and self.affiliations_compatible(candidate_ids, work_ids)
            ):
                return candidate
        return None

    @staticmethod
    def get_cod_rh(author):
        for external_id in author.get("external_ids", []):
            value = external_id.get("id")
            if external_id.get("source") in (
                    "scienti",
                    "minciencias") and isinstance(
                    value,
                    dict):
                if value.get("COD_RH"):
                    return value["COD_RH"]
        return None

    def process_one(self, author):
        works = self.works.find(
            {"authors.id": author["_id"]}, {"authors": 1, "external_ids": 1})
        cod_rh = self.get_cod_rh(author)
        person_affiliations = {
            affiliation.get("id") for affiliation in author.get(
                "affiliations",
                []) if affiliation.get("id") not in (
                None,
                "")}
        changed = 0
        for work in works:
            # Check if the author has the cod_rh in the work
            cod_rh_work = [
                x["id"]["COD_RH"]
                for x in work["external_ids"]
                if x.get("source") == "scienti" and "COD_RH" in x.get("id", {})
            ]
            if cod_rh in cod_rh_work:
                # The author has the cod_rh in the work
                continue

            # Check if the author has the affiliation in the work
            for work_author in work.get("authors", []):
                # Only analyze the author we are looking for in the work
                if work_author["id"] == author["_id"]:
                    if not work_author.get("affiliations"):
                        # if not affiliation we assume it is right
                        continue
                    work_affiliations = {
                        affiliation.get("id")
                        for affiliation in work_author.get("affiliations", [])
                        if affiliation.get("id") not in (None, "")
                    }
                    person_ids = tuple(sorted(person_affiliations, key=str))
                    work_ids = tuple(sorted(work_affiliations, key=str))
                    candidate = None
                    if not self.affiliations_compatible(person_ids, work_ids):
                        candidate = self.find_better_candidate(
                            author,
                            work_author.get("full_name") or author.get("full_name"),
                            work_affiliations,
                        )
                    if candidate is not None:
                        changed += 1
                        if not self.dry_run:
                            self.works.update_one(
                                {"_id": work["_id"], "authors.id": author["_id"]},
                                {"$set": {
                                    "authors.$.id": candidate["_id"],
                                    "authors.$.full_name": candidate.get(
                                        "full_name", work_author.get("full_name", "")
                                    ),
                                }},
                            )
        return changed

    @staticmethod
    def consolidate_authors(authors):
        """Merge repeated, non-empty author IDs while preserving author order."""
        consolidated = []
        positions = {}
        removed = 0
        for author in authors:
            author_id = author.get("id")
            if author_id in (None, ""):
                consolidated.append(deepcopy(author))
                continue
            try:
                position = positions.get(author_id)
            except TypeError:
                consolidated.append(deepcopy(author))
                continue
            if position is None:
                positions[author_id] = len(consolidated)
                consolidated.append(deepcopy(author))
                continue

            target = consolidated[position]
            removed += 1
            for key, value in author.items():
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
                    if value and len(str(value).strip()) > len(str(current or "").strip()):
                        target[key] = value
                elif current in (None, "") and value not in (None, ""):
                    target[key] = value
        return consolidated, removed

    def consolidate_duplicate_entity_authors(self, collection, entity_name):
        """Consolidate entities containing a repeated, non-empty author ID."""
        pipeline = [
            {"$match": {"authors.1": {"$exists": True}}},
            {"$project": {
                "authors": 1,
                "author_count": 1,
                "nonempty_author_ids": {"$filter": {
                    "input": "$authors.id",
                    "as": "author_id",
                    "cond": {"$and": [
                        {"$ne": ["$$author_id", None]},
                        {"$ne": ["$$author_id", ""]},
                    ]},
                }},
            }},
            {"$match": {"$expr": {"$lt": [
                {"$size": {"$setUnion": ["$nonempty_author_ids", []]}},
                {"$size": "$nonempty_author_ids"},
            ]}}},
        ]
        operations = []
        entities_changed = 0
        references_removed = 0
        for entity in collection.aggregate(pipeline, allowDiskUse=True):
            authors, removed = self.consolidate_authors(entity.get("authors", []))
            if not removed:
                continue
            entities_changed += 1
            references_removed += removed
            if not self.dry_run:
                operations.append(UpdateOne(
                    {"_id": entity["_id"]},
                    {"$set": {"authors": authors, "author_count": len(authors)}},
                ))
                if len(operations) >= 1000:
                    collection.bulk_write(operations, ordered=False)
                    operations = []
            if self.verbose and entities_changed % self.progress_interval == 0:
                print(
                    f"INFO: duplicate-author cleanup: {entities_changed} "
                    f"{entity_name}, "
                    f"{references_removed} references removed"
                )
        if operations:
            collection.bulk_write(operations, ordered=False)
        return entities_changed, references_removed

    def consolidate_duplicate_work_authors(self):
        """Preserve the public helper used by previous versions and tests."""
        return self.consolidate_duplicate_entity_authors(
            self.works, "works"
        )

    def run(self):
        # https://github.com/colav/impactu/issues/141
        # only authors from scienti, staff or ciarp
        authors = self.person.find(
            {"updated.source": {"$in": ["scienti", "staff", "ciarp"]}},
            {"_id": 1, "full_name": 1, "external_ids": 1, "affiliations": 1},
        ).batch_size(1000)
        results = Parallel(
            n_jobs=self.n_jobs, verbose=self.verbose, backend="threading",
            pre_dispatch="2*n_jobs", return_as="generator_unordered",
        )(delayed(self.process_one)(author) for author in authors)
        total = sum(results)
        print(
            f"INFO: {'Would relink' if self.dry_run else 'Relinked'} {total} work authors")
        works_changed, references_removed = self.consolidate_duplicate_work_authors()
        print(
            f"INFO: {'Would consolidate' if self.dry_run else 'Consolidated'} "
            f"{works_changed} works, {references_removed} duplicate author references"
        )
        projects_changed, references_removed = (
            self.consolidate_duplicate_entity_authors(
                self.projects, "projects"
            )
        )
        print(
            f"INFO: {'Would consolidate' if self.dry_run else 'Consolidated'} "
            f"{projects_changed} projects, {references_removed} "
            "duplicate author references"
        )
        return 0
