"""Settle the identifiers that several works or people share.

``collect`` finds the disputed identifiers (a DOI that is the ``doi`` of two
or more works, an ORCID iD held by two or more people) and queues them in the
identifier resolver database as ``{"_id": <identifier>, "status": "pending"}``.
The ``identifier_resolver_capture`` DAG of impactu_airflow then asks the public
registries what each one identifies; this plugin never uses the network.

An identifier already in the resolver is left as it is, so ``collect`` can run
after every works stage and only the new disputes are queued.

``apply`` settles the shared DOIs with what the resolver stored (see
``disputes.decide``): a work that loses the DOI gets ``doi: None``, loses the
matching ``external_ids`` entry and gains an ``updated`` entry with source
``disputes``. Every decision is kept in ``identifier_disputes``,
including what was removed. DOIs the resolver has not resolved yet are skipped.
A shared ORCID iD stays with the person whose name matches the public record
(see ``disputes.decide_orcid``); the others lose the ``external_ids`` entry.
With ``dry_run`` nothing is changed and the decisions go to
``identifier_disputes_dry_run``.
"""

from collections import Counter, defaultdict
from time import time

from kahi.KahiBase import KahiBase
from kahi.progress import ProgressLogger
from pymongo import MongoClient, UpdateOne

from kahi_post_identifier_disputes.disputes import decide, decide_orcid, work_titles
from kahi_post_identifier_disputes.identifiers import (
    normalize_doi,
    normalize_orcid,
    shared_dois,
    shared_orcids,
)

TASKS = ("collect", "apply")
BATCH_SIZE = 1000
GROUP_BATCH = 200
# Source name of the ``updated`` entry; the trace collection is ``identifier_disputes``.
SOURCE = "disputes"
WORK_PROJECTION = {
    "titles.title": 1, "authors.full_name": 1, "doi": 1, "external_ids": 1, "updated": 1,
}


class Kahi_post_identifier_disputes(KahiBase):
    """Queue the shared DOIs and ORCID iDs for the identifier resolver."""

    config = {}

    @staticmethod
    def manages_checkpoints(task_config):
        """Always run: the result depends on the works and people loaded so far."""
        return True

    def __init__(self, config):
        self.config = config
        plugin = config["post_identifier_disputes"]
        self.task = plugin.get("task")
        if self.task not in TASKS:
            raise RuntimeError(
                f"post_identifier_disputes: unknown task {self.task!r}; use one of {TASKS}"
            )
        self.database_name = config["database_name"]
        self.client = MongoClient(config["database_url"])
        self.db = self.client[self.database_name]
        self.resolver_client = MongoClient(
            plugin.get("resolver_database_url", config["database_url"])
        )
        self.resolver = self.resolver_client[
            plugin.get("resolver_database_name", "impactu_identifier_resolver")
        ]
        self.dry_run = bool(plugin.get("dry_run", False))
        self.verbose = plugin.get("verbose", 0)

    def queue(self, kind, identifiers, now):
        """Queue the identifiers of one kind; return the status of each in the resolver."""
        collection = self.resolver[kind]
        collection.create_index([("status", 1)])
        identifiers = sorted(identifiers)
        progress = ProgressLogger(f"post_identifier_disputes:collect:{kind}",
                                  total=len(identifiers)).start()
        for start in range(0, len(identifiers), BATCH_SIZE):
            batch = identifiers[start:start + BATCH_SIZE]
            known = {doc["_id"] for doc in collection.find({"_id": {"$in": batch}}, {"_id": 1})}
            new = [identifier for identifier in batch if identifier not in known]
            if new:
                collection.bulk_write([
                    UpdateOne(
                        {"_id": identifier},
                        {"$setOnInsert": {"status": "pending", "requested_at": now,
                                          "requested_by": self.database_name}},
                        upsert=True,
                    )
                    for identifier in new
                ], ordered=False)
            progress.step("nuevos", len(new))
            progress.step("existentes", len(batch) - len(new))
        statuses = Counter()
        for start in range(0, len(identifiers), BATCH_SIZE):
            batch = identifiers[start:start + BATCH_SIZE]
            for doc in collection.find({"_id": {"$in": batch}}, {"status": 1}):
                statuses[doc.get("status") or "pending"] += 1
        progress.done(**dict(statuses))
        return dict(statuses)

    def collect(self):
        now = int(time())
        dois = shared_dois(self.db["works"])
        orcids = shared_orcids(self.db["person"])
        summary = {
            "doi": {"shared": len(dois), "works": sum(dois.values()),
                    "resolver": self.queue("doi", dois, now)},
            "orcid": {"shared": len(orcids), "people": sum(orcids.values()),
                      "resolver": self.queue("orcid", orcids, now)},
        }
        self.db["identifier_disputes_runs"].insert_one(
            {"task": "collect", "time": now, "resolver": self.resolver.name, **summary}
        )
        return summary

    def doi_groups(self):
        """Return ``{doi: [work ids]}`` for the DOIs shared by two or more works."""
        groups = defaultdict(list)
        cursor = self.db["works"].find({"doi": {"$type": "string", "$ne": ""}}, {"doi": 1})
        for work in cursor:
            doi = normalize_doi(work["doi"])
            if doi:
                groups[doi].append(work["_id"])
        return {doi: ids for doi, ids in groups.items() if len(ids) > 1}

    @staticmethod
    def removal(work, doi, now):
        """Fields of ``work`` without ``doi`` and what was taken out."""
        external_ids = work.get("external_ids") or []
        removed = [item for item in external_ids
                   if item.get("source") == "doi" and normalize_doi(item.get("id")) == doi]
        updated = [item for item in work.get("updated") or [] if item.get("source") != SOURCE]
        updated.append({"source": SOURCE, "time": now})
        fields = {
            "doi": None,
            "external_ids": [item for item in external_ids if item not in removed],
            "updated": updated,
        }
        return fields, {"doi": work.get("doi"), "external_ids": removed}

    def apply(self):
        now = int(time())
        works = self.db["works"]
        audit = self.db["identifier_disputes_dry_run" if self.dry_run else "identifier_disputes"]
        if self.dry_run:
            audit.delete_many({})
        groups = self.doi_groups()
        dois = sorted(groups)
        progress = ProgressLogger("post_identifier_disputes:apply:doi", total=len(dois)).start(
            "dry_run" if self.dry_run else "")
        counts = Counter()
        for start in range(0, len(dois), GROUP_BATCH):
            batch = dois[start:start + GROUP_BATCH]
            registry = {
                doc["_id"]: doc for doc in self.resolver["doi"].find(
                    {"_id": {"$in": batch}, "status": {"$in": ["ok", "not_found"]}})
            }
            updates, records = [], []
            for doi in batch:
                if doi not in registry:
                    counts["pending"] += len(groups[doi])
                    progress.step("pendientes")
                    continue
                found = {work["_id"]: work
                         for work in works.find({"_id": {"$in": groups[doi]}}, WORK_PROJECTION)}
                members = [found[work_id] for work_id in groups[doi] if work_id in found]
                decisions = decide(registry[doi], members)
                for work, decision in zip(members, decisions):
                    counts[decision["action"]] += 1
                    counts["{}:{}".format(decision["action"], decision["reason"])] += 1
                    record = {
                        "kind": "doi", "identifier": doi, "work_id": work["_id"],
                        "work_title": (work_titles(work) or [None])[0],
                        "group_size": len(members), **decision,
                        "registry": {
                            "status": registry[doi].get("status"),
                            "type": registry[doi].get("type"),
                            "title": (registry[doi].get("titles") or [None])[0],
                        },
                    }
                    if decision["action"] == "removed":
                        fields, record["removed"] = self.removal(work, doi, now)
                        updates.append(UpdateOne(
                            {"_id": work["_id"], "doi": work.get("doi")}, {"$set": fields}))
                    records.append(UpdateOne(
                        {"_id": "{}:doi:{}".format(work["_id"], doi)},
                        {"$set": record, "$setOnInsert": {"time": now}}, upsert=True))
                changed = any(decision["action"] == "removed" for decision in decisions)
                progress.step("corregidos" if changed else "sin_cambios")
            if updates and not self.dry_run:
                works.bulk_write(updates, ordered=False)
            if records:
                audit.bulk_write(records, ordered=False)
        progress.done(**{key: value for key, value in counts.items() if ":" not in key})
        summary = {
            "task": "apply", "time": now, "dry_run": self.dry_run,
            "resolver": self.resolver.name,
            "doi": {"shared": len(groups), "works": sum(map(len, groups.values())),
                    "decisions": dict(counts)},
            "orcid": self.apply_orcids(audit, now),
        }
        self.db["identifier_disputes_runs"].insert_one(dict(summary))
        return summary

    def orcid_groups(self):
        """Return ``{orcid: [person ids]}`` for the iDs held by two or more people."""
        groups = defaultdict(list)
        cursor = self.db["person"].find({"external_ids.source": "orcid"}, {"external_ids": 1})
        for person in cursor:
            orcids = {normalize_orcid(item.get("id")) for item in person.get("external_ids") or []
                      if item.get("source") == "orcid"}
            for orcid in orcids:
                if orcid:
                    groups[orcid].append(person["_id"])
        return {orcid: ids for orcid, ids in groups.items() if len(ids) > 1}

    def apply_orcids(self, audit, now):
        """Leave each shared ORCID iD on the person whose name matches the public record."""
        people = self.db["person"]
        groups = self.orcid_groups()
        registry = {doc["_id"]: doc for doc in self.resolver["orcid"].find(
            {"_id": {"$in": sorted(groups)}, "status": {"$in": ["ok", "not_found"]}})}
        progress = ProgressLogger("post_identifier_disputes:apply:orcid",
                                  total=len(groups)).start("dry_run" if self.dry_run else "")
        counts, updates, records = Counter(), [], []
        for orcid in sorted(groups):
            if orcid not in registry:
                counts["pending"] += len(groups[orcid])
                progress.step("pendientes")
                continue
            found = {person["_id"]: person for person in people.find(
                {"_id": {"$in": groups[orcid]}},
                {"full_name": 1, "external_ids": 1, "updated": 1})}
            members = [found[person_id] for person_id in groups[orcid] if person_id in found]
            decisions = decide_orcid(registry[orcid], members)
            for person, decision in zip(members, decisions):
                counts[decision["action"]] += 1
                counts["{}:{}".format(decision["action"], decision["reason"])] += 1
                record = {
                    "kind": "orcid", "identifier": orcid, "person_id": person["_id"],
                    "person_name": person.get("full_name"), "group_size": len(members),
                    **decision,
                    "registry": {key: registry[orcid].get(key) for key in
                                 ("status", "given_names", "family_name", "credit_name")},
                }
                if decision["action"] == "removed":
                    external_ids = person.get("external_ids") or []
                    removed = [item for item in external_ids if item.get("source") == "orcid"
                               and normalize_orcid(item.get("id")) == orcid]
                    remaining = [item for item in external_ids if item not in removed]
                    updated = [item for item in person.get("updated") or []
                               if item.get("source") != SOURCE]
                    record["removed"] = {"external_ids": removed}
                    if not any(item.get("source") == "orcid" for item in remaining):
                        # No ORCID iD left: the profile no longer has ORCID as a source.
                        record["removed"]["updated"] = [
                            item for item in updated if item.get("source") == "orcid"]
                        updated = [item for item in updated if item.get("source") != "orcid"]
                    updated.append({"source": SOURCE, "time": now})
                    # The ORCID plugin may have enriched this person from the wrong record.
                    record["review"] = any(item.get("provenance") == "orcid" for item in removed)
                    updates.append(UpdateOne({"_id": person["_id"]}, {"$set": {
                        "external_ids": remaining,
                        "updated": updated,
                    }}))
                records.append(UpdateOne(
                    {"_id": "{}:orcid:{}".format(person["_id"], orcid)},
                    {"$set": record, "$setOnInsert": {"time": now}}, upsert=True))
            changed = any(decision["action"] == "removed" for decision in decisions)
            progress.step("corregidos" if changed else "sin_cambios")
        if updates and not self.dry_run:
            people.bulk_write(updates, ordered=False)
        if records:
            audit.bulk_write(records, ordered=False)
        progress.done(**{key: value for key, value in counts.items() if ":" not in key})
        return {"shared": len(groups), "people": sum(map(len, groups.values())),
                "decisions": dict(counts)}

    def run(self):
        if self.task == "collect":
            self.collect()
        else:
            self.apply()
        return 0
