"""Enrich Kahi patents with the Lens ID and the Lens abstracts.

Only patents already in Kahi are touched; Lens-only patents are never
inserted. For every matched patent the plugin keeps exactly one Lens entry in
``external_ids`` and ``external_urls``, the Lens abstracts (one per language,
inverted index as in works) in ``abstracts`` and one ``updated`` entry with
source ``lens``. Entries from other sources are left as they are. A patent
that no longer matches loses its Lens entries, so a re-run converges to the
current matches; an unchanged patent is not rewritten.

Every decision is written to the audit collection (``lens_patents_matches``
by default), one document per patent, including the ones without a match.

With ``merge_duplicates`` the patents that matched the same Lens record and
are proven to be the same patent (see ``merging.clusters``) become one: the
survivor absorbs authors, groups, identifiers, titles and types of the others,
which are deleted and kept whole in ``lens_patents_merges`` so the merge can be
undone. ``merge_dry_run`` only writes ``lens_patents_merges_dry_run``.
"""

import re
from collections import Counter, defaultdict
from time import time

from kahi.KahiBase import KahiBase
from kahi.progress import ProgressLogger
from kahi_impactu_utils.String import text_to_inverted_index
from pymongo import DeleteOne, MongoClient, UpdateOne

from kahi_lens_patents.matching import LensIndex, country_code, match_patent
from kahi_lens_patents.merging import choose_survivor, clusters, merge_documents

LENS_URL = "https://www.lens.org/lens/patent/{}"
PATENT_FILTER = {"types": {"$elemMatch": {"source": "impactu", "type": "Patente"}}}
MARKUP_RE = re.compile(
    r"</?(?:p|br|sub|sup|b|i|u|image|img|emi|chem|maths?|figref|tables?)\b[^>]*>",
    re.IGNORECASE,
)


def clean_abstract(text):
    """Drop the publication markup Lens keeps in some abstracts."""
    return " ".join(MARKUP_RE.sub(" ", str(text or "")).split())


def lens_abstracts(record):
    """Lens abstracts as Kahi entries, one per language."""
    entries, languages = [], set()
    for item in record.get("abstract") or []:
        text = clean_abstract(item.get("text"))
        lang = str(item.get("lang") or "").lower() or None
        if not text or lang in languages:
            continue
        languages.add(lang)
        entries.append({
            "abstract": text_to_inverted_index(text),
            "lang": lang,
            "source": "lens",
            "provenance": "lens",
        })
    return entries


def patent_evidence(patent, snapshot):
    """Matching input from the Kahi patent and its snapshot record."""
    numbers, countries, years = [], set(), []
    occurrences = ((snapshot or {}).get("source_metadata") or {}).get("occurrences") or []
    for occurrence in occurrences:
        metadata = occurrence.get("metadata") or {}
        code = country_code(metadata.get("country"))
        if code:
            countries.add(code)
        if metadata.get("registration_number"):
            numbers.append((metadata["registration_number"], code))
        year = metadata.get("year")
        if isinstance(year, int) and 1900 < year < 2100:
            years.append(year)
    return {
        "titles": [item.get("title") for item in patent.get("titles") or [] if item.get("title")],
        "numbers": numbers,
        "countries": countries,
        "year": min(years) if years else None,
        "authors": [author.get("full_name") for author in patent.get("authors") or []],
    }


def without_lens(items):
    return [item for item in items or [] if item.get("source") != "lens"]


def desired_fields(patent, lens_id, record, now):
    """Return the four arrays with the Lens entries of ``lens_id`` (or none)."""
    external_ids = without_lens(patent.get("external_ids"))
    external_urls = without_lens(patent.get("external_urls"))
    abstracts = without_lens(patent.get("abstracts"))
    updated = without_lens(patent.get("updated"))
    if lens_id:
        external_ids.append({"provenance": "lens", "source": "lens", "id": lens_id})
        external_urls.append(
            {"provenance": "lens", "source": "lens", "url": LENS_URL.format(lens_id)}
        )
        abstracts.extend(lens_abstracts(record))
    fields = {
        "external_ids": external_ids,
        "external_urls": external_urls,
        "abstracts": abstracts,
    }
    current = {field: patent.get(field) or [] for field in fields}
    if fields == current:
        return None
    if lens_id:
        updated.append({"source": "lens", "time": now})
    fields["updated"] = updated
    return fields


class Kahi_lens_patents(KahiBase):
    """Add the Lens ID, URL and abstracts to the patents matched in Lens."""

    config = {}

    def __init__(self, config):
        self.config = config
        plugin = config["lens_patents"]
        self.client = MongoClient(config["database_url"])
        self.db = self.client[config["database_name"]]
        self.collection = self.db["patents"]
        self.audit = self.db[plugin.get("audit_collection", "lens_patents_matches")]
        self.runs = self.db[plugin.get("runs_collection", "minciencias_opendata_patents_runs")]

        self.lens_client = MongoClient(plugin["database_url"])
        if plugin["database_name"] not in self.lens_client.list_database_names():
            raise RuntimeError(f"Database {plugin['database_name']} not found")
        lens_db = self.lens_client[plugin["database_name"]]
        collection_name = plugin.get("collection_name", "patents")
        if collection_name not in lens_db.list_collection_names():
            raise RuntimeError(f"Collection {plugin['database_name']}.{collection_name} not found")
        self.lens_collection = lens_db[collection_name]

        self.snapshot_client = MongoClient(
            plugin.get("snapshot_database_url", plugin["database_url"])
        )
        self.snapshot_db = self.snapshot_client[plugin.get("snapshot_database_name", "dam")]
        self.merge_duplicates = bool(plugin.get("merge_duplicates", False))
        self.merge_dry_run = bool(plugin.get("merge_dry_run", False))
        self.merges_name = plugin.get("merges_collection", "lens_patents_merges")
        self.verbose = plugin.get("verbose", 0)

    def snapshot_collection(self):
        """Snapshot collection of the last complete minciencias_opendata_patents run."""
        run = self.runs.find_one(
            {"status": "complete", "source_collection": {"$exists": True}},
            sort=[("finished_at", -1), ("started_at", -1)],
        )
        if not run:
            raise RuntimeError(
                "minciencias_opendata_patents has no complete snapshot run; run it first"
            )
        name = run["source_collection"]
        if name not in self.snapshot_db.list_collection_names():
            raise RuntimeError(f"Snapshot collection {name} not found")
        return self.snapshot_db[name]

    def run(self):
        now = int(time())
        snapshot = self.snapshot_collection()
        records = {doc["_id"]: doc["lens"] for doc in self.lens_collection.find({}, {"lens": 1})}
        index = LensIndex(records.values())

        projection = {
            "titles": 1, "authors.full_name": 1, "external_ids": 1,
            "external_urls": 1, "abstracts": 1, "updated": 1,
        }
        patents = list(self.collection.find(PATENT_FILTER, projection))
        sources = {
            doc["_id"]: doc
            for doc in snapshot.find(
                {"_id": {"$in": [patent["_id"] for patent in patents]}},
                {"source_metadata.occurrences.metadata": 1, "authors.id": 1},
            )
        }
        progress = ProgressLogger("lens_patents:match", total=len(patents)).start(
            f"lens={len(records)} snapshot={snapshot.name}"
        )
        operations, audit, outcomes = [], [], Counter()
        for patent in patents:
            result = match_patent(index, patent_evidence(patent, sources.get(patent["_id"])))
            lens_id = result.get("lens_id")
            record = records.get(lens_id) if lens_id else None
            audit.append({
                "_id": patent["_id"],
                **result,
                "jurisdiction": record.get("jurisdiction") if record else None,
                "doc_number": record.get("doc_number") if record else None,
                "has_abstract": bool(record and record.get("abstract")),
                "in_snapshot": patent["_id"] in sources,
                "run_at": now,
            })
            fields = desired_fields(patent, lens_id, record, now)
            if fields is None:
                outcome = "existentes" if lens_id else "omitidos"
            else:
                outcome = "actualizados" if lens_id else "retirados"
                operations.append(UpdateOne({"_id": patent["_id"]}, {"$set": fields}))
            outcomes[result.get("matched_by") or result.get("reason")] += 1
            progress.step(outcome)
        if operations:
            self.collection.bulk_write(operations, ordered=False)
        self.audit.delete_many({})
        if audit:
            self.audit.insert_many(audit)
        matched = [doc for doc in audit if doc.get("lens_id")]
        progress.done(
            **dict(outcomes),
            con_abstract=sum(1 for doc in matched if doc["has_abstract"]),
            lens_distintos=len({doc["lens_id"] for doc in matched}),
        )
        if self.merge_duplicates or self.merge_dry_run:
            self.merge(matched, sources, index, now)
        return 0

    def merge(self, matched, sources, index, now):
        """Merge the patents of each Lens record that are proven to be one patent."""
        dry_run = self.merge_dry_run
        target = self.db[self.merges_name + "_dry_run" if dry_run else self.merges_name]
        if dry_run:
            target.delete_many({})
        by_lens = defaultdict(list)
        for doc in matched:
            by_lens[doc["lens_id"]].append(doc)
        shared = {lens_id: docs for lens_id, docs in by_lens.items() if len(docs) > 1}
        progress = ProgressLogger("lens_patents:merge", total=len(shared)).start(
            "dry_run" if dry_run else ""
        )
        updates, deletes, records, removed_total = [], [], [], 0
        for lens_id, docs in sorted(shared.items()):
            full = {
                patent["_id"]: patent
                for patent in self.collection.find({"_id": {"$in": [doc["_id"] for doc in docs]}})
            }
            members = []
            for doc in docs:
                patent = full.get(doc["_id"])
                if not patent:
                    continue
                snapshot = sources.get(doc["_id"]) or {}
                occurrences = (snapshot.get("source_metadata") or {}).get("occurrences") or []
                members.append({
                    "_id": doc["_id"],
                    "matched_by": doc.get("matched_by"),
                    "names": [author.get("full_name") for author in patent.get("authors") or []
                              if author.get("full_name")],
                    "author_ids": [str(author["id"]) for author in snapshot.get("authors") or []
                                   if author.get("id")],
                    "owner_ids": [str(owner) for occurrence in occurrences
                                  for owner in (occurrence.get("metadata") or {}).get("owner_ids")
                                  or []],
                })
            found = clusters(members, index.records[lens_id]["inventor_names"])
            for cluster in found:
                survivor = choose_survivor(
                    [member for member in members if member["_id"] in cluster["ids"]]
                )
                others = [full[item] for item in cluster["ids"] if item != survivor]
                updates.append(UpdateOne(
                    {"_id": survivor}, {"$set": merge_documents(full[survivor], others)}
                ))
                for other in others:
                    deletes.append(DeleteOne({"_id": other["_id"]}))
                    records.append(UpdateOne({"_id": other["_id"]}, {"$set": {
                        "merged_into": survivor,
                        "lens_id": lens_id,
                        "evidence": cluster["evidence"][other["_id"]],
                        "survivor_evidence": cluster["evidence"][survivor],
                        "document": other,
                        "run_at": now,
                    }}, upsert=True))
                removed_total += len(others)
            progress.step("grupos_unidos" if found else "grupos_sin_evidencia")
        if records:
            target.bulk_write(records, ordered=False)
        if updates and not dry_run:
            self.collection.bulk_write(updates, ordered=False)
            self.collection.bulk_write(deletes, ordered=False)
            self.audit.bulk_write([
                UpdateOne({"_id": record._filter["_id"]},
                          {"$set": {"merged_into": record._doc["$set"]["merged_into"]}})
                for record in records
            ], ordered=False)
        progress.done(eliminadas=removed_total, sobrevivientes=len(updates))
        return removed_total
