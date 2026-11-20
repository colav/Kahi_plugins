## 0.1.13 - 2026-11-14

* Snapshot mode follows the convention of the other works plugins:
  `minciencias_opendata_works/doi` is the DOI stage and
  `minciencias_opendata_works` (no task) is the stage for works without a
  DOI. `/nodoi` remains accepted as the former name of that stage, with the
  same checkpoint. **Behaviour change:** no task used to import both
  partitions in one pass; that is now the explicit task `all`

## 0.1.12 - 2026-11-14

* Book/chapter veto reads only the most specific type level per provenance:
  a Scienti chapter (level 2 chapter under the level 1 "Libro" category) is a
  chapter, not book+chapter (policy book-chapter-veto-v2)
* Similarity: disjoint person ids fall back to the strict author-name match
  instead of rejecting (one researcher with several person records)
* `updated` of imported works is only `minciencias`; Yuku's
  `minciencias_materialization` marker is dropped
* On the November cut, 22 023 of 33 722 unmerged same-title/year pairs pass
  the compatibility check (15 456 by the veto fix)

## 0.1.11 - 2026-11-13

* Size the Mohan Elasticsearch connection pool to `workers`
  (kahi_impactu_utils>=0.1.11)

## 0.1.10 - 2026-11-13

* `workers` (default 1): the rows of each snapshot batch are imported by a
  thread pool; per-row counters are merged under a lock and the Elasticsearch
  sync cache is locked. Identity leases already serialize rows that share a
  DOI, identifier or title (real-Mongo test: 8 same-title rows insert once)

## 0.1.9 - 2026-11-12

* Detect the language of snapshot titles that come without `lang` (Yuku does
  not set it; 302 k titles had `lang: null`)
* `updated.time` is the Kahi import time, not the Yuku parsing time, and a
  merge keeps one `updated` entry per source (refreshing its time)

## 0.1.8 - 2026-11-12

* Accept the Yuku `type_audit` field as a source-only field: it is discarded
  on import and never copied to `works`
* Resolve the identified (filtered) works collection from the `current`
  pointer of `scienti_identified_work_publications` when `collection_name` is
  not configured; `identified: false` forces the full works graph
* Non-DOI routes (identifiers, fingerprints, Elasticsearch similarity) use the
  strict title comparison of kahi_impactu_utils 0.1.9; DOI routes accept
  nested, similar or translated titles
* Require kahi_impactu_utils>=0.1.9

## 0.1.7 - 2026-11-11

* Attach faculty and department units only when the work lists the parent
  institution for the author and the unit years cover the publication year
  (or the previous one); shared kahi_impactu_utils.Affiliations.get_units_affiliations
* Require kahi_impactu_utils>=0.1.7

## 0.1.6 - 2026-11-10

* Normalize every hyphen and dash variant in unresolved authors[].full_name from the snapshot to spaces
* Require kahi_impactu_utils>=0.1.6

## 0.1.5

* Prevent known books and book chapters from merging in non-DOI identifier, exact-title and Elasticsearch routes, even with shared ISBN/title/authors/year.
* Recognize exact canonical/native type labels from the installed ImpactU catalog, preserving the distinction between a chapter and a whole book organized into chapters.
* Refuse missing/conflicting book-family evidence against a known book/chapter; retain normal enrichment between the same kind. Bind the rule and catalog alias digest to non-DOI checkpoints.
* No work schema changes, source rewrites or changes to the DOI ingestion branch.

* Add dedup plan/merge, work-id migration/repair and publication-metadata
  backfill scripts with tests.
* Report standard START/progress/DONE lines with `kahi.progress.ProgressLogger`;
  joblib progress output disabled (`verbose=0`).
* Require `kahi>=0.1.3` and declare runtime dependencies.

## 0.1.4

* Add optional Elasticsearch/Mohan similarity to snapshot task=nodoi, after scoped identifiers and exact title candidates.
* Validate candidates against current MongoDB titles, authors, publication year and bibliography; report ambiguous and stale hits.
* Keep Elasticsearch synchronized after inserts/merges and fail the batch on search/index errors so retries repair it without duplicate MongoDB works.
* Coordinate exact title decisions outside works and bind similarity policy/index to separate checkpoints. DOI mode remains independent of Elasticsearch.

## 0.1.3

* Consume independently audited identified-work snapshots tied to the parent six-entity release.
* Split snapshot ingestion into `doi` and `nodoi`, including DOI stored in external_ids, with separate checkpoints and batch reads of selected metadata.
* Fill sparse model fields with KahiBase defaults, preserving field order and adding no new fields to works.
* Match non-DOI products by scoped identifiers or ISBN plus compatible titles and years; protect distinct chapters/authors, report ambiguity and coordinate across processes outside works.

# Changelog

- Restore the isolated `legacy` reader for `gruplac_production_data`; snapshot
  ingestion remains unchanged and person-related-work promotion stays off.

## 0.1.2 - 2026-09-09

- Replace the legacy GrupLAC parser and Elasticsearch path with bounded,
  resumable ingestion from an explicit audited six-entity works snapshot.
- Remove `person_related_works`; resolve authors and groups against Kahi without
  name-only person matching or source-database mutations.
- Preserve existing non-empty metadata and reuse `bibliographic_info` exactly as
  enriched by the Yuku snapshot.
