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
