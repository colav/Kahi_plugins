# Changelog

## 0.1.5 - 2026-11-06

### Added

- Enrich DOI-bearing works without abstracts from the local abstract resolver,
  requiring a fuzzy title match and storing only the inverted-index abstract.
- Record query, matching, rejection and extraction totals for every abstract
  enrichment execution in a dedicated resolver audit collection.
- Normalize affiliation country, Colombian department and curated city names
  to Spanish using CLDR and a bundled, versioned alias catalog.
- Support audit-only geographic normalization and report unresolved values.
- Add a fail-fast preflight for every enabled post-calculation phase, including
  a real topic-model inference validated against the OpenAlex topic catalog.
- Include works whose `primary_topic` is `null` in topic inference.
- Recover missing OpenAlex abstracts and native topics before inference.
- Allow resumed runs to skip completed persistent-ID and type phases.
- Distribute topic batches across isolated model endpoints with one exclusive
  sequential worker per endpoint.
- Recursively split failed topic batches, isolate irrecoverable works and keep
  processing healthy records.
- Abort safely on remote endpoint/network failures instead of treating them as
  invalid works; expose local/remote inference mode in workflow configuration.
- Aggregate up to five work topics for people identified by `COD_RH`, ORCID or
  a typed identity document, prioritizing repeated topics and using
  high-confidence single-work topics to fill remaining slots. The phase has
  auditable staging, dry-run support, parallel batches and idempotent
  application to `person.primary_topic` and `person.topics`.

- Refuse persistent-id migration from empty ids; add a repair script for
  affected migrations and an optional blank-id author reconciliation phase.
- Classify CvLAC/GrupLAC snapshot types through unique ImpactU catalog aliases.
- Add a targeted works denormalization script.

## 0.1.4 - 2026-09-09

### Added

- Populate each person's OpenAlex citation count during denormalization.

### Changed

- Load work type mappings from the versioned `Kahi_impactu_type_catalog`
  package instead of maintaining a private workbook copy.
