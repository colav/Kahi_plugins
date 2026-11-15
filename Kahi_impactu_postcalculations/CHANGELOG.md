# Changelog

## 0.1.11 - 2026-11-15

### Added

- Colombian addresses are mapped to DIVIPOLA (DANE, 1 122 municipalities,
  packaged in `data/divipola_municipios.json`) right after the geographic
  normalization and under its switches: `city` gets the common name ("Cali",
  "Bogotá", "Cartagena", "Cúcuta"; the official name otherwise), `state` the
  department, plus the new `city_code` and `state_code`; empty `lat`/`lng` take
  the coordinates of the municipality. A department that contradicts a
  municipality with a unique name is corrected; "Armenia" without department is
  Armenia, Quindío; other ambiguous names are left and reported.
- Research groups keep only Colombian addresses, one per city: foreign
  addresses inherited from related institutions are removed, and an address
  without a real country (empty, or the misassigned "Afganistán") whose city is
  a Colombian municipality becomes Colombian.

### Changed

- The city alias `Cartagena -> Cartagena de Indias` is gone: the common name is
  kept.

## 0.1.10 - 2026-11-15

### Added

- Stage `normalize_affiliation_names` (before denormalization, switch
  `affiliation_names_enabled`, default true): the affiliation name copied in
  `affiliations.relations`, `person.affiliations` and the author affiliations
  of works, patents, projects and events becomes the Spanish name of the
  affiliation: ROR `es`, else SNIES `es`, else MinCiencias or staff `es` for
  institutions. Wikidata and ScienTI names are not used; groups, departments
  and faculties keep their names; with no Spanish name the copy is left as it
  is (colav/impactu#706).

## 0.1.9 - 2026-11-14

### Added

- Person stage `set_person_age`, last of the person stages: `age` is the
  completed years at the run date (`None` outside 15-100 years or when the
  value is not a date) and `birthdate` is cleared to `-1`, so the published
  profile carries the age but not the birth date. Requires Kahi 0.1.6.
- Tests of the h-index, of the h5 window (the five complete years before the
  run) and of the age calculation.

## 0.1.8 - 2026-11-14

### Added

- Tests that fail if a denormalization stage rebuilds `authors` without the
  original author fields, or if author reconciliation drops the `type`
  (advisor, author) of thesis authors. No behaviour change.

## 0.1.7 - 2026-11-14

### Added

- Topic inference for other collections: `topic_collections` (default
  `[works]`, e.g. `[works, patents]`) and `topic_abstract_langs` (preferred
  abstract languages per collection, applied in memory). Person topics keep
  using works only.

### Changed

- Topic inference moved to `process_topics(db, openalex_db)`.

## 0.1.6 - 2026-11-12

### Changed

- Remove the `_id_migration_complete` flag (and its index) from `person` once
  every persistent id migration is complete; `_id_old` is kept.

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
