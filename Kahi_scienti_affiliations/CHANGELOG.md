# Changelog

## 0.1.6 - 2026-10-18

- Store valid group formation years in `year_established` instead of `birthdate`.
- Fill missing group years while preserving existing years; select the matching
  source group when a product contains multiple groups.
- Keep `campuses` last when inserting affiliations.

- Use the exact `COD_INST` for source-only institution ids.
- Keep the Scienti institution catalog read-only and materialize only group avaladoras.
- Add both the selected unit/campus and its canonical parent to group relations.

- Require distinctive terms in institution name matching and repair fused
  Scienti codes.
- Report per-phase progress on `kahi.progress.ProgressLogger`, keeping
  `progress_interval`/`progress_seconds` (default: 5% of the total, minimum
  1000) and silence with verbose 0.
- Require `kahi>=0.1.3`.

## 0.1.5 - 2026-09-03

### Added

- Consolidated ingestion of root Scienti institution catalogs before groups.
- Institution enrichment through existing affiliation fields for names,
  abbreviations, identifiers, addresses, establishment year, URLs, and update
  timestamps.
- Parent institution relations from `COD_INST_MACRO` and identifier-first group
  affiliation resolution.

### Changed

- Groups inherit addresses from their resolved parent institutions.
- The Scienti ranking code `00` is exposed as `Reconocido`.
