# Changelog

## 0.1.5 - 2026-09-09

### Added

- Audited ingestion from Yuku's atomic six-entity ScienTI release.
- Direct reuse of the Kahi affiliation schema and deterministic run summary.
- Batch checkpoints and immutable import-run evidence for safe resumption.

### Changed

- Source mode must be explicit; `snapshot` is the production path and
  `legacy_open_data` preserves the former reader.
- Endorsing institutions from snapshot relations are resolved against existing
  affiliations before the strict `IUA` fallback is considered.
- Snapshot mode treats DAM as read-only and creates no source indexes.

## 0.1.4 - 2026-09-03

### Added

- Deterministic fallback affiliations for unresolved endorsing institutions.
- Missing names for known DAM research groups.

### Changed

- Institution matching now prioritizes catalog identifiers, exact names,
  geography, and strict unambiguous similarity.
- Cataloged institutions replace obsolete synthetic relations when identified.
- Existing groups are enriched with endorsing institution relations and
  addresses on reruns.

### Fixed

- Institution aliases for ITM and other known organization name variants.
- Duplicate group addresses and unsafe shallow copies during processing.
