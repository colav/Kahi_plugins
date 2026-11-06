# Changelog

## Unreleased - 2026-10-16

- Treat GrupLAC's trailing endorsement marker as status, not name: exclude
  declarations marked `- (No Avalado)` from group relations (a relation means
  endorsement and drives work attribution) and strip `- (Avalado)`. Exclusions
  are audited as `non_endorsing_institution` and counted in the run summary
  (`relations_excluded_non_endorsing`); no provisional `IUA_` institution is
  created for them. Prior group links (e.g. from ScienTI, which does not record
  endorsement) to institutions a group declares non-endorsing are removed unless
  the same group also declares them endorsing (`non_endorsing_prior_links_removed`).
- Exclude the observed country-only `COLOMBIA` fragment from institution
  resolution, audit its exclusion and remove only proven provisional links.
- Explain provisional punctuation/spacing variants through unique catalog
  identities corroborated by identifiers and each group's source declaration.
- Repair catalog-verified enriched compound IUA labels, retaining institution
  identities and reconciling group links against each group's own declarations.
- Persist repair evidence before writes and reuse it across retries/code updates;
  stop for conflicting codes, identifiers or mixed atomic institutional names.
- Validate source-backed group corrections and explain strict canonical fallback
  replacements in the migration audit instead of requiring identical legacy IDs.
- Consume the audited entity-specific affiliation current independently of
  six-entity releases, pinning its immutable publication for the whole import.
- Automatically invalidate checkpoints for source, importer code and catalog
  changes; delegate Kahi's workflow-log skip decision to snapshot checkpoints.
- Remove the need for release references, manual revisions and extra workflows
  in normal affiliation imports; keep explicit releases for historical audits.
- Split institutional declarations on `;` and `|` before identity resolution;
  preserve punctuation within parenthetical campus labels.
- Reject compound declarations and candidate names in approximate matching.
- Repair group references to proven compound IUA fallback documents only when
  all individual components resolve, preserving entity schema/order and campuses.
- Bump the resolver to v3 and the import revision to
  `institutions-v3-split-lists`, allowing reimport of existing snapshots.
- Audit remaining compound IUA group links as a migration failure.

- Report standard START/progress/DONE lines with `kahi.progress.ProgressLogger`;
  joblib progress output disabled (`verbose=0`).
- Require `kahi>=0.1.3a0`.

## Unreleased - 2026-10-12

- Share canonical institution relation handling between legacy and snapshot,
  retaining authoritative campus identities and replacing only proven obsolete
  campus or provisional links.
- Keep KahiBase entity order, preserve existing SNIES campuses and store all
  new resolution metadata exclusively in target control collections.
- Version snapshot checkpoints with resolver/import revisions and deterministic
  catalog fingerprints; audit unique institutional materialization across retries.
- Add an isolated, detached migration audit and guarded workflow activation.

- Resolve group avaladoras against the configured Scienti catalogs on demand.
- Preserve exact `COD_INST` ids and add canonical parents for campuses/units.
- Use normalized deterministic `IUA_` ids only after strict resolution fails.
- Resolve parenthetical campus/seccional labels to their declared canonical
  principal before fuzzy matching, without collapsing clinics or other units.
- Serialize on-demand Scienti materialization and keep one `COD_INST` per
  document; NIT matching is allowed only when the NIT is valid and unique.

- Accept `source_mode: legacy` for `gruplac_groups_data` while preserving the
  audited `snapshot` path.

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
