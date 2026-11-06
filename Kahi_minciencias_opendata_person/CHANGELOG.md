# Changelog

## 0.1.6-beta - 2026-10-18

- Create MongoDB ObjectIds and match people by COD_RH before using a validated
  ORCID and compatible names. Never use the snapshot COD_RH as the MongoDB id.
- Skip and report snapshot records without a name.
- Add missing principal institutions from imported group relations, with
  unknown dates and empty years; preserve all existing institutional history.
- Cache group/institution lookups and combine updates to the same person within
  a batch. Preserve all COD_RH and their metadata when ORCID links profiles.
- Version the snapshot checkpoint policy independently from the source release.

- Add explicit `snapshot`/`legacy` selection. Legacy mode reads `cvlac_data`,
  `gruplac_production_data` and immutable raw CvLAC HTML, ignores employment
  experiences and retains only DOI-related works.

- Add the `clean_cvlac_experience_affiliations.py` maintenance script.
- Report standard START/progress/DONE lines with `kahi.progress.ProgressLogger`;
  joblib progress output disabled (`verbose=0`).
- Require `kahi>=0.1.3a0`; declare `joblib`, `beautifulsoup4` and
  `kahi_impactu_utils`.

## 0.1.5-beta - 2026-09-09

- Replace the legacy multi-collection reader and global aggregations with direct
  streaming from an explicit audited six-entity ScienTI person snapshot.
- Import only DOI-identified MinCiencias related works and resolve group
  affiliations against affiliations already present in Kahi.
- Record immutable source evidence and batch checkpoints for safe resumption.
- Resolve existing people and write replacements in bounded batches, rejecting
  ambiguous `COD_RH` mappings.
