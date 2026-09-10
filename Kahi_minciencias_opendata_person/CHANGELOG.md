# Changelog

## 0.1.5-beta - 2026-09-09

- Replace the legacy multi-collection reader and global aggregations with direct
  streaming from an explicit audited six-entity ScienTI person snapshot.
- Import only DOI-identified MinCiencias related works and resolve group
  affiliations against affiliations already present in Kahi.
- Record immutable source evidence and batch checkpoints for safe resumption.
- Resolve existing people and write replacements in bounded batches, rejecting
  ambiguous `COD_RH` mappings.
