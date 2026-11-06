# Changelog

## Unreleased - 2026-11-06

- Share DOI lookup, title guard and _kahi_work_identity coordination;
  use insert-only upserts
- Reject DOIs with unresolved repository templates or malformed percent
  escapes
- Read each collection's institution ROR from its *_identity document;
  repository_affiliations overrides it (README Univalle ROR fixed)
- Normalize publication years, keeping the earliest valid date

- Report START/progress/DONE with kahi ProgressLogger per task and
  collection, counting skipped records; joblib verbose=0
- Require kahi>=0.1.3a0 and declare kahi_impactu_utils
