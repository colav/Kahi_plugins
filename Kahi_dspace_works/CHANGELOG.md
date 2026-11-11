# Changelog
## 0.0.3-alpha - 2026-11-11
- Attach faculty and department units only when the work lists the parent
  institution for the author and the unit years cover the publication year
  (or the previous one); shared kahi_impactu_utils.Affiliations.get_units_affiliations
- Require kahi_impactu_utils>=0.1.7

## 0.0.2-alpha - 2026-11-10
- Normalize every hyphen and dash variant (not only ASCII) in author names before splitting; add test
- Require kahi_impactu_utils>=0.1.6

## 0.0.1-alpha - 2026-11-06

- Share DOI lookup, title guard and _kahi_work_identity coordination;
  use insert-only upserts
- Reject DOIs with unresolved repository templates or malformed percent
  escapes
- Read each collection's institution ROR from its *_identity document;
  repository_affiliations overrides it (README Univalle ROR fixed)
- Normalize publication years, keeping the earliest valid date

- Report START/progress/DONE with kahi ProgressLogger per task and
  collection, counting skipped records; joblib verbose=0
- Require kahi>=0.1.3 and declare kahi_impactu_utils
