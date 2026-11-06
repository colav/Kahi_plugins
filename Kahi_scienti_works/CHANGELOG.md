# Changelog

## Unreleased - 2026-11-06

- Share DOI lookup, title guard and _kahi_work_identity coordination;
  use insert-only upserts
- Keep the first group's institution only for the identified source
  author on updates and retries
- Fill missing year, DOI, date and source without overwriting; normalize
  publication years
- Update README and tests

- Report START/progress/DONE with kahi ProgressLogger per phase; joblib
  verbose=0
- Require kahi>=0.1.3a0
