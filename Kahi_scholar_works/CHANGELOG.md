# Changelog

## 0.1.8 - 2026-11-13

- Size the Mohan Elasticsearch connection pool to `es_max_concurrency`
  (EmptyPoolError with more than 10 concurrent searches)

## 0.1.7 - 2026-11-10
- Normalize every hyphen and dash variant in authors[].full_name to spaces; add test
- Require kahi_impactu_utils>=0.1.6

## 0.1.6 - 2026-11-06

- Share DOI lookup, title guard and _kahi_work_identity coordination;
  use insert-only upserts
- Fill missing year, DOI, date and source without overwriting; normalize
  publication years
- Update README and tests

- Report START/progress/DONE with kahi ProgressLogger; joblib verbose=0
- Require kahi>=0.1.3
