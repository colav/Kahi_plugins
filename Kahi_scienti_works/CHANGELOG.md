# Changelog

## 0.1.11 - 2026-11-14

- Directed works (theses): the advisors (`TPO_PARTICIPACION` TUT, ASE, COT) and
  the student (ORI) of `re_author_others` are authors of the work with type
  `advisor` / `author`, also when the work already exists. A thesis inserted
  first by OpenAlex lists only the student, so the CvLAC owner matched no
  author and the advisor was lost (19 % of a 4 000 sample, 6 % in September)
- Updating a work keeps the author `type` instead of rebuilding the author
  without it
- A second CvLAC of a work already updated by ScienTI adds its owner as advisor

## 0.1.10 - 2026-11-13

- Size the Mohan Elasticsearch connection pool to `es_max_concurrency`
  (EmptyPoolError with more than 10 concurrent searches)

## 0.1.9 - 2026-11-11
- Attach faculty and department units only when the work lists the parent
  institution for the author and the unit years cover the publication year
  (or the previous one); shared kahi_impactu_utils.Affiliations.get_units_affiliations
- Require kahi_impactu_utils>=0.1.7

## 0.1.8 - 2026-11-10
- Normalize every hyphen and dash variant in authors[].full_name (TXT_TOTAL_NAMES) to spaces
- Require kahi_impactu_utils>=0.1.6

## 0.1.7 - 2026-11-06

- Share DOI lookup, title guard and _kahi_work_identity coordination;
  use insert-only upserts
- Keep the first group's institution only for the identified source
  author on updates and retries
- Fill missing year, DOI, date and source without overwriting; normalize
  publication years
- Update README and tests

- Report START/progress/DONE with kahi ProgressLogger per phase; joblib
  verbose=0
- Require kahi>=0.1.3
