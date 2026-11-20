# Changelog
## 0.1.8 - 2026-11-07
- Build names with scienti_author_names: dots removed, every hyphen or dash variant turned into a space, dashed raw name kept as alias; add tests
- Require kahi_impactu_utils>=0.1.6

## 0.1.7 - 2026-11-06

- Normalize COD_ORCID with parse_orcid_id_from_url
- Store -1 when the ScienTI birthdate is empty or invalid; add tests

- Report START/progress/DONE with kahi ProgressLogger per database
  phase; optional progress_interval, silent with verbose 0
- Require kahi>=0.1.3
