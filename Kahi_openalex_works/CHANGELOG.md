# Changelog
## 0.1.11 - 2026-11-10
- Store OpenAlex referenced_works in `references` (one entry per cited work
  id, provenance openalex) with `references_count`, on insert and on update
- Store `cited_by_api_url` rebuilt from the work id (absent from the snapshot)
- Require kahi>=0.1.4

## 0.1.10 - 2026-11-10
- Normalize every hyphen and dash variant in authors[].full_name (display_name) to spaces
- Require kahi_impactu_utils>=0.1.6

## 0.1.9 - 2026-11-06

- Share DOI lookup, title guard and _kahi_work_identity coordination;
  use insert-only upserts
- On reruns fill missing year, DOI, date and source and merge abstracts
  and topics without overwriting
- Build canonical ORCID URLs (fixes 'https: //orcid.org'); tolerate
  missing topics; normalize publication years
- Use generator output only with the threading backend (unsupported by
  multiprocessing)

- Report START/progress/DONE with kahi ProgressLogger, counting
  multiprocessing completions in the parent; joblib verbose=0
- Require kahi>=0.1.3
