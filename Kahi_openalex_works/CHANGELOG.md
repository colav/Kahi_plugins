# Changelog

## Unreleased - 2026-11-06

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
- Require kahi>=0.1.3a0
