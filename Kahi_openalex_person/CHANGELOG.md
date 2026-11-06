# Changelog

## Unreleased - 2026-11-06

- Import only last_known_institutions and merge OpenAlex affiliation
  years as a sorted union, never erasing existing years or dates
- Resolve people by unique OpenAlex id or valid ORCID; log
  identity_conflict warnings and skip ambiguous authors or institutions
- Use one pipeline update per person and batch institution prefetch with
  a bounded cache (batch_size); requires MongoDB 5.2+
- Let skip_previously_processed still enrich years; load related works
  only when needed
- Canonicalize OpenAlex/ORCID ids, skip implausible names and fail
  loudly on name split errors
- Fix README title/config key; add tests

- Report START/progress/DONE with kahi ProgressLogger; joblib verbose=0
- Require kahi>=0.1.3a0
