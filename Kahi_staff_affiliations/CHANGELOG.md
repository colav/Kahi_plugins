# Changelog

## Unreleased - 2026-11-06

- Build faculty/department ids from institution and unit codes, with a
  normalized-name fingerprint when a code is missing; dedupe by id
- Find the institution with canonical_ror_url (fixes 'https: //ror.org')
- Add only non-empty unit codes as external ids; KahiBase updated order
- Add tests and README notes

- Report START/progress/DONE with kahi ProgressLogger per institution
  with unit counters
- Require kahi>=0.1.3a0
