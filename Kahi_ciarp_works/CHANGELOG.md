# Changelog

## Unreleased - 2026-11-06

- Add work affiliations only when the author's dated parent affiliation
  covers the publication year or the previous one; missing start date
  adds nothing
- Select faculties/departments from the person's dated affiliations,
  never from CIARP product codes; never remove affiliations from other
  sources
- Resolve authors without treating an empty name as an identifier
- Share DOI lookup (works.doi/external_ids), title guard and
  _kahi_work_identity coordination; use insert-only upserts
- Fill missing year, DOI, date and source without overwriting; normalize
  publication years
- Build ROR URLs with canonical_ror_url (fixes 'https: //ror.org')
- Add backfill_ciarp_author_affiliations.py audit/backfill script,
  README contracts and tests

- Report START/progress/DONE with kahi ProgressLogger per institution
- Require kahi>=0.1.3a0
