# Changelog
## 0.1.8 - 2026-11-10
- Fall back to the optional nombre_completo column as full_name (without splitting it) when
  nombres, primer_apellido and segundo_apellido are empty or absent; name parts stay empty
- Skip records without any name instead of inserting nameless people (omitidos_sin_nombre);
  a nameless record still enriches a person already known by another source
- Never add an empty alias
- Name columns are optional in staff files too, as they already were when reading the collection

## 0.1.7 - 2026-11-07
- Normalize every hyphen and dash variant in names and last names to spaces; keep the dashed name as an alias
- Require kahi_impactu_utils>=0.1.6

## 0.1.6 - 2026-11-06

- Merge staff evidence into people inserted by other sources instead of
  skipping them; add repair_missing_parent_affiliations.py
- Resolve faculties/departments by deterministic staff_affiliations ids,
  type and parent relation; unit columns are optional
- Store actual covered years and date bounds for every affiliation
  without filling gaps
- Add staff ORCID and CvLAC COD_RH as external ids
- Add task ciarp_related_works to backfill CIARP DOI references
- Build ROR URLs with canonical_ror_url; add tests

- Report START/progress/DONE with kahi ProgressLogger
  (nuevos/actualizados)
- Require kahi>=0.1.3
