# Changelog
## 0.0.2-alpha - 2026-11-07
- When several people carry the same ORCID, keep it only on those whose name is
  compatible with the ORCID record (orcid_name_compatible); remove the identifier, the
  aliases copied from the record and the orcid updated mark from the others, and log
  every decision in orcid_person_shared_orcids
- Mark the ORCID with provenance orcid when updating an existing person
- Normalize every hyphen and dash variant in given and family names with normalize_dashes; keep the dashed name as an alias
- Require kahi_impactu_utils>=0.1.6

## 0.0.1-alpha - 2026-11-06

- Normalize ORCID URIs with parse_orcid_id_from_url

- Report START/progress/DONE with kahi ProgressLogger
  (nuevos/actualizados/omitidos); joblib verbose=0; per-record logs only
  with verbose > 4
- Require kahi>=0.1.3
