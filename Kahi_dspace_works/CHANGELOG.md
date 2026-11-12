# Changelog
## 0.0.6-alpha - 2026-11-12
- Skip records answered by the OAI server of another configured repository
  (`OAI-PMH.request`): dspace_uac_records held a copy of uniatlantico and
  attributed it to UAC; counted as `foraneos`
- Re-processing a record already in the work (same dspace id) refreshes it
  without a second `updated` entry

## 0.0.5-alpha - 2026-11-12
- Keep author first_names/last_names/initials in the parsed entry until the
  work is stored: when another worker wins the DOI, the entry is merged with
  process_one_update, which failed with KeyError 'first_names'

## 0.0.4-alpha - 2026-11-11
- Register each repository in `sources` (type repository, dspace and ror ids,
  repository and OAI URLs, institution as publisher/relation); reuse the
  OpenAlex source given by `repository_sources` / `repository_sources_file`
  (collection key -> OpenAlex source id) instead of creating a duplicate
- Handle entry of external_urls: `source: "handle"` (was `dspace`) plus
  `source_ref` {id, provenance, name, types, url, affiliation}
- Require kahi_impactu_utils>=0.1.8

## 0.0.3-alpha - 2026-11-11
- Attach faculty and department units only when the work lists the parent
  institution for the author and the unit years cover the publication year
  (or the previous one); shared kahi_impactu_utils.Affiliations.get_units_affiliations
- Require kahi_impactu_utils>=0.1.7

## 0.0.2-alpha - 2026-11-10
- Normalize every hyphen and dash variant (not only ASCII) in author names before splitting; add test
- Require kahi_impactu_utils>=0.1.6

## 0.0.1-alpha - 2026-11-06

- Share DOI lookup, title guard and _kahi_work_identity coordination;
  use insert-only upserts
- Reject DOIs with unresolved repository templates or malformed percent
  escapes
- Read each collection's institution ROR from its *_identity document;
  repository_affiliations overrides it (README Univalle ROR fixed)
- Normalize publication years, keeping the earliest valid date

- Report START/progress/DONE with kahi ProgressLogger per task and
  collection, counting skipped records; joblib verbose=0
- Require kahi>=0.1.3 and declare kahi_impactu_utils
