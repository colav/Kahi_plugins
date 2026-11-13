# Changelog
## 0.1.14 - 2026-11-13
- Keep one author per OpenAlex author id when a work lists the same author in
  several authorships (one per affiliation, common in large collaborations),
  joining its affiliations; 1.4 % of the DOI works had repeated person ids

## 0.1.13 - 2026-11-11
- Label the open-access URL entry in external_urls by site kind (handle, doi,
  scielo, doaj, dialnet, redalyc, arxiv, pmc; else open_access) via
  kahi_impactu_utils.Urls.url_source_kind; on update add OpenAlex URLs not
  already present instead of checking the open_access label
- Require kahi_impactu_utils>=0.1.8

## 0.1.12 - 2026-11-11
- Attach faculty and department units only when the work lists the parent
  institution for the author and the unit years cover the publication year
  (or the previous one); shared kahi_impactu_utils.Affiliations.get_units_affiliations
- Require kahi_impactu_utils>=0.1.7

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
