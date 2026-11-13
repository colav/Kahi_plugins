
<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Change Log
This is a change log for the project. It is a living document that is updated
with each release. It is a way to communicate to the team and to the public
what has changed in the project. It is also a way to communicate what needs to
be done to upgrade to the new version of the project.

This is the meta package for the project. It is a collection of all the
packages that make up the project. Each package has its own change log.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).
 
## [0.0.32] - 2026-11-13

### Changed

- Pinned Kahi_minciencias_opendata_works 0.1.10 (`workers` thread pool per
  snapshot batch for the no-DOI phase).

## [0.0.31] - 2026-11-13

### Fixed

- Pinned Kahi_openalex_works 0.1.14 (one author per OpenAlex author id; the
  raw cut repeats authors with several affiliations).

## [0.0.30] - 2026-11-12

### Changed

- Pinned Kahi_dspace_works 0.0.8-alpha (OpenAlex source of each repository
  read from its identity document; no mapping file in the workflow).

## [0.0.29] - 2026-11-12

### Changed

- Pinned Kahi_dspace_works 0.0.7-alpha (repository affiliation only with
  evidence or for single-author records).

## [0.0.28] - 2026-11-12

### Changed

- Pinned Kahi_dspace_works 0.0.6-alpha (foreign records skipped, idempotent
  re-runs), Kahi_minciencias_opendata_works 0.1.9 (title language, import
  time in updated) and Kahi_ciarp_works 0.1.2-beta (author loop performance).

## [0.0.27] - 2026-11-12

### Fixed

- Pinned Kahi_dspace_works 0.0.5-alpha (KeyError 'first_names' when a DOI
  race ends in a merge).

## [0.0.26] - 2026-11-12

### Changed

- Pinned Kahi_openalex_affiliations 0.1.5 (no `external_ids`/`external_urls`
  entries with empty value from OpenAlex `ids`).

## [0.0.25] - 2026-11-12

### Changed

- Pinned Kahi_wikidata_affiliations 0.1.3-beta (follows the redirects stored in
  `wikidata.redirects` by the impactu_airflow wikidata_capture DAG when the
  ROR QID was merged into another Wikidata item).

## [0.0.24] - 2026-11-12

### Changed

- Pinned Kahi_impactu_postcalculations 0.1.6 (persistent id migration flag
  removed from person when complete). kahi_impactu_utils 0.1.10 repairs DOIs
  with repeated prefixes, embedded URLs or trailing garbage.

## [0.0.23] - 2026-11-12

### Changed

- Pinned Kahi_minciencias_opendata_works 0.1.8 (type_audit discarded, identified
  collection resolved from the current pointer, strict title matching on
  non-DOI routes; requires kahi_impactu_utils 0.1.9 with the relaxed DOI title
  identity).

## [0.0.22] - 2026-11-11

### Changed

- Pinned Kahi_openalex_works 0.1.13 (open-access URL labelled by site kind in
  external_urls). Kahi_dspace_works 0.0.4-alpha registers repositories as
  sources and adds source_ref to handle entries (not pinned here).

## [0.0.21] - 2026-11-11

### Changed

- Pinned Kahi_openalex_works 0.1.12, Kahi_scienti_works 0.1.9 and
  Kahi_minciencias_opendata_works 0.1.7: author units validated by publication
  year (issue 07), shared through kahi_impactu_utils 0.1.7.

## [0.0.20] - 2026-11-10

### Changed

- Pinned kahi 0.1.4 (cited_by_api_url in the work template) and
  Kahi_openalex_works 0.1.11 (references and cited_by_api_url from OpenAlex).

## [0.0.19] - 2026-11-10

### Changed

- Updated the works plugin pins (openalex 0.1.10, scienti 0.1.8, scholar 0.1.7,
  minciencias_opendata 0.1.6) to the releases that normalize dashes in author names.

## [0.0.18] - 2026-11-10

### Changed

- Updated Kahi_staff_person to 0.1.8 (nombre_completo fallback, nameless records skipped).

## [0.0.17] - 2026-11-07

### Changed

- Updated the person plugin pins (openalex 0.1.7, orcid 0.0.2-alpha, scienti 0.1.8,
  staff 0.1.7, scholar 0.1.2, minciencias_opendata 0.1.7-beta) to the releases that
  normalize ASCII and Unicode dashes in names; they require kahi_impactu_utils>=0.1.6.

## [0.0.16] - 2026-11-06

### Changed

- Updated Kahi to 0.1.3, which adds the shared progress logger required by the
  plugins, and plugin pins to the versions used by the November 2026 workflow.

 
## [0.0.15] - 2026-09-09

### Changed

- Updated Kahi, postcalculation, and six-entity ScienTI snapshot plugin pins to
  the versions used by the current ImpactU workflow.
- Added the person graph unicity plugin and pinned its audited release.

 
## [0.0.6] - 2024-02-16
  
Meta package version 0.0.6 is released.
This is for old data of openalex. This is only with doi data and scienti products 111 only.
This release includes the following 
### Added
  all packages required for impactu. This was developed for the old openalex version.
  This is only with doi data.
### Changed
- initial package version. 
### Fixed
 
- all the main issues with the old openalex version.
- issues on parallel processing.
- issues on the data processing. 
