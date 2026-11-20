<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi minciencias openadata person plugin
Kahi uses this plugin to import the audited ScienTI person snapshot.

# Description
With `source_mode: snapshot`, the plugin consumes `persons` from an explicit
immutable six-entity Yuku release. With `source_mode: legacy`, it reads
`cvlac_data`, `gruplac_production_data` and the immutable raw CvLAC HTML
collection. It also uses `recognized_researchers` to add verified `COD_RH` and
typed identity documents. The raw HTML replaces both historical `cvlac_stage`
collections.

Only DOI-identified MinCiencias related works are imported. Existing related works
from other providers are preserved, while legacy MinCiencias related works are
replaced. Group affiliations are resolved to their Kahi identifiers; therefore the
affiliations plugin must run first.

In snapshot mode, new people receive a MongoDB `ObjectId`. Existing people are
looked up first by `external_ids.id.COD_RH`, then by ORCID when no COD_RH match
exists. The snapshot `_id` is never used as a destination id or a lookup key.
ORCID matching validates the checksum and requires compatible names: normalized
token equality, or token-sort similarity of at least 92 with no contradictory
known given/surname components. Name normalization handles accents, punctuation,
case and token order without NLP models or network requests.
Unresolved ORCID ambiguities or incompatible names produce a warning and a
separate person; they never overwrite an arbitrary candidate. Matching an
existing non-ObjectId destination fails, since persistent ids belong to the
postcalculations stage. Incoming records without a normalized full name are
excluded and counted as `skipped_empty_name`.

For every group received in the snapshot, its already imported affiliation
relations supply principal institutions. Groups, faculties, departments, units,
subunits and campuses are excluded as institution candidates. A missing
institutional id is added once, with `start_date: null`, `end_date: -1`,
`position: ""` and `years: []`. These are indirect links, not employment periods.
No group membership dates, product dates or institution establishment years are
used for them. Existing entries with the institutional id retain all their
metadata, dates and years. Group membership entries still use full-object
equality and can retain different periods. Lookups are cached and preloaded in
batches. No CvLAC employment experience is imported in snapshot mode.

The summary also reports `matched_orcid`, `orcid_conflicts` and
`indirect_institutions_added`. Warnings include the source COD_RH and candidate
ids. These diagnostics remain in the log/technical checkpoint, not in person
documents. When a compatible ORCID links different COD_RH profiles, their codes
and metadata are retained together; repeated batches preserve that union.
The checkpoint includes the `objectid-orcid-group-institutions-v1` import policy
so the previous snapshot implementation cannot silently mark this one complete.

In legacy mode, an institution linked through a group's product is retained as
indirect affiliation evidence, with `start_date: null`, `end_date: -1` and
`years: []`. The product's `fcreacion_pd` does not establish institutional
membership or an employment interval. Existing institutional histories from
other sources are preserved. The group link still retains the product date;
that date must not be interpreted as confirmed employment at its institution.
This change affects new imports; it does not remove dates already stored.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_minciencias_opendata_person
```

## Dependencies
The source database must contain a published and audited six-entity ScienTI release.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
   minciencias_opendata_person:
    database_url: localhost:27017
    database_name: dam
    source_mode: snapshot
    release_name: scienti_kahi_release_YYYYMMDD
    batch_size: 1000
    verbose: 5
```

`release_name` cannot be `current` in snapshot mode. Legacy mode defaults to
`cvlac_data`, `gruplac_production_data` and
`cvlac_stage_raw_full_corrected_20260823`. It enriches identifiers from
`recognized_researchers`, but does not publish an identity document when its
type cannot be established. It ignores CvLAC employment experiences and
retains only DOI-identified `related_works`.
Progress and immutable source evidence are stored in
`minciencias_opendata_person_runs`, allowing safe batch-level resumption.

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
