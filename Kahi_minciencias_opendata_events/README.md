# Kahi minciencias opendata events

Imports events from the `events` member of an audited, published six-entity
ScienTI release. The plugin resolves `COD_RH` authors and `COL` research groups
against the Kahi `person` and `affiliations` collections without changing them.

The source release must include exactly `works`, `projects`, `patents`, `events`,
`persons`, and `affiliations`, have a passing audit with no critical anomalies,
and retain the audited document count. `release_name` must be immutable and
explicit; `current` and the former open-data options are rejected.

## Usage

```yaml
config:
  database_url: mongodb://localhost:27017
  database_name: kahi
workflow:
  minciencias_opendata_events:
    database_url: mongodb://localhost:27017
    database_name: dam
    release_name: scienti_six_entities_YYYYMMDD
    batch_size: 500
    verbose: 1
```

Progress and source evidence are recorded in
`minciencias_opendata_events_runs`, so a failed import resumes after the last
completed batch. Existing event metadata from other sources is preserved.

## Installation

```shell
pip install kahi_minciencias_opendata_events
```

BSD-3-Clause License.
