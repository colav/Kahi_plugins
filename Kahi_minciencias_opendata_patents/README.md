# Kahi minciencias opendata patents

Imports patents from the `patents` member of an audited, published six-entity
ScienTI release. The plugin resolves `COD_RH` authors and `COL` research groups
against the Kahi `person` and `affiliations` collections without changing them.

The source release must include exactly `works`, `projects`, `patents`, `events`,
`persons`, and `affiliations`, have a passing audit with no critical anomalies,
and retain the audited document count. `release_name` must be immutable and
explicit in snapshot mode. Legacy mode reads `gruplac_production_data`.

## Usage

```yaml
config:
  database_url: mongodb://localhost:27017
  database_name: kahi
workflow:
  minciencias_opendata_patents:
    database_url: mongodb://localhost:27017
    database_name: dam
    source_mode: snapshot
    release_name: scienti_six_entities_YYYYMMDD
    batch_size: 500
    verbose: 1
```

Progress and source evidence are recorded in
`minciencias_opendata_patents_runs`, so a failed import resumes after the last
completed batch. Existing patent metadata from other sources is preserved.

For the historical reader use `source_mode: legacy`; `collection_name` defaults
to `gruplac_production_data`.

## Installation

```shell
pip install kahi_minciencias_opendata_patents
```

BSD-3-Clause License.
