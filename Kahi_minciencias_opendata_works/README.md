<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi minciencias opendata works plugin
Kahi uses this plugin to import the audited ScienTI works snapshot.

# Description
Snapshot mode streams `works` from an explicit immutable six-entity Yuku
release. Explicit legacy mode reads `gruplac_production_data`; promotion from
`person.related_works` remains disabled by default.

Authors identified by `COD_RH` and groups identified by `COL...` are resolved
against Kahi. Empty author identifiers remain empty and are never matched by
name. `source_metadata` and `authorship_status` stay in DAM. Existing non-empty
metadata is preserved, while missing `bibliographic_info` values are enriched
recursively from the snapshot.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_minciencias_opendata_works
```
# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  minciencias_opendata_works:
    database_url: localhost:27017
    database_name: dam
    source_mode: snapshot
    release_name: scienti_kahi_release_YYYYMMDD
    batch_size: 500
    verbose: 1
```

`release_name` cannot be `current`. The plugin records checkpoints and source
evidence in `minciencias_opendata_works_runs`, allowing an interrupted import to
resume from its last completed batch. Affiliations and people must be loaded first.

For the historical reader use `source_mode: legacy`; `collection_name` defaults
to `gruplac_production_data`.

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/


## DOI and title identity

DOI imports use the shared Kahi DOI lookup and title policy: exact normalized
titles or conservative spelling variants (98/100). Different titles are kept
in separate documents with the same DOI. Insertions coordinate through the
existing `_kahi_work_identity` collection, without adding fields to `works`.
The title comparison is local and does not make DOI registry requests.

## Identified snapshots and execution phases

A filtered Yuku snapshot is accepted when its immutable collection publication
in `scienti_identified_work_publications`, passed audit and completed run agree
with the configured parent six-entity release. The parent release is never
rewritten. Sparse optional fields receive `KahiBase.empty_work()` defaults.

Without `collection_name` the plugin reads the `current` pointer of
`scienti_identified_work_publications` (`release_name`, `full_graph`,
`collection`) and consumes its filtered collection after the same validation;
the pointer must belong to the resolved release. `identified: false` forces
the full works graph, and an explicit `collection_name` overrides the pointer.
Source-only audit fields such as `type_audit` are discarded on import.

Use `minciencias_opendata_works/doi` in the DOI stage and
`minciencias_opendata_works/nodoi` after the other no-DOI importers. Both use:

```yaml
database_url: localhost:27017
database_name: dam
source_mode: snapshot
release_ref: minciencias_open_data_current
batch_size: 500
verbose: 1
```

`release_ref` must be defined in the Kahi workflow configuration. A direct
immutable `release_name` is also supported. The partitions use the shared DOI
validator on the primary field and DOI external IDs. A malformed primary DOI
with a valid external DOI belongs to the DOI phase. Missing or invalid DOI
belongs to `nodoi`. Two conflicting external DOIs without a valid primary DOI
are rejected rather than guessed. Progress counters distinguish scanned
(`processed`), selected and skipped rows. Checkpoints bind the collection,
selection and identified audit evidence, so completing one partition cannot
skip the other.

The no-DOI partition checks existing source ids, ScienTI author/product pairs,
MinCiencias product ids and ISBN with compatible titles and publication years.
ISBN formatting variants have a title-indexed fallback. Conflicting known
authors prevent ISBN-only merging; ISSN never identifies a publication. It
preserves an existing work's DOI when enriching it. Ambiguous matches or source
id collisions are logged and counted as `identity_conflicts`, without inserting
another duplicate or overwriting an unrelated document. Cross-process locks
use the existing `_kahi_work_identity` technical collection. When Elasticsearch is configured for `nodoi`, a conservative similarity
fallback can also recognize existing works with different identifiers.
Equivalence is never established from a title score alone.

## Elasticsearch in the no-DOI snapshot stage

Add `es_index`, `es_url`, and optionally both `es_user` / `es_password` to
`minciencias_opendata_works/nodoi`. Use the index populated from the current
MongoDB works collection after the DOI stage and updated by the other no-DOI
plugins. The enabled DOI block must not receive these parameters.

The importer first checks source/product identifiers. On no match, it checks
exact live titles and then requests up to 20 candidates per source title using
Mohan's existing Elasticsearch query. MongoDB supplies the authoritative title,
authors, year and bibliography for the final decision: conservative shared
98/100 title matching, no contradictory known years/volume/issue/pages and
shared canonical authors or compatible full names when ids are unavailable.
Authorless records require an exact substantial title, matching year and at
least two bibliography matches. The legacy loose author/title thresholds are
not reused for automatic merging.

Stale hits are logged/counted as `stale_es_hits`; multiple compatible live
candidates are logged as `identity_conflicts` without adding another work.
A found work keeps its id and existing DOI and non-empty metadata. Updated and
new documents are indexed with the existing Mohan mapping and refreshed before
the next source decision; a bounded cache avoids identical repeat writes.
Search/index errors fail the batch rather than inserting blindly. A retry
repairs an interrupted index write through the already stored MongoDB identity.
Checkpoints include the similarity policy, index and URL without credentials.
The technical title locks add no fields to works. Elasticsearch's bounded
candidate search is conservative and does not prove global absence of duplicates.

## Book versus chapter veto in the no-DOI stage

Both identifier lookup and exact/Elasticsearch similarity reject a known book
versus a known chapter, even with the same ISBN, title, year and authors. The
rule uses existing `types` values and exact native labels from the installed
ImpactU catalog, with English/Spanish canonical aliases as a fallback. A whole
`Libro resultado de investigacion por capitulos` remains a book. A known book
or chapter cannot merge with missing or contradictory book-family evidence.
Same-kind enrichment still follows the other title, author and year checks.
The rule creates no new fields in works. A source-id collision is reported
without overwriting the existing work. Non-DOI checkpoint identities include
the veto policy and a digest of the recognized aliases; DOI checkpoints and
DOI matching are unchanged.

This guarantee depends on type evidence. When both records have no identifiable
book/chapter types, the importer cannot infer a family from title or ISBN alone;
the existing matching policy still applies. Incorrect types describing both as
books cannot reveal that one is actually a chapter. This change prevents new
incompatible merges; it does not repair earlier stored data or reload Python
modules already imported by an active ETL process.


`workers` (default 1) sets how many threads import the rows of each snapshot batch; identity leases keep concurrent rows consistent. Use it with `task=nodoi`, where each row queries Elasticsearch.
