<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi minciencias openadata affiliations plugin 
Kahi will use this plugin to insert or update the affiliations information from minciencias opendata database.

# Description
Plugin that reads Yuku's current audited affiliation snapshot and
updates or inserts research groups in CoLav's database format. The historical
open-data reader remains available only through an explicit legacy mode.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_minciencias_opendata_affiliations
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
You have to run https://github.com/colav/yuku got download the data from minciencias opendata.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  minciencias_opendata_affiliations:
      database_url: localhost:27017
      database_name: dam
      source_mode: snapshot
      batch_size: 500
      num_jobs: 20
      verbose: 4
```

No collection name, release reference or import revision is needed. At plugin
startup, snapshot mode reads `dam.scienti_affiliation_publications.current`
and pins its immutable `current_run` and collection for the entire import.
It verifies the publication, passed materialization audit, completed run,
source fingerprints and affiliation count, resolves endorsing institutions
against existing Kahi affiliations, and never creates indexes in DAM. Progress
and immutable source evidence are stored in `minciencias_opendata_affiliations_runs`.
Invalid or missing entity publications stop the import; there is no fallback
to an older six-entity release. Explicit immutable six-entity `release_name`
remains supported for historical audits, but is unnecessary in normal workflows.

When `scienti_affiliations.databases` is present in the same workflow, its
`institution` collections are used as read-only lookup catalogs. Only endorsing
institutions required by groups are materialized. Resolution prioritizes
`COD_INST`, then NIT/SNIES evidence, strict normalized name and geography.
Both modes share the same group relation policy: resolve the institution's
hierarchy and select identities backed by ROR or authoritative SNIES. If a
campus and its principal both have that evidence, both relations are retained.
Without authoritative evidence, retain the initially resolved institution.
Legacy labels in the form `campus or section (principal)` are
resolved from the declared principal before fuzzy matching, so the group keeps
only the canonical principal relation. Clinics and other distinct units are
not collapsed by this rule. Unmatched names receive a deterministic `IUA_`
identifier.

Each materialized Scienti document owns at most one `COD_INST`. A second code
is stored in its own source document and may point to a verified canonical
parent; shared NIT values and fuzzy names never fuse persistent codes.

The same catalogs can be supplied explicitly as
`minciencias_opendata_affiliations.scienti_databases` using the same database
objects accepted by `scienti_affiliations.databases`.

### Checkpoints and institution audit

Snapshot checkpoints include the pinned publication, resolver version, an
automatic fingerprint of the importer's code and content fingerprints of the
configured Scienti catalogs. Changing these inputs
creates a separate import run for the same snapshot, rather than reusing a
completed checkpoint with outdated resolution evidence. Completeness ties in
catalog records are resolved deterministically, independently of cursor order.
The current Kahi runner delegates checkpoint decisions to this plugin in
snapshot mode, even when the workflow has an existing successful log with the
same YAML. An unchanged snapshot/code/catalog reuses its completed checkpoint;
a new publication or a code/catalog update starts a separate import without
editing the workflow. The revision label belongs to code; old `import_revision`
settings are ignored and should be removed from YAML.

Per-institution and per-group resolution evidence lives in the target control
collection `minciencias_opendata_affiliations_institution_resolutions` (override
with `institution_resolutions_collection`). Unique institution counters survive
partial batches and retries; summaries distinguish reused institutions,
Scienti creations, provisional creations and unresolved declarations. Imports
cannot complete with unresolved declarations or missing resolved targets.
These controls add no fields to affiliations. All entity writes follow
`KahiBase.empty_affiliation` order. Existing SNIES `campuses` are preserved;
this plugin never creates that field.

Institution lists from historical sources and old snapshots are split at
top-level `;` and `|` separators before catalog or name matching. Commas,
hyphens, conjunctions and parenthetical campus labels remain intact. Compound
names are excluded from approximate matching, including candidate aliases.
New Yuku affiliation materializations apply the same list parsing to all three
input sources and reject any remaining compound institution declaration.

On reimport, group links to proven compound `IUA_` fallback documents are
replaced only after all components resolve. This also repairs existing links
when the new snapshot already contains individual institutions. Canonical
links and unrelated group relations remain; fallback entity documents are
retained for other consumers. Registry-enriched compound IUA documents are
repaired only when one catalog code, all registered identifiers and their
atomic names prove the same institution. Source labels must also reproduce
the original IUA hash; conflicting identifiers, multiple code owners and
authoritative registry identities remain protected and stop the import for review.
The original labels/identifiers and affected references are saved before changes,
so retries and future runs can recover the evidence after labels are cleaned.
Codes and institution IDs stay with their verified owner. Each group's own
snapshot declarations determine whether its relation is retained with the
correct label or removed; a shared alias never supplies additional institutions
to other groups. Per-component and compound resolution
evidence is written only to the control collection.

The automatic code fingerprint reprocesses a snapshot previously imported with
v2. This does not require rebuilding works, persons or querying DOI services.

The isolated audit also validates these per-group decisions and unchanged
registered identifiers. Missing legacy relation IDs require either a documented
source-backed removal or an exact-name authoritative replacement of a proven
provisional identity. The report lists each explained replacement; unexplained
differences remain failures.

The observed bare country fragment `COLOMBIA` is retained in resolution controls
as `excluded / country_only_fragment`. It cannot match a foundation containing
that word or create an institution. Existing proven provisional country links
are removed while their entity documents remain as evidence; registered
identities remain protected. Excluded fragments do not make an otherwise
resolved compound declaration incomplete.

Spacing/punctuation variants such as `INTERTELCO S.A.S` / `INTER-TELCO S.A.S`
can explain a missing provisional ID only when a single Scienti catalog code
owns that normalized name, the destination owns that code uniquely, additional
NIT/institution identifiers agree, and the group's source declares the variant.
The audit records the replacement for both the group link and the institution
coverage check. This does not weaken approximate matching or discard registered
identities.

### Isolated migration validation

Run the standalone controller to clone the source inputs and target
affiliations into uniquely named audit databases, compare both modes and
verify schema/order, campus preservation, coverage and idempotence:

```shell
python scripts/launch_snapshot_audit.py \
  --workflow /path/to/workflow.yml \
  --output-dir /path/to/audit-report
tail -f /path/to/audit-report/audit.log
```

The controller uses `subprocess.Popen(start_new_session=True)`, `DEVNULL`
stdin and persistent `pid`, `status.json`, `audit.log` and `report.json` files.
Original DAM and Kahi entities are read-only during validation. Optional
`--check-yuku` also reconstructs the complete group graph from projected
metadata in the isolated source database, checking group coverage, field order
and the absence of compound institution declarations. This option requires
the local Yuku package and does not change the live release or its pointers.
Optional
`--activate-workflow` changes only affiliations to snapshot and removes manual
source/version overrides after every audit check passes; concurrent
workflow edits prevent activation. It does not run the production workflow.

The former ingestion is intentionally opt-in:

```yaml
workflow:
  minciencias_opendata_affiliations:
      database_url: localhost:27017
      database_name: dam
      source_mode: legacy
      collection_name: gruplac_groups_data
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
