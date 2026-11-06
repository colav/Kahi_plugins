<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi scienti affiliations plugin 
Kahi will use this plugin to insert or update the affiliations information from scienti dump

# Description
Plugin that reads the information from a scienti dump to insert or update affiliations in colav's database.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_scienti_affiliations
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have at least one database obtained from minciencias and previously processed by [kayPacha](https://github.com/colav/KayPacha "KayPacha") and uploaded on a mongodb database.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  scienti_affiliations:
    databases:
      - database_url: localhost:27017
        database_name: scienti_111
        collection_name: products
        institution_collection_name: institution # optional; defaults to institution
```

If you have several scienti databases use the example below
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  scienti_affiliations:
    databases:
      - database_url: localhost:27017
        database_name: scienti_111
        collection_name: products
      - database_url: localhost:27017
        database_name: scienti_uec_2022
        collection_name: product
    verbose: 2
```

The plugin consolidates the root `institution` collections as a lookup catalog
before processing groups. It does not insert that catalog wholesale: only
institutions referenced by a processed group are enriched or materialized.
New source-only institutions use the exact `COD_INST` as `_id`.

Groups use `ANO_FORMACAO` for `year_established` when it is a four-digit year
between 1500 and the current year. The month is not required. Existing years
are preserved; missing values (`null`, empty string or `-1`) can be filled.
The plugin does not create `birthdate` for affiliations. `campuses` is always
the final field when inserting an affiliation, defaulting to `[]`.

SNIES campuses embedded in a principal affiliation resolve directly to that
principal and are not materialized as separate Scienti affiliations. Other
source units may retain their own identifiers for lineage, but group relations
record only an authoritative ROR/SNIES principal when one is available.
Institution name matching requires the same distinctive terms on both sides.

With `verbose: 1`, progress is printed immediately for catalog loading,
institution ingestion, hierarchy relations and groups in each source database.
Each phase reports its total, completed records, percentage, elapsed seconds
and processing rate, plus a final summary. Existing groups are also counted
as processed. `verbose: 0` disables these progress messages.

Optional plugin parameters: `progress_interval: 1000` and `progress_seconds: 60`.
A message is emitted after either threshold is reached, at the end of a record;
a single slow database operation is not interrupted to emit progress.

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
