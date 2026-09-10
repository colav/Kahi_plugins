<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi minciencias openadata affiliations plugin 
Kahi will use this plugin to insert or update the affiliations information from minciencias opendata database.

# Description
Plugin that reads the audited six-entity ScienTI release produced by Yuku and
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
      release_name: RELEASE_SEIS_ENTIDADES
      batch_size: 500
      num_jobs: 20
      verbose: 4
```

`release_name` must identify an immutable published release containing works,
projects, patents, events, persons and affiliations. The plugin verifies its
passed audit, lineage and affiliation count, resolves endorsing institutions
against existing Kahi affiliations, and never creates indexes in DAM. Progress
and immutable source evidence are stored in `minciencias_opendata_affiliations_runs`.

The former ingestion is intentionally opt-in:

```yaml
workflow:
  minciencias_opendata_affiliations:
      database_url: localhost:27017
      database_name: dam
      source_mode: legacy_open_data
      collection_name: gruplac_groups_data
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/

