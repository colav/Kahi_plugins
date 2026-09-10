<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi minciencias opendata projects plugin
Kahi uses this plugin to import the audited ScienTI projects snapshot.

# Description
The plugin streams `projects` from an explicit immutable six-entity Yuku
release. It does not parse `gruplac_production_data` or modify DAM.

Authors identified by `COD_RH` and groups identified by `COL...` are resolved
against Kahi. Empty author identifiers remain empty and are never matched by
name. `source_metadata` stays in DAM. Existing non-empty metadata is preserved
so the snapshot can coexist with later SIIU enrichment.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_minciencias_opendata_projects
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
  minciencias_opendata_projects:
    database_url: localhost:27017
    database_name: dam
    release_name: scienti_kahi_release_YYYYMMDD
    batch_size: 500
    verbose: 1
```

`release_name` cannot be `current`. Progress and source evidence are stored in
`minciencias_opendata_projects_runs`, allowing interrupted imports to resume.
Affiliations, people and person unicity must run first; SIIU should run later.

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
