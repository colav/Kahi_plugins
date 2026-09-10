<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi minciencias openadata person plugin
Kahi uses this plugin to import the audited ScienTI person snapshot.

# Description
The plugin consumes the `persons` collection from an explicit immutable six-entity
Yuku release. It does not read or aggregate the historical open-data, CvLAC HTML,
private-profile or group-production collections.

Only DOI-identified MinCiencias related works are imported. Existing related works
from other providers are preserved, while legacy MinCiencias related works are
replaced. Group affiliations are resolved to their Kahi identifiers; therefore the
affiliations plugin must run first.

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
    release_name: scienti_kahi_release_YYYYMMDD
    batch_size: 1000
    verbose: 5
```

`release_name` cannot be `current`. Options from the former reader are rejected.
Progress and immutable source evidence are stored in
`minciencias_opendata_person_runs`, allowing safe batch-level resumption.

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/

