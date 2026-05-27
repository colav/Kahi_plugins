<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi publindex sources plugin
Kahi will use this plugin to insert or update journal source records from a Publindex MongoDB collection.

# Description
Plugin that reads records from MongoDB Publindex collections and upserts them in CoLav's `sources` collection format.

The implementation separates:
- insertion of new source records
- update of existing source records
- national Publindex ingestion before international homologation ingestion

# Installation
You can install from source:
```shell
pip3 install .
```

Or from package:
```shell
pip3 install kahi_publindex_sources
```

## Dependencies
Software dependencies are installed with the package.
You need:
- a target Kahi MongoDB database
- a source MongoDB database with national Publindex records, usually `publindex_data`
- optionally, an `international` collection with homologation records

# Usage
To use this plugin you must have Kahi installed and define a workflow like:

```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  publindex_sources:
    database_url: localhost:27017
    database_name: publindex
    national_collection_name: publindex_data
    international_collection_name: international
    verbose: 5
```

The plugin processes `national_collection_name` first. If `national_collection_name` is set to `national` and that collection does not exist, it falls back to `publindex_data`.

If `international_collection_name` exists, it is processed after the national collection. International records use `nombreRevista`, `calificacion`, `issns`, and `vigencia`; rankings are stored with source `publindex`.

# License
BSD-3-Clause License

# Links
http://colav.udea.edu.co/
