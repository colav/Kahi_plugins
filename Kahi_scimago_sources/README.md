<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi scimago sources plugin 
Kahi will use this plugin to insert or update the journal information from scimago

# Description
Plugin that reads ScimagoJR ranking records from MongoDB to update or insert journal information in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_scimago_sources
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have ScimagoJR data loaded in MongoDB. The expected records are the ones produced by the `scimagojr_capture` extractor, with at least `Sourceid`, `year`, `Issn`, `Title`, `Rank`, `SJR`, `SJR Best Quartile`, `H index`, `Categories`, `Country`, `Publisher`, and `Type`.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  scimago_sources:
    database_url: localhost:27017
    database_name: scimagojr
    collection_name: scimagojr
    verbose: 4
```
Where `database_url`, `database_name`, and `collection_name` under `scimago_sources` point to the MongoDB collection with ScimagoJR records.

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/


