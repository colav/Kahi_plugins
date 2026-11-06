<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi google scholar works plugin 
Kahi will use this plugin to insert or update the works information from Google Scholar database.

# Description
Plugin that read the information from a mongodb database with google scholar information to update or insert the information of the research products in CoLav's database format.

## DOI identity and concurrent imports

Valid DOI values are normalized and looked up with `$or` in existing
`works.doi` and `works.external_ids.id`. This logic adds no fields to
the works model and creates no unique index on DOI. Concurrent insertion
decisions use the technical `_kahi_work_identity` collection, outside
works. Reserved ObjectIds preserve insertion identity even if a process
recovers another process's expired coordination lease. Source IDs and titles
are unioned atomically with `$addToSet`.

Every DOI import validates the title, including single matches and source-ID
fallbacks. Exact normalized titles match immediately. Near matches require
at least 98/100, unchanged numbers and word count, and at most one spelling
edit per changed long word. Short titles require exact normalized equality.
The comparison uses a bounded local cache and native RapidFuzz operations;
it adds no HTTP requests or extra document fields. Otherwise it inserts a separate
work preserving the same DOI and emits a warning; retries select that work.
When several works share a DOI, the incoming title selects the closest
compatible candidate. Contradictory source/DOI identities are logged and
that record is skipped, allowing the workflow to continue. Historical
duplicates are not automatically fused. Missing/malformed DOI values retain
the existing source/similarity fallback. Existing `_doi_key` data must
be cleaned after all old writers have stopped.


# Installation

You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_scholar_works
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
For the data dependencies the user must have the output of [Moai's](https://github.com/colav/Moai) scrapping of google scholar.
C++ library libhunspell-dev must be installed on your system. On ubuntu you can do it by typing
```shell
$ sudo apt install libhunspell-dev
```

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  scholar_works:
    num_jobs: 5
    verbose: 5
    database_url: localhost:27017
    database_name: scholar
    collection_name: stage
```

* WARNING *. This process could take several hours

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
