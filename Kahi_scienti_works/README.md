<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi scienti works plugin 
Kahi will use this plugin to insert or update the works information from scienti database.

# Description
Plugin that reads the information from a scienti dump to insert or update the information of the of academic products in colav's database.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_scienti_works
```
# Similarity support
To process works without doi, similarity is mandaroty. Then a elastic search server must be running. The plugin will use the server to find the most similar works in the database. To deply it please read https://github.com/colav/Chia/tree/main/elasticsaerch and follow the instructions.

Docker and docker-compose are required to deploy the server.

if you only wants to process works with doi, you can skip this step and remove the es_index, es_url, es_user and es_password from the yaml file.

**But it is mandatory to put `scienti_works/doi` in the yaml file.**

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


## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have at least one database obtained from minciencias and previously processed by [kayPacha](https://github.com/colav/KayPacha "KayPacha") and uploaded on a mongodb database.
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
  log_database: kahi
  log_collection: log
workflow:
  scienti_works/doi:
    es_index: kahi_es
    es_url: http://localhost:9200
    es_user: elastic_user
    es_password: elastic_pass
    databases:
    - database_url: localhost:27017
      database_name: scienti
      collection_name: products
    num_jobs: 5
    verbose: 5
  scienti_works:
    es_index: kahi_es
    es_url: http://localhost:9200
    es_user: elastic_user
    es_password: elastic_pass
    databases:
    - database_url: localhost:27017
      database_name: scienti
      collection_name: products
    num_jobs: 5
    verbose: 5
```

If you have several scienti databases use the example below
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  scienti_works/doi:
    es_index: kahi_es
    es_url: http://localhost:9200
    es_user: elastic_user
    es_password: elastic_pass
    databases:
      - database_url: localhost:27017
        database_name: scienti_udea
        collection_name: products
      - database_url: localhost:27017
        database_name: scienti_uec_2022
        collection_name: product
    num_jobs: 5
    verbose: 5
```

* WARNING *. This process could take several hours

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
