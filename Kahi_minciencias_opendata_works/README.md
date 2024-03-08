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
pip3 install kahi_minciencias_opendata_works
```
# Similarity support
To process works without doi, similarity is mandaroty. Then a elastic search server must be running. The plugin will use the server to find the most similar works in the database. To deply it please read https://github.com/colav/Chia/tree/main/elasticsaerch and follow the instructions.

Docker and docker-compose are required to deploy the server.

if you only wants to process works with doi, you can skip this step and remove the es_index, es_url, es_user and es_password from the yaml file.

**But it is mandatory to put `kahi_minciencias_opendata_works/doi` in the yaml file.**

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have at least one database obtained from minciencias and previously processed by [kayPacha](https://github.com/colav/KayPacha "KayPacha") and uploaded on a mongodb database.


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

