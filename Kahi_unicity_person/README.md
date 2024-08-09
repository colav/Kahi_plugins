<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi unicity for person 
Kahi will use this plugin to do an author unicity process based on the ORCID, scholar, scopus, researchgate ids  and DOIs of your works.

# Description
This plugin will analyze the person collection and based on the ORCID, scholar, scopus, researchgate ids and DOIs of their works, it will merge records of authors that correspond to the same person.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_unicity_person
```

## Dependencies
This plugin requires kahi's person collection, it is recommended to make a backup of the collection before executing it.


# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  unicity_person:
    collection_name: person
    max_authors_threshold: 0
    num_jobs: 20
    task:
      - scholar
      - scopus
      - researchgate
      - orcid
      - doi
    verbose: 1
```

max_authors_threshold is used to filter the works to be processed with DOI according to their number of authors, use 0 to process all the works.

Task corresponds to a list of unicity processes, the available options by id are  ['linkedin', 'orcid', 'publons', 'researchgate', 'scholar', 'scopus', 'ssrn', 'wos'] and by work is “doi”. It is possible to set only one option.

* WARNING *. The doi unicity process could take several minutes

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/

