<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi impactu postcalculations
This plugin helps to calculate netwoks, top words and some other stuff from the impactu output.

# Description
Supports the calculation of the following metrics:
- Co-authorship network 
  - affiliations
  - authors 
- Top words for 
  - affiliations 
  - authors

# Installation

## Dependencies
This package requires MongoDB to be installed and running and kahi already executed.

## Package
To install the package, run the following command:
`pip install kahi_impactu_postcalculations
`


# Usage

example for workflow:

```
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  impactu_postcalculations:
    database_url: localhost:27017
    database_name: kahi_calculations
    openalex_database_url: localhost:27017
    openalex_database_name: openalexco
    inference_endpoint: http://localhost:8080/invocations
    backend: multiprocessing
    n_jobs: 6
    verbose: 5
    author_count: 6 #use this with warning, maybe the network is too big and it can not be saved in MongoDB
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



