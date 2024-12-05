<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi siiu_projects plugin 
Plugin for projects insertion from UdeA SIIU database.

# Description
To run this plugin is required to load the data in Oracle DB and extract it using KayPacha. 
# Installation

## Dependencies
* https://github.com/colav/oracle-docker
* https://github.com/colav/Kaypacha
* MongoDB


## Package
Write here how to install this plugin
usauly is 

`pip install kahi_siiu_projects`


# Usage
example yml file section:

```
config:
  database_url: localhost
  database_name: kahi
  log_database: kahi
  log_collection: log
  profile: False
workflow:
  siiu_projects:
    database_url: localhost:27017
    database_name: siiu
    collection_name: project
    num_jobs: 20
    verbose: 1

```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



