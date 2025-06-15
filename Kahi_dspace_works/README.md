<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi dspace_works plugin 
This is the plugin for the kahi workflow system that allows to process the works from the Dspace repository.


# Description
The dataset have to be downloaded from the Dspace repository using https://github.com/colav/oxomoc and stored in mongodb, 
then the works are processed to extract the metadata.

# Installation

## Dependencies
- Kahi_impactu_utils
- MongoDB
- oxomoc dataset already downloaded

## Package
The package is available in the PyPi repository, so you can install it using pip:
`pip install kahi_dspace_works`


# Usage
Parameters for kahi_run in the workflow should be similar to.

```
  dspace_works:
    database_url: localhost:27017
    database_name: oxomoc
    repositories:
      - institution_id: https://ror.org/03bp5hc83 # Universidad de Antioquia
        collection_name: dspace_udea_records
        repository_url: https://repositorio.udea.edu.co
      - institution_id: https://ror.org/00jb9vg53 # Universidad del Valle
        collection_name: dspace_univalle_records
        repository_url: https://bibliotecadigital.univalle.edu.co
      - institution_id: https://ror.org/05tkb8v92 #Universidad Autónoma Latinoamericana
        collection_name: dspace_unaula_records
        repository_url: http://repositorio.unaula.edu.co:4000
      - institution_id: https://ror.org/02xtwpk10 # Universidad Externado de Colombia
        collection_name: dspace_uext_records
        repository_url: https://bdigital.uexternado.edu.co
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



