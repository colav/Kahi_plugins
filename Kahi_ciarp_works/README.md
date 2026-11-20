<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi CIARP works plugin 
Kahi will use this plugin to insert or update the works information from ciarp office file from public universities.

# Description
Plugin that reads papers published and reported to the CIARP office from MongoDB to update or insert the information of the research products in CoLav's database format.

## Author-affiliation contract

CIARP affiliations on a work are temporal evidence and must follow these
rules consistently for inserts and updates:

1. The work publication year or the previous year must be covered by the
   author's parent affiliation. Explicit `years` take precedence over the
   start/end envelope, so interruptions are not treated as continuous service.
2. A missing end date means that the affiliation is current; the interval
   extends through the execution year.
3. A missing start date provides no verifiable interval. In that case CIARP
   must not add the institution, faculty, or department to the work.
4. Select faculties and departments from the person's dated affiliations,
   validating their relation to the parent institution. For each unit type,
   prefer the publication year and use the previous year only when needed.
   Never infer units from the product's CIARP codes. Without eligible units,
   add only the eligible parent institution. Unknown years add no affiliations.
5. CIARP must never remove affiliations contributed by OpenAlex, ScienTI,
   staff, or another source when its own temporal evidence is insufficient.
6. These rules govern affiliations embedded in products. The complete staff
   affiliation history remains stored on the person document.

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
pip3 install Kahi_ciarp_works
```
# Similarity support
To process works without doi, similarity is mandaroty. Then a elastic search server must be running. The plugin will use the server to find the most similar works in the database. To deply it please read https://github.com/colav/Chia/tree/main/elasticsaerch and follow the instructions.

Docker and docker-compose are required to deploy the server.

if you only wants to process works with doi, you can skip this step and remove the es_index, es_url, es_user and es_password from the yaml file.

**But it is mandatory to put `ciarp_works/doi` in the yaml file.**


# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  ciarp_works/doi:
    database_url: localhost:27017
    database_name: institutional_data
    ciarp_collection_name: ciarp
    num_jobs: 1
    verbose: 4
  ciarp_works:
    database_url: localhost:27017
    database_name: institutional_data
    ciarp_collection_name: ciarp
    num_jobs: 1
    verbose: 4
```

* WARNING *. This process could take several hours

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
