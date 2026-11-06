<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi OpenAlex person plugin
Kahi will use this plugin to insert or update authors' information from openalex

# Description
Plugin that reads the information from a mongodb collection with openalex information to update or insert the information of the authors in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_openalex_person
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have a copy of the openalex dumpwith the collection of venues which can be downloaded at [OpenAlex data dump website](https://docs.openalex.org/download-all-data/openalex-snapshot "OpenAlex data dump website") and import it on a mongodb database.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  openalex_person:
    database_url: localhost:27017
    database_name: openalexco
    collection_name: authors
    collection_name_works: works #required for related works
    num_jobs: 20
    batch_size: 500 # bounded author batches for institution lookups
    skip_previously_processed: false # true: enrich affiliation years of processed authors without reloading works
    verbose: 2
```

## Affiliation years

The plugin imports only `authors.last_known_institutions` from the local OpenAlex
dump. It resolves each institution through its ROR/OpenAlex identifiers, reads
`authors.affiliations[].years` for the same resolved institution, and updates
`person.affiliations[].years` as a sorted union with the years already stored.
Historical institutions outside `last_known_institutions` are not added or
enriched. Missing or empty `last_known_institutions` adds no affiliations.
Existing affiliations from staff or other sources are preserved, including any
historical institutions already imported by an earlier plugin version.
Incoming years use the shared `Utils.normalize_publication_year` validation
(1800 through the current year + 2). Missing source years contribute an empty list
and never erase existing years. When both the existing affiliation and incoming
evidence lack years, the existing affiliation stays unchanged, preserving date
fallbacks used by CIARP. Newly added institutions have `years: []` if unavailable.
Institution years do not alter affiliation dates
or extend faculty, department or group histories.

Person lookup requires a unique OpenAlex ID or a unique ORCID with a valid check
digit. Ambiguous identifiers, contradictory identities and unresolved/conflicting
institution identifiers produce `openalex_person.identity_conflict` warnings in
the ETL log, including the author identifiers, candidate IDs and reason. Ambiguous
authors are skipped; an unresolved institution is skipped independently. A unique
ORCID is still source evidence, not proof that the source assigned it correctly.

Updates use one MongoDB aggregation-pipeline write per person, merging each
institution by its internal ID against the current stored document. Concurrent
updates retain years without adding duplicate affiliation objects. Each author
batch prefetches its institutions in one indexed query; a bounded, thread-safe
cache persists across batches. **MongoDB 5.2+** is required for
`$sortArray`. No HTTP requests, work-document fields or workflow audit steps are
introduced. Run OpenAlex after staff to enrich staff years; later staff runs can
replace these lists with their own source intervals.

When `skip_previously_processed` is enabled, already-processed authors still
receive affiliation-year enrichment, without querying related works again.


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
