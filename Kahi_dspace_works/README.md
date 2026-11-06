<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi dspace_works plugin 
This is the plugin for the kahi workflow system that allows to process the works from the Dspace repository.


# Description
The dataset have to be downloaded from the Dspace repository using https://github.com/colav/oxomoc and stored in mongodb, 
then the works are processed to extract the metadata.

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
    database_name: oxomoc_colombia
    batch_size: 500
    num_jobs: 12
    # Optional override: collections are discovered automatically from *_records
    # and their institution ROR is read from the matching *_identity document.
    repository_affiliations:
      dspace_udea_records: https://ror.org/03bp5hc83
      dspace_univalle_records: https://ror.org/00jb9vg53
```

`repositories` remains supported for backwards compatibility. Use `collections`
or `exclude_collections` to restrict automatic discovery.
When present, `repository_affiliations` overrides the `ror` stored in the
matching `*_identity` document. This keeps older Oxomoc datasets compatible.


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
