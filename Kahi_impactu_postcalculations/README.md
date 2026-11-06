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
ImpactU type mappings are loaded from the versioned
`Kahi_impactu_type_catalog` package; this plugin no longer maintains a separate
Excel copy.

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
    inference_endpoint: http://localhost:8082/invocations
    topic_inference_mode: local
    inference_endpoints:
      - http://localhost:8082/invocations
      - http://localhost:8083/invocations
    topics_enabled: true
    topic_jobs: 4
    topic_batch_size: 1
    topic_request_timeout: 300
    topic_request_retries: 3
    person_topics_enabled: false
    person_topics_dry_run: true
    person_topics_jobs: 24
    person_topics_batch_size: 1000
    person_topics_min_works: 2
    person_topics_limit: 5
    person_topics_singleton_min_score: 0.90
    person_topics_progress_interval: 10000
    person_topics_staging_collection: person_topics_tmp
    person_topics_runs_collection: person_topics_runs
    denormalization_enabled: true
    geographic_normalization_enabled: true
    geographic_normalization_dry_run: false
    geographic_batch_size: 1000
    persistent_ids_enabled: true
    types_enabled: true
    openalex_enrichment_enabled: true
    openalex_enrichment_dry_run: false
    openalex_enrichment_batch_size: 1000
    abstracts_enrichment_enabled: false
    abstracts_dry_run: true
    abstracts_database_url: localhost:27017
    abstracts_database_name: impactu_abstract_resolver
    abstracts_collection_name: resolved
    abstracts_runs_collection_name: kahi_enrichment_runs
    abstracts_batch_size: 1000
    abstracts_title_threshold: 90.0
    networks_enabled: true
    top_words_strategy: legacy
    top_words_batch_size: 500
    top_words_backend: threading
    top_words_jobs: 4
    force_recalculate: false
    backend: multiprocessing
    n_jobs: 6
    verbose: 5
    author_count: 6 #use this with warning, maybe the network is too big and it can not be saved in MongoDB
```

Notes:
- Denormalization runs with collection-level parallelization enabled by default.
- Geographic normalization updates existing `addresses.country`, `state` and
  `city` values in place before other calculations. It uses Spanish CLDR country
  names plus the versioned alias catalog bundled with the plugin; it adds no fields.
- Use `geographic_normalization_dry_run: true` to audit changes without writing.
- OpenAlex enrichment runs before IDs, types and topic inference. It restores
  missing abstracts and native topics by exact OpenAlex work identifier.
- Abstract resolver enrichment selects DOI-bearing works without abstracts,
  looks up the DOI in `impactu_abstract_resolver.resolved`, requires a fuzzy
  title score at or above `abstracts_title_threshold`, and stores only
  `abstract_inverted_index`. Each execution writes a summary document to
  `abstracts_runs_collection_name`, including dry runs and failures.
- The internal denormalization parallel setup is fixed to `parallel_collections = true`.
- The internal denormalization parallel setup is fixed to `collection_jobs = 3`.
- These two values are defined in code and are not configurable from the workflow YAML.
- Topic inference sends `topic_batch_size` works per request and writes each
  completed batch with a single unordered bulk operation.
- `inference_endpoints` creates one sequential worker per isolated model
  container; no endpoint receives concurrent TensorFlow requests.
- `topic_inference_mode` documents whether endpoints are `local` or `remote`.
  Remote connection failures abort safely and leave pending works untouched.
- Person-topic aggregation runs after work-topic inference and only includes
  people with a `COD_RH`, ORCID or a recognized identity-document type. It
  ranks topics by normalized accumulated work confidence and keeps at most
  five. Topics supported by at least two works take the available slots first;
  single-work topics at or above `person_topics_singleton_min_score` fill the
  remaining slots. `primary_topic` is always the retained topic with the
  highest aggregate score and equals `topics[0]`.
- `person_topics_dry_run: true` rebuilds the auditable staging collection and
  run report without changing `person`. Apply mode updates only
  `primary_topic` and `topics`; `subjects` remains untouched. The phase uses a
  configurable threaded worker pool through `person_topics_jobs`.
- Set `denormalization_enabled: false` only when denormalization already
  completed and the plugin is being resumed at topic inference.
- `persistent_ids_enabled` and `types_enabled` allow a resumed run to skip
  those completed, idempotent phases.
- Set `force_recalculate` to `true` when the calculations database contains
  results from an older ETL and networks/top words must be refreshed.
- Set `networks_enabled` to `false` to skip affiliation and person
  co-authorship networks without disabling the remaining post-calculations.
- Set `topics_enabled` to `false` to skip topic inference when all pending
  works were already processed and the plugin is being resumed.
- `top_words_backend` and `top_words_jobs` control only the top-words phase.
  Use `threading` with a small worker count to avoid exhausting MongoDB
  connections while the general plugin backend remains unchanged.
- Set `top_words_strategy: works_scan` to compute person and affiliation
  top words by scanning `works` once. The output schema is unchanged, but
  titles are processed once per work instead of once per person/affiliation.
- Type-mapping warnings are emitted only with `verbose: 4` or higher. Type
  mappings are compiled once per source and reused by all workers.
- Type rules come from the shared catalog JSON bundled by
  `Kahi_impactu_type_catalog==0.2.0`.
- Before any database mutation, preflight validates MongoDB, required
  collections, the type/geographic catalogs, spaCy models and a real topic
  inference whose predicted topic must exist in the OpenAlex catalog.


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
