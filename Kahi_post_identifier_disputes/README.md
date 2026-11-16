<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi post identifier disputes plugin

Settles the identifiers that Kahi ends up holding on more than one entity:

- a **DOI** that is the `doi` of two or more works (a journal DOI copied to its
  articles, a truncated DOI, the DOI of another paper);
- an **ORCID iD** held by two or more people.

The plugin never uses the network. It works with the identifier resolver
database (`impactu_identifier_resolver`), which the `identifier_resolver_capture`
DAG of [impactu_airflow](https://github.com/colav/impactu_airflow) fills from
Crossref, DataCite, doi.org and the ORCID public API.

# Tasks

## collect

Finds the shared identifiers and queues the ones the resolver does not know yet
as `{"_id": <identifier>, "status": "pending"}` in the resolver collections
`doi` (bare DOI in lower case) and `orcid` (ORCID iD without the URL).
Identifiers already in the resolver are left untouched, so it can run after
every works stage and only the new disputes are queued. Each execution leaves a
summary in `identifier_disputes_runs`, with how many of the shared identifiers
are `pending`, `ok`, `not_found` or `error` in the resolver.

Kahi's log skips `collect` and a real `apply` once they succeeded with the same
configuration: they are due again only after restoring the database (the log is
restored with it) or in a new release. To repeat them on purpose, for instance
after importing works again, run the workflow with `--log no`. An `apply` with
`dry_run` always runs.

## apply

Settles every shared DOI the resolver has already resolved, work by work:

| situation | action | reason |
|---|---|---|
| the DOI does not exist | removed | `doi_not_found` |
| the work title matches the registered one (markup ignored, subtitle tolerated) | kept | `title_match` |
| the DOI is a journal, issue, proceedings, or a book registered without authors | removed | `container_doi` |
| other title, similarity 70-89 | untouched | `similar_title` |
| other title, an author in common with the registry | untouched | `shared_author` |
| other title, no way to compare authors | untouched | `no_author_evidence` |
| other title, no author in common | removed | `other_work_doi` |

Authors are compared with the ones the registry lists (with the authors of the
work that matches the title when it lists none).

A work that loses the DOI gets `doi: null`, loses the `external_ids` entry of
that DOI and gains an `updated` entry with source `disputes`. Every
decision is upserted in `identifier_disputes` (`_id: "<work>:doi:<doi>"`), with
the removed values under `removed`, so it can be undone. DOIs still `pending` or
`error` in the resolver are skipped and counted. Two works that both match the
title keep the DOI: they are duplicates to merge, which this plugin does not do.

A shared ORCID iD is settled with the public names of the record (given names,
family name, credit name and other names):

| situation | action | reason |
|---|---|---|
| the only person with the most registered words in the name (two or more, a given name and a family name; a registered initial counts) | kept | `name_match` |
| another person whose name has a word the record does not have | removed | `other_person_orcid` |
| another person whose name fits inside the registered one (maybe a second profile) | untouched | `possible_duplicate_person` |
| two people tie, or nobody matches | untouched | `tie`, `no_owner` |
| the record does not exist or publishes no name | untouched | `orcid_not_found`, `no_registry_name` |

The person loses the `external_ids` entries with source `orcid` of that iD and
gains the `updated` entry. When no ORCID iD is left on the profile, its `updated`
entries with source `orcid` are removed too (kept under `removed.updated`). The trace (`_id: "<person>:orcid:<orcid>"`) has
`review: true` when one of the removed entries came from the ORCID plugin: that
person may have been enriched from the wrong record. Works are not reassigned.

`dry_run: true` changes no work and writes the decisions to
`identifier_disputes_dry_run` (rebuilt on every run).

Run it after every works plugin and before `post_cites_count` and
`impactu_postcalculations`. Importing works again afterwards brings the bad DOIs
back from the sources; run `collect`, the capture and `apply` again.

# Workflow

```
... works with DOI (phase A)
post_identifier_disputes/collect        # queue
(impactu_airflow) identifier_resolver_capture   # resolve; can run while Kahi continues
... works without DOI (phase B)
post_identifier_disputes/collect        # queue the few new disputes
(impactu_airflow) identifier_resolver_capture
post_identifier_disputes/apply
```

# Usage

```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  post_identifier_disputes/collect:
    resolver_database_url: localhost:27017   # default: database_url
    resolver_database_name: impactu_identifier_resolver
    verbose: 1
  post_identifier_disputes/apply:
    resolver_database_name: impactu_identifier_resolver
    dry_run: false
    verbose: 1
```

# Installation

```shell
pip install kahi_post_identifier_disputes
```

# License

BSD-3-Clause License

# Links

http://colav.udea.edu.co/
