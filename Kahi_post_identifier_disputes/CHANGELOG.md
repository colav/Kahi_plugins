# Changelog

## 0.0.5-alpha - 2026-11-15

- Kahi's log now skips `collect` and a real `apply` once they succeeded with the
  same configuration; they run again after restoring the database, in a new
  release or with `--log no`. An `apply` with `dry_run` still always runs.

## 0.0.4-alpha - 2026-11-14

- A person left without any ORCID iD also loses the `updated` entries with
  source `orcid`; the trace keeps them under `removed.updated`.
- The `updated` entry of corrected works and people has source `disputes`
  (0.0.3-alpha renamed it from `identifier_disputes`).

## 0.0.3-alpha - 2026-11-14

- `apply` settles the ORCID iDs shared by several people with the public names
  of the record: the person named in it keeps the iD, a person whose name has a
  word the record lacks loses it, and a name that fits inside the registered
  one is left untouched as a possible duplicate profile.

## 0.0.2-alpha - 2026-11-14

- Task `apply`: settle the DOIs shared by several works with what the
  identifier resolver stored. A work loses the DOI when the DOI does not exist,
  identifies a container (journal, issue, proceedings, book without authors) or
  is registered with another title and no author in common. A matching title
  keeps it; a shared author, a similar title or missing author evidence leave
  the work untouched. Trace in `identifier_disputes`; `dry_run` writes the
  decisions to `identifier_disputes_dry_run` and changes no work.

## 0.0.1-alpha - 2026-11-14

- New plugin. Task `collect`: queue the DOIs shared by two or more works and
  the ORCID iDs shared by two or more people in the identifier resolver
  database (`impactu_identifier_resolver`), only the ones it does not know yet.
