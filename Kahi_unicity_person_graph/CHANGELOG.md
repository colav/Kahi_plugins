# Changelog

## Unreleased

- Reject explicit incompatible complete given names before consulting aliases,
  including short initial-and-surname matches and shared identifiers. Preserve
  compatible truncation, initials and full-name reordering, and enforce the
  same rule for transitive cluster unions.
- Allow one DOI above the exact-name author limit when the normalized full
  names are identical with three or more complete words and are corroborated
  by a valid shared affiliation or authoritative personal identifier. Keep
  alias-only, missing-count, identity-conflict and transitive-cluster guards.
- Record the exception reason and supporting name evidence in graph audit
  records only; entity documents retain their existing schema.

## 0.1.1 - 2026-09-09

- Read DOI `author_count` evidence from `person.related_works` before works are
  ingested, retaining the works lookup as a conservative complement.
- Use shared affiliations only when their identifiers resolve in the configured
  Kahi affiliations collection and report dangling references in the run audit.
