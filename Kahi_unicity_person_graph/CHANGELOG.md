# Changelog

## 0.1.1 - 2026-09-09

- Read DOI `author_count` evidence from `person.related_works` before works are
  ingested, retaining the works lookup as a conservative complement.
- Use shared affiliations only when their identifiers resolve in the configured
  Kahi affiliations collection and report dangling references in the run audit.
