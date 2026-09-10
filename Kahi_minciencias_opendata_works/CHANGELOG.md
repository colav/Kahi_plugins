# Changelog

## 0.1.2 - 2026-09-09

- Replace the legacy GrupLAC parser and Elasticsearch path with bounded,
  resumable ingestion from an explicit audited six-entity works snapshot.
- Remove `person_related_works`; resolve authors and groups against Kahi without
  name-only person matching or source-database mutations.
- Preserve existing non-empty metadata and reuse `bibliographic_info` exactly as
  enriched by the Yuku snapshot.
