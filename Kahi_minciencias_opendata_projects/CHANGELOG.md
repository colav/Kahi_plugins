# Changelog

## 0.1.3-beta - 2026-11-15

- Template fields the snapshot does not carry (`primary_topic`, `topics`,
  `topic_text`, added in Kahi 0.1.7) start from the empty template instead of
  failing; a re-import keeps the topics already inferred

## 0.1.2-beta - 2026-11-06

- Restore an explicit isolated `legacy` reader for `gruplac_production_data`.
- Resolve a COD_RH shared by several people through strict name
  compatibility; fail only when the match is ambiguous.
- Build snapshot documents in KahiBase template field order.
- Declare runtime dependencies.

## 0.1.1-beta - 2026-09-09

- Replace the legacy GrupLAC parser with bounded, resumable ingestion from an
  explicit audited six-entity projects snapshot.
- Resolve authors and groups against Kahi, retain unidentified authors, exclude
  source-only evidence and preserve existing SIIU metadata.
