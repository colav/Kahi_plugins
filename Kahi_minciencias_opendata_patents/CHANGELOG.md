# Changelog

## 0.1.3-beta - 2026-11-14

- Build snapshot documents from the full Kahi 0.1.5 patent template: fields
  the snapshot does not carry (`abstracts`, `primary_topic`, `topics`) start
  empty instead of raising `KeyError`; re-imports keep the values written by
  Lens and postcalculations.

## 0.1.2-beta - 2026-11-06

- Restore an explicit isolated `legacy` reader for `gruplac_production_data`.
- Resolve a COD_RH shared by several people through strict name
  compatibility; fail only when the match is ambiguous.
- Build snapshot documents in KahiBase template field order.
- Declare runtime dependencies.

## 0.1.1-beta - 2026-09-09

- Consume only the audited six-entity ScienTI patents snapshot.
- Resolve patent authors and research groups to canonical Kahi identifiers.
- Add deterministic, checkpointed imports with immutable release evidence.
- Remove the legacy `gruplac_production_data` and Elasticsearch ingestion path.
