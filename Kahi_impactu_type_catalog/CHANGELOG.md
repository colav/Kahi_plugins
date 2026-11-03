# Changelog

## 0.2.0 - 2026-10-17 (catalog 1.2.0)

- Consolidate the editorial workbook into one seven-column sheet: 800 unchanged
  executable mappings, two original unmapped rows and 180 reviewed scraping types.
- Distinguish Socrata, scraping and dumps origins; scraping always uses the
  Minciencias provider, while original coded ScienTI identities remain intact.
- Add exact parent/type scraping lookup and an explicit origin-aware API without
  changing the existing source/type lookup.
- Publish the nine reviewed event variants as Evento and the three provisional
  classifications (Programa radial, Iniciación Científica, Regulaciones y Normas)
  as Otro in their reviewed contexts.

## 0.1.0 - 2026-09-08

### Added

- Versioned ImpactU type catalog generated from the four maintained workbook
  sheets.
- Runtime loader for exact source/type lookup and Minciencias classification.
- JSON Schema, source hash, deterministic generator, and unmapped-row audit.
