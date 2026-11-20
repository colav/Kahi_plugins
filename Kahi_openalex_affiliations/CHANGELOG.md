# Changelog

## 0.1.5 - 2026-11-12

- Skip empty OpenAlex ids (`null`, `''`) so no `external_ids`/`external_urls`
  entries without value are written (117 k `wikidata: null` in the Nov 2026 cut)

## 0.1.4 - 2026-11-06

- Write updated entries in KahiBase field order (source, time)

- Report START/progress/DONE with kahi ProgressLogger
  (nuevos/actualizados/existentes); joblib verbose=0; close source
  cursor
- Require kahi>=0.1.3
