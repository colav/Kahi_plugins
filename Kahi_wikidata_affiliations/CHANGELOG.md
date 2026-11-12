# Changelog

## 0.1.3-beta - 2026-11-12

- Follow the redirects recorded in `wikidata.redirects` by the wikidata_capture
  DAG when the QID reported by ROR was merged into another item
  (`redirects_collection_name`, default `redirects`)

## 0.1.2-beta - 2026-11-06

- Build URL-encoded Wikimedia Commons logo URLs (fixes 'https: //' and
  'Special: Redirect')
- Reprocess without duplicating names, replacing the previous Wikidata
  logo and refreshing its update time
- Add tests

- Report START/progress/DONE with kahi ProgressLogger
  (actualizados/sin_registro_wikidata); joblib verbose=0
- Require kahi>=0.1.3
