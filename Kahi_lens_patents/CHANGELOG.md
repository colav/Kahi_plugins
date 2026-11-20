# Changelog

## 0.0.2-alpha - 2026-11-14

- `merge_duplicates`: patents matched to the same Lens record become one when
  they are proven to be the same patent (number or family match, author among
  the Lens inventors, shared inventor or shared owner). Deleted patents are
  kept whole in `lens_patents_merges`; `merge_dry_run` only reports.
- Number keys: Colombian file numbers padded with empty groups
  (`13-192338-00000-0000` -> `CO13192338`), short years (`9-108636` ->
  `CO09108636`) and leading zeros (`US008303926B1` -> `US8303926`).

## 0.0.1-alpha - 2026-11-14

- New plugin: match Kahi patents with Lens records (number, family, exact
  title, fuzzy title, inventor + title) and add the Lens ID, URL and
  abstracts (one per language, inverted index). Never inserts Lens-only
  patents; audit in `lens_patents_matches`.
