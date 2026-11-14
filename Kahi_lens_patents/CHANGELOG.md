# Changelog

## 0.0.1-alpha - 2026-11-14

- New plugin: match Kahi patents with Lens records (number, family, exact
  title, fuzzy title, inventor + title) and add the Lens ID, URL and
  abstracts (one per language, inverted index). Never inserts Lens-only
  patents; audit in `lens_patents_matches`.
