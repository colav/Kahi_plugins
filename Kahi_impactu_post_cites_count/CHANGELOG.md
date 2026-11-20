# Changelog

## 0.0.5-alpha - 2026-11-06

- Follow relations.id with $graphLookup so works of groups and units
  also count for their canonical parent affiliations
- Count each work once per entity even when it lists both child and
  parent; ignore empty affiliation ids
- Update README and tests
