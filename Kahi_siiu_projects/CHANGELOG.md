# Changelog

## 0.0.3-alpha - 2026-11-15

- `abstract` is the SIIU descriptive text labelled "Resumen" (also "Resumen
  ejecutivo"), as plain text without HTML
- Without one, `topic_text` (new field of the project template in Kahi 0.1.7)
  joins the general objective and the problem statement, up to 3 000
  characters: postcalculations infers the topics from it, but it is not shown
  as the abstract. On the current SIIU cut: 2 732 projects get an abstract,
  3 562 a topic text and 3 432 only have the title

## 0.0.2-alpha - 2026-11-06

- Replace $setOnInsert with insert_only_update to preserve field order
- Stub it in the mongomock idempotency test
