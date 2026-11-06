# Changelog

## Unreleased - 2026-11-06

- Store official sectionals as campuses of their principal affiliation
  instead of separate affiliations
- Require distinctive tokens and reject candidates with another SNIES
  code in name matching
- Repair unrelated SNIES codes previously fused into a ROR affiliation
- Ignore malformed or placeholder NITs and websites; mark SNIES names as
  Spanish
- Process sequentially, principals before sectionals; drop
  joblib/num_jobs
- Add tests and README notes

- Report START/progress/DONE with kahi ProgressLogger (repaired fusions,
  linked sectionals)
- Require kahi>=0.1.3a0
