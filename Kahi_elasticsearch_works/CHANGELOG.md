# Changelog

## Unreleased - 2026-11-06

- Fix: the last partial bulk batch was never sent when the final records
  had no title; flush pending entries at the end (with test)
- Report START/progress/DONE with kahi ProgressLogger for
  bulk_insert/delete (nuevos/omitidos); optional progress_interval,
  silent with verbose 0
- Require kahi>=0.1.3a0
