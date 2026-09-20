# Callum review package audit — 2026-09-20

This directory contains SHA-256 manifests for the regenerated 2024 and 2025
Hawthorne Leighton-analysis outputs. Each year pairs the unfiltered primary
analysis with the SR/clearing-index sensitivity analysis. No atmospheric-
chemistry interpretation is included.

## Regenerated analyses

| Year | Analysis | Retained rows | Positive inferred HO2 | LR regime counts (`<1`, `1–1.5`, `1.5–2`, `>=2`) |
| --- | --- | ---: | ---: | --- |
| 2024 | Unfiltered | 758 | 196 | 562, 100, 63, 33 |
| 2024 | SR/CI | 87 | 36 | 51, 14, 13, 9 |
| 2025 | Unfiltered | 403 | 237 | 166, 141, 65, 31 |
| 2025 | SR/CI | 44 | 37 | 7, 15, 15, 7 |

Each analysis contains 16 requested condition relationships, a correlation
table with pairwise-valid counts and Pearson/Spearman statistics, an oxidative-
regime summary, and the inferred-HO2 diagnostic. Clearing-index relationships
in the unfiltered analyses correctly report no valid pairs because clearing
index is joined only for the SR/CI sensitivity analyses.

## Verification

- The complete unit-test suite passed: 63 tests.
- `git diff --check` passed.
- `manifest_2024.json` and `manifest_2025.json` each inventory 59 files.
- All 118 manifest entries were checked for existence, byte count, and SHA-256
  equality.
- Analysis Parquet rows, hourly-diagnostics rows, summary row accounting, and
  oxidative-regime counts reconcile for all four analyses.
- Representative LR-condition, equivalent-RO2, missing-data, and inferred-HO2
  figures were visually inspected after regeneration.

## Scope limitations

- The 14.9% LR uncertainty remains a partial provisional budget, not a complete
  monitor-specific uncertainty estimate.
- Monitor-specific AQS QA values have not been retrieved and verified.
- The pinned Hawthorne source snapshot has no PM2.5 or other designated smoke
  indicator; adding one requires a separately documented source expansion.
- A corrected Leighton relationship including inferred HO2 is not reported
  because its definition still requires Callum/Jaron clarification.
