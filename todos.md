# Callum's Leighton Relationship To-Do List

This checklist consolidates Callum Flowerday's requests from the August 31,
September 2, and September 4, 2026 `Leighton Relationship Update` emails.

Status was reconciled against `main` at merge commit `f01331b` on September 6,
2026. The merge completed the provisional uncertainty and JPL kinetics update,
SZA QC plumbing, available-observation annual runs, and the first requested LR
plots. It did not implement the downstream excess-oxidation diagnostics.

## Next recommended task

- [ ] Confirm the actual SZA limits of the underlying TUV calibration grid with
  Callum or its source artifact, then replace the provisional 19.74–50.47°
  reviewed-May observation span used by the current QC flag. The code already
  preserves and flags the bounds without dropping extrapolated rows, but the
  present interval must not be described as the validated TUV range.

## Analysis foundations and full-period run

- [x] Use 14.9% as the provisional total relative LR uncertainty in plots and
  outputs, calculated point by point so the absolute uncertainty reflects each
  observation. Implemented as `abs(LR) * 0.149`, with the supplied component
  budget retained in output metadata.
- [x] Replace the older NO + O3 Arrhenius expression with the JPL Evaluation
  19-5 C19 non-Arrhenius rate expression.
- [x] Incorporate the JPL uncertainty for the NO + O3 rate term if it can be
  done cleanly; otherwise retain the provisional 8% kinetic-term uncertainty.
  The merged provisional budget explicitly retains the permitted 8% term.
- [ ] Flag observations whose SZA is outside the validated range of the
  TUV-based UV-to-J conversion instead of silently extrapolating. Flagging is
  implemented, but the actual TUV grid range still needs confirmation; current
  bounds are explicitly labeled as provisional.
- [x] Run the available Hawthorne observations for calendar years 2024 and
  2025 after the requested calculation and diagnostics changes. Coverage
  metadata explicitly reports missing months rather than claiming complete
  full-year coverage.
- [x] Keep the SR/clearing-index-filtered analysis as a separate clear-sky
  sensitivity analysis, and provide an unfiltered comparison rather than using
  SR/CI filtering to define the primary dataset. Separate 2024 and 2025 output
  directories were generated for both configurations.

## Annual diagnostics dataset

- [x] Export a compact hourly diagnostics table for every retained observation,
  containing local and UTC timestamps, NO, NO2, O3, NOx, UV, solar radiation,
  J(NO2), SZA, temperature, clearing index, LR, and log10(LR). The implemented
  monthly artifact is `hourly_diagnostics_<year>_<month>.parquet`; NOx is
  derived as `NO + NO2`, and clearing index is null when it was not joined.
  Completed by merge commit `487c5ce`, with schema, artifact-writing, summary,
  packaging, and regression-test coverage.
- [ ] Calculate and save Callum's three requested excess-NO2-production
  quantities for each retained hour, using the equations embedded in his
  September 2 email. These quantify NO-to-NO2 production not explained by the
  classical NO + O3 pathway.
- [ ] Calculate `ROx_equiv` with the effective ROx + NO rate coefficient used
  for this diagnostic, reporting it only where the inferred excess production
  satisfies Callum's stated positivity condition.
- [ ] Calculate and save fractional excess oxidation: the fraction of inferred
  NO-to-NO2 oxidation beyond the classical O3 pathway.
- [ ] Add Boolean columns `LR_gt_1p5` and `LR_gt_2`.
- [ ] Add a Boolean flag identifying observations robustly above LR = 1 after
  accounting for the estimated uncertainty, using Callum's email equation.
- [ ] If readily available, add PM2.5 or another defensible smoke indicator to
  the diagnostics table; otherwise document that no indicator was added.

## Diagnostic plots and statistics

- [x] Produce monthly hourly LR plots with uncertainty and, if practical, a
  full-year hourly LR plot.
- [x] Produce monthly hourly log10(LR) plots and, if practical, a full-year
  hourly log10(LR) plot.
- [ ] Plot LR against NO, NO2, O3, NOx, J, UV, solar radiation, SZA,
  temperature, and clearing index using individual retained observations.
- [ ] Plot equivalent ROx against NOx, temperature, UV, solar radiation, J,
  and clearing index using individual retained observations.
- [ ] Create a correlation/statistics table for both LR and equivalent ROx
  against the requested predictors, including at least sample count, Pearson
  correlation, Spearman correlation, and appropriate p-values.
- [x] Retain separate non-SR/CI-filtered versions of representative plots for
  comparison with the clear-sky sensitivity analysis.

## Oxidative-regime analysis

- [ ] Classify each retained observation into one of four LR regimes:
  `LR < 1`, `1 <= LR < 1.5`, `1.5 <= LR < 2`, or `LR >= 2`.
- [ ] Summarize the distributions of NO, NO2, O3, NOx, J, UV, solar radiation,
  SZA, temperature, and clearing index within each LR regime.

## Jaron's HO2 diagnostic

- [ ] Implement Jaron's specific HO2 calculation using the derivation and
  HO2 + NO rate coefficient embedded in Callum's September 4 email.
- [ ] Save the inferred HO2 concentration for each hour where the calculated
  excess is positive, together with the coefficient/rate value used.
- [ ] Preserve enough information to inspect negative inferred results rather
  than silently discarding them.
- [ ] Plot the inferred HO2 diagnostic and/or the corrected Leighton
  relationship including the inferred HO2 contribution.

## Final figures and review

- [ ] Give Callum the first full-period diagnostics outputs and plots for
  scientific review before finalizing the presentation.
- [ ] After review, rerun the analysis with any agreed changes.
- [ ] Format final paper figures to the target journal's standards, including
  removing plot titles where required.
