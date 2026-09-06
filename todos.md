# Callum's Leighton Relationship To-Do List

This checklist consolidates Callum Flowerday's requests from the August 31,
September 2, and September 4, 2026 `Leighton Relationship Update` emails.

## Analysis foundations and full-period run

- [ ] Use 14.9% as the provisional total relative LR uncertainty in plots and
  outputs, calculated point by point so the absolute uncertainty reflects each
  observation (especially the increased sensitivity at low NO).
- [ ] Replace the older NO + O3 Arrhenius expression with the JPL Evaluation
  19-5 recommended rate expression.
- [ ] Incorporate the JPL uncertainty for the NO + O3 rate term if it can be
  done cleanly; otherwise retain the provisional 8% kinetic-term uncertainty.
- [ ] Flag observations whose SZA is outside the validated range of the
  TUV-based UV-to-J conversion instead of silently extrapolating.
- [ ] Run the full available Hawthorne dataset after the requested calculation
  and diagnostics changes are complete.
- [ ] Keep the SR/clearing-index-filtered analysis as a separate clear-sky
  sensitivity analysis, and provide an unfiltered comparison rather than using
  SR/CI filtering to define the primary dataset.

## Annual diagnostics dataset

- [x] Export a compact hourly diagnostics table for every retained observation,
  containing local and UTC timestamps, NO, NO2, O3, NOx, UV, solar radiation,
  J(NO2), SZA, temperature, clearing index, LR, and log10(LR). The implemented
  monthly artifact is `hourly_diagnostics_<year>_<month>.parquet`; NOx is
  derived as `NO + NO2`, and clearing index is null when it was not joined.
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

- [ ] Produce monthly hourly LR plots with uncertainty and, if practical, a
  full-year hourly LR plot.
- [ ] Produce monthly hourly log10(LR) plots and, if practical, a full-year
  hourly log10(LR) plot.
- [ ] Plot LR against NO, NO2, O3, NOx, J, UV, solar radiation, SZA,
  temperature, and clearing index using individual retained observations.
- [ ] Plot equivalent ROx against NOx, temperature, UV, solar radiation, J,
  and clearing index using individual retained observations.
- [ ] Create a correlation/statistics table for both LR and equivalent ROx
  against the requested predictors, including at least sample count, Pearson
  correlation, Spearman correlation, and appropriate p-values.
- [ ] Retain separate non-SR/CI-filtered versions of representative plots for
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
