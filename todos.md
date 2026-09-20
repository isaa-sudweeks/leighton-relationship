# Callum's Leighton Relationship To-Do List

This checklist consolidates Callum Flowerday's requests from the August 31,
September 2, September 4, and September 16, 2026 `Leighton Relationship
Update` emails.

Status was reconciled against `origin/main` at merge commit `5eb289f` on
September 19, 2026. The merged work completes the provisional uncertainty and
JPL kinetics update, documented single-day TUV/SZA QC, available-observation
annual runs, and the first requested LR plots. The current working tree adds
the confirmed downstream excess-oxidation and radical-equivalent diagnostics.

## September 16 clarification

- Callum confirmed that the three excess-NO2-production quantities requested
  on September 2 are the intended quantities and should all be saved in the
  diagnostics table.
- The temperature-dependent NO + O3 expression shown in the email is the
  current JPL Evaluation 20 recommendation and supersedes the different
  parameterization previously present in the repository.
  Callum also supplied JPL-20 uncertainty-factor information that should be
  retained as provenance; the current 8% provisional kinetic uncertainty
  remains in the combined LR budget pending a separately reviewed change.
- The generalized `ROx_equiv` diagnostic must use Callum's published generic
  organic RO2 + NO coefficient. It must not use the HO2 + NO coefficient.
- Jaron's inferred-HO2 diagnostic is separate. Evaluate its JPL HO2 + NO rate
  coefficient at each observation's temperature, then save both the evaluated
  coefficient and inferred HO2 concentration.
- Preserve the signed excess-production result for inspection. Populate the
  reported equivalent-RO2 and inferred-HO2 concentrations only where the
  stated positive-excess condition is satisfied.

## Next recommended task

- [ ] Build the requested condition plots and correlation/statistics table from
  the now-complete hourly diagnostics schema. Plot LR against every requested
  predictor and equivalent RO2 against NOx, temperature, UV, solar radiation,
  J, and clearing index; report sample count, Pearson and Spearman correlation,
  and p-values with missing-value handling documented.

## Analysis foundations and full-period run

- [x] Use 14.9% as the provisional total relative LR uncertainty in plots and
  outputs, calculated point by point so the absolute uncertainty reflects each
  observation. Implemented as `abs(LR) * 0.149`, with the supplied component
  budget retained in output metadata.
- [x] Use the JPL Evaluation 20 NO + O3 expression confirmed in Callum's
  September 16 email: `3.0e-12 * exp(-1500 / T)`.
- [x] Incorporate the JPL uncertainty for the NO + O3 rate term if it can be
  done cleanly; otherwise retain the provisional 8% kinetic-term uncertainty.
  The merged provisional budget explicitly retains the permitted 8% term.
- [x] Flag observations whose SZA is outside the documented support range of
  the TUV-based UV-to-J conversion instead of silently extrapolating. Callum's
  August 14 handoff documents six May 21 TUV calculations spanning 21.0–49.8°.
  Rows outside that inclusive range remain available and are flagged. The
  transfer function remains explicitly provisional and single-day.
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
- [x] Calculate and save Callum's three confirmed excess-NO2-production
  quantities for each retained hour, using the equations embedded in his
  September 16 reply (repeated from September 2). These quantify NO-to-NO2
  production not explained by the classical NO + O3 pathway.
- [x] Calculate `ROx_equiv` with the effective ROx + NO rate coefficient used
  for this diagnostic, reporting it only where the inferred excess production
  satisfies Callum's stated positivity condition. Per Callum's September 16
  clarification, use the published generic organic RO2 + NO coefficient, not
  the HO2 + NO coefficient.
- [x] Calculate and save fractional excess oxidation: the fraction of inferred
  NO-to-NO2 oxidation beyond the classical O3 pathway.
- [x] Add Boolean columns `LR_gt_1p5` and `LR_gt_2`.
- [x] Add a Boolean flag identifying observations robustly above LR = 1 after
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

- [x] Implement Jaron's specific HO2 calculation using the derivation and JPL
  HO2 + NO rate expression repeated in Callum's September 16 email. Evaluate
  the coefficient from each observation's temperature rather than at 298 K.
- [x] Save the inferred HO2 concentration for each hour where the calculated
  excess is positive, together with the evaluated coefficient/rate value used.
- [x] Preserve the signed excess-production numerator so negative inferred
  results remain inspectable rather than being silently discarded.
- [ ] Plot the inferred HO2 diagnostic and/or the corrected Leighton
  relationship including the inferred HO2 contribution.

## Final figures and review

- [ ] Give Callum the first full-period diagnostics outputs and plots for
  scientific review before finalizing the presentation.
- [ ] After review, rerun the analysis with any agreed changes.
- [ ] Format final paper figures to the target journal's standards, including
  removing plot titles where required.
