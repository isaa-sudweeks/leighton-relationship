# Leighton audit follow-up action log

Date: 2026-07-28 (America/Denver)

Scope: Read and validate the 14 findings in the dated code-review document,
recompute the current median Leighton ratio, test supported corrections and
bounded sensitivities, and preserve reproducible evidence. No source dataset,
notebook, scraper, or analysis module was overwritten by this follow-up.

## Evidence inspected

1. Extracted all paragraphs and tables from
   `output/doc/Leighton_Relationship_Code_Review_2026-07-28.docx` with
   `python-docx`. The first PDF text-extraction attempt could not run because
   `pdftotext` is not installed; the DOCX is the same dated review artifact and
   supplied the complete 14-item finding list.
2. Inspected `README.MD`, `leighton_relationship_analysis.py`,
   `Scrapers/scraper.py`, `sr_ci_filter.py`, all current tests, AQS manifests,
   monitor inventory, analysis-ready schema, existing summaries, and the
   archived calculation notebook.
3. Recorded SHA-256 hashes for all inputs used by the reproduction in
   `run_record.json`.
4. Confirmed that the repository had pre-existing tracked and untracked user
   changes before this work. Those files were not reverted, staged, or
   overwritten.

## Validation actions and outcomes

1. Ran `.venv/bin/python -m unittest discover -s tests -v`.
   Result: 17 tests passed.
2. Profiled the analysis-ready table.
   Result: 21,888 hourly rows; no duplicate UTC or Local Standard Time keys;
   explicit NO POC 2, NO2 POC 3, O3 POC 1, SR POC 1, and Temp POC 1.
3. Reproduced the current analysis with the stored Hawthorne AQS snapshot and
   Hawthorne UV column.
   Result: 74 finite ratios; median 1.9834238484; p10 0.9444418197; p90
   4.9582261699.
4. Independently recomputed the ratio from the stated constants and unit
   conversions.
   Result: maximum absolute difference from the pipeline was 0.0.
5. Tested UV shifts from -3 through +3 hours against collocated AQS solar
   radiation.
   Result: 0 hours had the highest Pearson correlation, 0.9946436097, over 231
   paired rows. The valid shift was selected by this criterion, not by which
   shift placed LR nearest 1.
6. Reconciled the stored SR/clearing-index subset against the corrected
   74-row calculation.
   Result: all 32 stored rows matched and the maximum absolute LR difference
   was 0.0; median 2.3552818968. The external clearing-index archive was not
   re-downloaded.
7. Tested the archived `J = C x UV` convention by removing the undocumented
   `10 / 2.84` multiplier from the same rows.
   Result: median 0.5632923730 on 74 rows and 0.6689000587 on the 32-row
   SR/CI subset. These are sensitivity results only because the repository
   lacks instrument documentation establishing either calibration convention.
8. Tested the current Hawthorne MDL policy against a raw-value plus positivity
   sensitivity.
   Result: the median remained 1.9834238484. All 45 selected NO observations
   below the 0.05 ppb MDL were -0.2, -0.1, or 0.0 ppb and were removed by the
   positivity check. This does not validate positivity-only censoring in
   general.
9. Tested all stored NO POCs (1, 2, 3) against stored NO2 POCs (1, 3) after
   the same cleaning and join.
   Result: viable combinations produced medians from 1.8395479296 to
   2.9538324870; two combinations using NO POC 1 retained zero rows. The
   repository does not contain a scientific policy proving which combination
   should be preferred.
10. Tested pressure sensitivity at 80, 87, and 95 kPa.
    Result: same-row medians were 2.1569734351, 1.9834238484, and 1.8163986822.
    These are sensitivity bounds, not observed-pressure corrections.
11. Ran `git diff --check`.
    Result: passed.
12. Built, validated, and rendered the complete technical report artifact with
    the five-scenario comparison chart and 14-finding evidence table. A final
    count check corrected the report heading to five closed, six partial, and
    three blocked findings.

## Reproduction-script corrections

1. First execution failed because the repository root was not on `PYTHONPATH`.
   Subsequent recorded runs used `PYTHONPATH=.`.
2. The next execution exposed a legitimate zero-row POC scenario. The script
   was corrected to record a null median for empty populations rather than
   aborting.
3. The final script completed and generated all CSV and JSON evidence files.

## Generated evidence

- `reproduce_audit.py`: exact calculation and validation logic.
- `run_record.json`: input hashes, row accounting, reconciliation checks,
  scenario results, unresolved inputs, and the 14-finding matrix.
- `scenario_results.csv`: main supported and sensitivity scenarios.
- `uv_shift_sensitivity.csv`: all tested timestamp shifts.
- `poc_selection_sensitivity.csv`: all stored NO/NO2 POC combinations.
- `pressure_sensitivity.csv`: bounded fixed-pressure sensitivity.
- `finding_matrix.csv`: status and evidence for each audit finding.

## Explicitly not performed

- No live AQS collection was needed; the immutable snapshot was used.
- The external clearing-index archive was not fetched again; the stored
  selection was reconciled row-for-row to the current calculation.
- No calibration convention, POC policy, observed pressure, sub-hourly
  aggregation result, or uncertainty model was invented to force LR toward 1.

## Full-overlap SR/CI analysis

Follow-up requested after the May-only sensitivity:

1. Profiled the complete local overlap. AQS spans 2024-01-01 through
   2026-06-30; Hawthorne UV spans 2023-04-20 through 2025-10-09; their exact
   hourly overlap is 2024-01-01 through 2025-10-09 (15,552 rows).
2. Tested UV shifts from -3 through +3 hours over the full overlap. Zero hours
   had the highest UV-SR Pearson correlation: 0.965119 over 7,723 paired rows.
3. Downloaded the historical IEM AFOS clearing-index archive read-only and
   cached the Northern Wasatch Front records locally for reproducible reruns.
4. Applied UV > 10, usable chemistry/temperature/SR, SR >= 710 W/m2, CI <=
   1000, and exclusion of qualified lower bounds such as `1000+`.
5. Retained 202 hourly rows from 2024-03-08 12:00 through 2025-09-29 12:00.
   Every retained timestamp was between 09:00 and 15:00 Local Standard Time.
6. Removed the factor with
   `LR_without_10_over_2_84 = LR / (10/2.84)`.
7. Result: mean 0.6707160579; median 0.5259059119; p10 0.1485107386; p90
   1.5775520369; minimum 0.0676630801; maximum 2.5044933988.
8. Generated a two-panel static plot. The hourly panel uses a log y-axis to
   preserve the full range; the monthly panel leaves months without retained
   observations as visible gaps rather than connecting across them.

Generated files:

- `analyze_full_sr_ci_without_multiplier.py`
- `full_available_sr_ci_summary.json`
- `full_available_sr_ci_without_multiplier.parquet`
- `full_available_ci_history_air_shed_5.parquet`
- `full_available_sr_ci_leighton_ratio_without_10_over_2_84.png`

## Plot-suspicion diagnostic

Read-only checks performed after the user questioned the plotted pattern:

1. Reopened the final 202-row parquet and the rendered PNG; no analysis logic
   or source data were changed.
2. Counted 78 distinct retained dates across 11 represented months. Monthly
   sample sizes range from 6 to 32 rows, and 48 of the 78 dates contribute at
   most two hourly rows.
3. Reconstructed CI disposition for all 731 rows that passed the SR filter:
   202 retained numeric CI values at or below 1000, 380 excluded `1000+`
   qualified lower bounds, and 149 rows with missing CI. There were no
   unqualified numeric CI values above 1000.
4. Verified retained hours are 09:00 through 15:00 Local Standard Time. This
   does not show the physically impossible nighttime-SR anomaly seen in a
   separate prior dataset.
5. Quantified the LR distribution: 40/202 (19.8%) exceed unity, 23 exceed 1.5,
   and 6 exceed 2.0. The ten largest observations account for 15.56% of the
   sum used by the arithmetic mean.
6. Tested the relationship to the NO denominator. LR versus NO has Pearson
   correlation -0.6421 and rank correlation -0.9244. All 15 largest LR values
   use NO = 0.1 ppb. The 28 rows at NO = 0.1 ppb have median LR 1.7139,
   whereas the lowest-NO quartile (NO <= 0.2 ppb) has median 1.4131 and the
   highest-NO quartile (NO > 1.7 ppb) has median 0.2126.
7. Verified those NO = 0.1 ppb values are stored raw values, are above the
   recorded 0.05 ppb detection limit, and are not marked below-MDL or
   quality-flagged. This supports sensitivity to a small denominator but does
   not establish that the measurements are erroneous.
8. Checked method codes and POCs by represented month. They are constant in
   this plotted subset (NO 699/POC 2, NO2 256/POC 3, O3 087, SR 011,
   temperature 040), so the visible month-to-month shifts are not explained by
   a method-code or selected-POC change in the stored data.
