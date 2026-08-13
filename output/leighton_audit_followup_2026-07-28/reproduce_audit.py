"""Reproduce the post-audit Leighton-ratio validation.

This script does not modify repository source data. It reads the immutable
Hawthorne AQS snapshot, the UV CSV, the current analysis code, and the stored
SR/clearing-index result. It writes only compact audit evidence beside itself.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from leighton_relationship_analysis import (
    AnalysisConfig,
    J_SCALE_FACTOR,
    UV_SENSOR_AREA_CM2,
    calculate_leighton_ratio,
    evaluate_uv_alignment,
    load_selected_measurements,
)
from Scrapers.scraper import build_analysis_table


ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = Path(__file__).resolve().parent
AQS_ANALYSIS = ROOT / (
    "data/downloads/Hawthorne/20240101_20260728_20260729T004631Z/"
    "aqs_analysis_ready.parquet"
)
AQS_SAMPLES = AQS_ANALYSIS.with_name("aqs_samples.parquet")
UV_FILE = ROOT / "data/UV Data/UV Data All Time HW LP RB.csv"
ANALYSIS_CODE = ROOT / "leighton_relationship_analysis.py"
AUDIT_DOC = ROOT / "output/doc/Leighton_Relationship_Code_Review_2026-07-28.docx"
SR_CI_RESULT = ROOT / (
    "output/leighton_analysis_sr_ci_uv_aligned/"
    "leighton_ratio_may_2025.parquet"
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def summarize_scenario(
    scenario: str,
    frame: pd.DataFrame,
    evidence_class: str,
    population: str,
    note: str,
) -> dict[str, object]:
    median = float(frame["LR"].median())
    return {
        "scenario": scenario,
        "evidence_class": evidence_class,
        "population": population,
        "count": int(len(frame)),
        "median_lr": median,
        "absolute_distance_from_unity": abs(median - 1.0),
        "p10": float(frame["LR"].quantile(0.10)),
        "p90": float(frame["LR"].quantile(0.90)),
        "note": note,
    }


def load_uv() -> pd.DataFrame:
    uv = pd.read_csv(UV_FILE)
    uv = uv.rename(columns={uv.columns[0]: "uv_datetime", "HW": "UV"})
    uv["uv_datetime"] = pd.to_datetime(
        uv["uv_datetime"], format="mixed", errors="coerce"
    )
    return uv[["uv_datetime", "UV"]].dropna(subset=["uv_datetime"])


def select_may_uv_rows(
    wide: pd.DataFrame,
    uv: pd.DataFrame,
    shift_hours: int,
) -> pd.DataFrame:
    work = wide.copy()
    work["datetime_local_standard"] = pd.to_datetime(
        work["datetime_local_standard"]
    )
    shifted = uv.copy()
    shifted["datetime_local_standard"] = shifted["uv_datetime"] + pd.to_timedelta(
        shift_hours, unit="h"
    )
    merged = work.merge(
        shifted[["datetime_local_standard", "uv_datetime", "UV"]],
        on="datetime_local_standard",
        how="inner",
        validate="one_to_one",
    )
    merged = merged[
        (merged["datetime_local_standard"].dt.year == 2025)
        & (merged["datetime_local_standard"].dt.month == 5)
        & merged["UV"].gt(10.0)
    ].copy()
    return merged.set_index("datetime_local_standard").sort_index()


def finite_positive_ratio(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["UV", "NO", "NO2", "O3", "Temp"]
    result = frame.dropna(subset=required).copy()
    result = result[
        result["NO"].gt(0) & result["NO2"].gt(0) & result["O3"].gt(0)
    ]
    if result.empty:
        result["LR"] = pd.Series(index=result.index, dtype=float)
        return result
    return calculate_leighton_ratio(result)


def main() -> None:
    config = AnalysisConfig(year=2025, month=5)
    clean_input, accounting = load_selected_measurements(
        AQS_ANALYSIS, UV_FILE, config
    )
    corrected = calculate_leighton_ratio(clean_input)
    independent_temp_k = (clean_input["Temp"] - 32.0) * (5.0 / 9.0) + 273.15
    independent_k = 3.0e-12 * np.exp(-1500.0 / independent_temp_k)
    independent_j = 7.784e-5 * clean_input["UV"] / 2.84 * 10.0
    independent_number_density = (
        87_000.0 / (1.380649e-23 * independent_temp_k)
    ) / 1e6
    independent_lr = (
        independent_j * (clean_input["NO2"] * 1e-9 * independent_number_density)
    ) / (
        independent_k
        * (clean_input["O3"] * 1e-6 * independent_number_density)
        * (clean_input["NO"] * 1e-9 * independent_number_density)
    )
    independent_formula_max_abs_difference = float(
        (corrected["LR"] - independent_lr).abs().max()
    )
    if not np.allclose(
        corrected["LR"], independent_lr, atol=1e-12, rtol=1e-12
    ):
        raise AssertionError("Independent LR calculation does not match pipeline")

    scenarios: list[dict[str, object]] = []
    scenarios.append(
        summarize_scenario(
            "Evidence-supported corrected pipeline",
            corrected,
            "supported",
            "Hawthorne, May 2025, UV > 10, all ratio inputs usable",
            (
                "One site; explicit POCs; row-level MDL and qualifier exclusion; "
                "minimal required columns; selected UV shift 0 h."
            ),
        )
    )

    raw = pd.read_parquet(AQS_ANALYSIS)
    uv = load_uv()
    raw_selected = select_may_uv_rows(raw, uv, int(accounting["uv_selected_shift_hours"]))
    raw_values = raw_selected.copy()
    for parameter in ["NO", "NO2", "O3", "Temp"]:
        raw_values[parameter] = raw_values[f"{parameter}_raw"]
    raw_values = raw_values[
        ~raw_values[
            [
                "NO_quality_flagged",
                "NO2_quality_flagged",
                "O3_quality_flagged",
                "Temp_quality_flagged",
            ]
        ]
        .fillna(False)
        .any(axis=1)
    ]
    raw_ratio = finite_positive_ratio(raw_values)
    scenarios.append(
        summarize_scenario(
            "Raw-positive/MDL policy sensitivity",
            raw_ratio,
            "sensitivity_only",
            "Same Hawthorne join, substituting raw values before positivity checks",
            (
                "The result is unchanged: all 45 selected NO values below the "
                "0.05 ppb MDL are -0.2, -0.1, or 0.0 ppb and are removed by "
                "the positivity check. This is not a general censoring solution."
            ),
        )
    )

    archived_j = corrected.copy()
    archived_j["LR"] = archived_j["LR"] / (
        J_SCALE_FACTOR / UV_SENSOR_AREA_CM2
    )
    scenarios.append(
        summarize_scenario(
            "Archived J = C x UV convention (sensitivity)",
            archived_j,
            "sensitivity_only",
            "Same 74 corrected rows",
            (
                "Removes the undocumented 10/2.84 multiplier. Instrument "
                "documentation is required before choosing this convention."
            ),
        )
    )

    stored_sr_ci = pd.read_parquet(SR_CI_RESULT).set_index(
        "datetime_local_standard"
    )
    shared_index = corrected.index.intersection(stored_sr_ci.index)
    if len(shared_index) != len(stored_sr_ci):
        raise AssertionError("Stored SR/CI rows are not a subset of corrected rows")
    lr_difference = (
        corrected.loc[shared_index, "LR"] - stored_sr_ci.loc[shared_index, "LR"]
    ).abs()
    if not np.allclose(lr_difference, 0.0, atol=1e-12, rtol=1e-12):
        raise AssertionError("Stored SR/CI ratios do not match current calculation")
    scenarios.append(
        summarize_scenario(
            "Corrected pipeline plus stored SR/CI selection",
            stored_sr_ci,
            "supported_stored_selection",
            "32-row subset with SR >= 710 W/m2 and CI <= 1000",
            (
                "Chemistry and LR values reconcile exactly to the corrected run; "
                "the external CI archive was not re-downloaded in this validation."
            ),
        )
    )
    stored_sr_ci_archived_j = stored_sr_ci.copy()
    stored_sr_ci_archived_j["LR"] = stored_sr_ci_archived_j["LR"] / (
        J_SCALE_FACTOR / UV_SENSOR_AREA_CM2
    )
    scenarios.append(
        summarize_scenario(
            "SR/CI subset plus archived J convention (sensitivity)",
            stored_sr_ci_archived_j,
            "sensitivity_only",
            "Same 32 stored SR/CI rows",
            "Combines a supported row selection with an unvalidated J convention.",
        )
    )

    pressure_rows: list[dict[str, object]] = []
    for pressure_pa in [80_000.0, 87_000.0, 95_000.0]:
        median = float(corrected["LR"].median() * 87_000.0 / pressure_pa)
        pressure_rows.append(
            {
                "pressure_pa": pressure_pa,
                "median_lr": median,
                "absolute_distance_from_unity": abs(median - 1.0),
                "evidence_class": (
                    "current_assumption"
                    if pressure_pa == 87_000.0
                    else "sensitivity_only"
                ),
            }
        )

    shift_rows: list[dict[str, object]] = []
    aqs_for_alignment = pd.read_parquet(AQS_ANALYSIS)
    aqs_for_alignment["datetime_local_standard"] = pd.to_datetime(
        aqs_for_alignment["datetime_local_standard"]
    )
    selected_shift, shift_diagnostics = evaluate_uv_alignment(
        aqs_for_alignment, uv, config
    )
    for diagnostic in shift_diagnostics:
        shift = int(diagnostic["shift_hours"])
        shift_config = AnalysisConfig(year=2025, month=5, uv_hour_shift=shift)
        shift_input, _ = load_selected_measurements(
            AQS_ANALYSIS, UV_FILE, shift_config
        )
        shift_ratio = calculate_leighton_ratio(shift_input)
        shift_rows.append(
            {
                **diagnostic,
                "analysis_rows": int(len(shift_ratio)),
                "median_lr": float(shift_ratio["LR"].median()),
                "absolute_distance_from_unity": abs(
                    float(shift_ratio["LR"].median()) - 1.0
                ),
                "selected_by_uv_sr_correlation": shift == selected_shift,
            }
        )

    samples = pd.read_parquet(AQS_SAMPLES)
    poc_rows: list[dict[str, object]] = []
    for no_poc in [1, 2, 3]:
        for no2_poc in [1, 3]:
            selections = {
                "NO": no_poc,
                "NO2": no2_poc,
                "O3": 1,
                "SR": 1,
                "Temp": 1,
            }
            wide, selection_accounting = build_analysis_table(samples, selections)
            selected_rows = select_may_uv_rows(wide, uv, selected_shift)
            ratio = finite_positive_ratio(selected_rows)
            median_lr = float(ratio["LR"].median()) if len(ratio) else None
            no_account = selection_accounting[
                selection_accounting["parameter_abbreviation"].eq("NO")
            ].iloc[0]
            no2_account = selection_accounting[
                selection_accounting["parameter_abbreviation"].eq("NO2")
            ].iloc[0]
            poc_rows.append(
                {
                    "no_poc": no_poc,
                    "no_method_code": str(no_account["method_code"]),
                    "no_mdl_ppb": float(no_account["detection_limit"]),
                    "no2_poc": no2_poc,
                    "no2_method_code": str(no2_account["method_code"]),
                    "no2_mdl_ppb": float(no2_account["detection_limit"]),
                    "analysis_rows": int(len(ratio)),
                    "median_lr": median_lr,
                    "absolute_distance_from_unity": (
                        abs(median_lr - 1.0) if median_lr is not None else None
                    ),
                    "current_selection": no_poc == 2 and no2_poc == 3,
                    "same_method_code": str(no_account["method_code"])
                    == str(no2_account["method_code"]),
                }
            )

    below_mdl_counts = {
        parameter: {
            "uv_threshold_rows": int(len(raw_selected)),
            "below_mdl_rows": int(
                raw_selected[f"{parameter}_below_mdl"].fillna(False).sum()
            ),
            "quality_flagged_rows": int(
                raw_selected[f"{parameter}_quality_flagged"].fillna(False).sum()
            ),
        }
        for parameter in ["NO", "NO2", "O3", "Temp"]
    }

    finding_matrix = [
        {
            "finding": 1,
            "audit_issue": "Mixed monitoring sites",
            "current_status": "closed_for_current_analysis",
            "median_test": "included",
            "evidence": "Hawthorne AQS site 49-035-3006 is joined only to UV column HW.",
        },
        {
            "finding": 2,
            "audit_issue": "AQS parameter request limit and payload validation",
            "current_status": "closed_in_current_scraper",
            "median_test": "no_direct_effect_on_frozen_snapshot",
            "evidence": "Six parameters are split into 5+1 batches; payload errors are tested.",
        },
        {
            "finding": 3,
            "audit_issue": "Below-MDL denominator values",
            "current_status": "partially_closed",
            "median_test": "included",
            "evidence": (
                "Row-level MDLs are retained and excluded. In the UV-threshold "
                "population, 45/123 selected NO values are below MDL."
            ),
        },
        {
            "finding": 4,
            "audit_issue": "Timezone and daylight-saving mismatch",
            "current_status": "partially_closed",
            "median_test": "included",
            "evidence": (
                "AQS UTC and Local Standard Time are preserved; UV shift 0 h has "
                "the highest tested UV-SR correlation (r=0.994644). UV metadata "
                "still lacks an explicit timezone contract."
            ),
        },
        {
            "finding": 5,
            "audit_issue": "Loss of POC, method, MDL, and qualifier provenance",
            "current_status": "closed_for_current_snapshot",
            "median_test": "included",
            "evidence": (
                "Long-form samples retain provenance; the analysis table uses "
                "explicit POCs and preserves raw/audit columns."
            ),
        },
        {
            "finding": 6,
            "audit_issue": "Destructive or non-transactional writes",
            "current_status": "partially_closed",
            "median_test": "no_direct_effect_on_frozen_snapshot",
            "evidence": (
                "AQS downloads are immutable dated snapshots and derived writes "
                "are atomic; the legacy Synoptic path was not revalidated here."
            ),
        },
        {
            "finding": 7,
            "audit_issue": "Undocumented J(NO2) calibration",
            "current_status": "open_blocker",
            "median_test": "sensitivity_only",
            "evidence": (
                "Current multiplier gives median 1.983424; removing 10/2.84 gives "
                "0.563292 on the same 74 rows."
            ),
        },
        {
            "finding": 8,
            "audit_issue": "Broad dropna and hidden filters",
            "current_status": "closed_for_current_analysis",
            "median_test": "included",
            "evidence": (
                "Only UV, NO, NO2, O3, and Temp are required; row counts and "
                "thresholds are recorded."
            ),
        },
        {
            "finding": 9,
            "audit_issue": "Overlapping and mislabeled clock windows",
            "current_status": "closed_for_clock_split",
            "median_test": "does_not_change_all_row_median",
            "evidence": "The current split is non-overlapping and exhaustive; unit test passes.",
        },
        {
            "finding": 10,
            "audit_issue": "Hourly means in a nonlinear ratio",
            "current_status": "open_data_blocker",
            "median_test": "not_testable",
            "evidence": (
                "Stored AQS measurements and UV are hourly; no colocated "
                "sub-hourly source is available in the repository."
            ),
        },
        {
            "finding": 11,
            "audit_issue": "Fixed pressure and no propagated uncertainty",
            "current_status": "open_data_and_method_blocker",
            "median_test": "sensitivity_only",
            "evidence": (
                "At 80, 87, and 95 kPa the same-row medians are 2.156973, "
                "1.983424, and 1.816399; observed hourly pressure is absent."
            ),
        },
        {
            "finding": 12,
            "audit_issue": "Stale notebooks and irreproducible artifacts",
            "current_status": "partially_closed",
            "median_test": "supports_reproducibility_only",
            "evidence": (
                "The Python entry point, hashes, row accounting, tests, and this "
                "reproduction record exist; current analysis files remain uncommitted."
            ),
        },
        {
            "finding": 13,
            "audit_issue": "Credential, TLS, and dependency handling",
            "current_status": "partially_closed",
            "median_test": "no_direct_effect_on_frozen_snapshot",
            "evidence": (
                "Current scraper requires environment credentials, uses verified "
                "HTTPS defaults, and dependencies are declared; rotation cannot "
                "be verified from repository evidence."
            ),
        },
        {
            "finding": 14,
            "audit_issue": "SR/CI filter validation and qualified values",
            "current_status": "partially_closed",
            "median_test": "included_as_separate_population",
            "evidence": (
                "Qualified lower bounds are excluded and tested. Stored 32-row "
                "selection reconciles exactly; no solar-elevation guard exists."
            ),
        },
    ]

    source_files = [
        AQS_ANALYSIS,
        AQS_SAMPLES,
        UV_FILE,
        ANALYSIS_CODE,
        AUDIT_DOC,
        SR_CI_RESULT,
    ]
    run_record = {
        "analysis_question": (
            "Does applying the audit-supported corrections move the median "
            "Leighton ratio closer to 1?"
        ),
        "metric": "absolute_distance_from_unity = abs(median(LR) - 1)",
        "data_period": "May 2025",
        "time_basis": "AQS Local Standard Time; UV integer-hour alignment tested",
        "source_hashes_sha256": {
            str(path.relative_to(ROOT)): sha256(path) for path in source_files
        },
        "selected_uv_shift_hours": selected_shift,
        "row_accounting": accounting,
        "below_mdl_and_quality_counts_after_uv_threshold": below_mdl_counts,
        "stored_sr_ci_reconciliation": {
            "stored_rows": int(len(stored_sr_ci)),
            "matched_corrected_rows": int(len(shared_index)),
            "maximum_absolute_lr_difference": float(lr_difference.max()),
        },
        "independent_formula_cross_check": {
            "rows": int(len(corrected)),
            "maximum_absolute_lr_difference": independent_formula_max_abs_difference,
        },
        "finding_matrix": finding_matrix,
        "scenario_results": scenarios,
        "pressure_sensitivity": pressure_rows,
        "uv_shift_sensitivity": shift_rows,
        "poc_selection_sensitivity": poc_rows,
        "unresolved_inputs": [
            "Documented UV instrument output units and calibration to J(NO2) in s^-1",
            "Scientifically approved POC/method selection policy",
            "Observed pressure aligned to each chemistry observation",
            "Sub-hourly colocated chemistry and photolysis data",
            "Uncertainty model for calibration, measurement error, and censoring",
        ],
    }

    pd.DataFrame(scenarios).to_csv(OUTPUT_DIR / "scenario_results.csv", index=False)
    pd.DataFrame(shift_rows).to_csv(
        OUTPUT_DIR / "uv_shift_sensitivity.csv", index=False
    )
    pd.DataFrame(poc_rows).to_csv(
        OUTPUT_DIR / "poc_selection_sensitivity.csv", index=False
    )
    pd.DataFrame(pressure_rows).to_csv(
        OUTPUT_DIR / "pressure_sensitivity.csv", index=False
    )
    pd.DataFrame(finding_matrix).to_csv(
        OUTPUT_DIR / "finding_matrix.csv", index=False
    )
    (OUTPUT_DIR / "run_record.json").write_text(
        json.dumps(run_record, indent=2) + "\n", encoding="utf-8"
    )

    print(pd.DataFrame(scenarios).to_string(index=False))
    print("\nUV shift sensitivity")
    print(pd.DataFrame(shift_rows).to_string(index=False))
    print("\nPOC sensitivity")
    print(pd.DataFrame(poc_rows).to_string(index=False))
    print("\nPressure sensitivity")
    print(pd.DataFrame(pressure_rows).to_string(index=False))


if __name__ == "__main__":
    main()
