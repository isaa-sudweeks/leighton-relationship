import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from leighton_relationship_analysis import (
    AnalysisConfig,
    DEFAULT_AQS_PATH,
    DEFAULT_TUV_QC_SZA_MAX_DEG,
    DEFAULT_TUV_QC_SZA_MIN_DEG,
    GENERIC_RO2_NO_ACTIVATION_OVER_R_K,
    GENERIC_RO2_NO_PREFACTOR,
    JPL_HO2_NO_ACTIVATION_OVER_R_K,
    JPL_HO2_NO_PREFACTOR,
    JPL_NO_O3_ACTIVATION_OVER_R_K,
    JPL_NO_O3_PREFACTOR,
    TUV_SZA_INTERCEPT_M2_W_S,
    TUV_SZA_SLOPE_M2_W_S_DEG,
    apply_no_threshold,
    build_hourly_diagnostics,
    calculate_leighton_ratio,
    calculate_solar_zenith_angle,
    calculate_tuv_j_no2,
    clear_stale_analysis_artifacts,
    evaluate_uv_alignment,
    plot_ho2_diagnostic,
    plot_ratio_distributions,
    period_label,
    run_analysis,
    split_time_windows,
    summarize_aqs_source_provenance,
    summarize_no_threshold_sensitivity,
)


class LeightonAnalysisTests(unittest.TestCase):
    def test_pinned_hawthorne_snapshot_has_no_pm25_indicator(self):
        result = summarize_aqs_source_provenance(DEFAULT_AQS_PATH)

        self.assertEqual(result["manifest_status"], "available")
        self.assertEqual(
            result["returned_parameter_codes"],
            ["42601", "42602", "44201", "62101", "63301"],
        )
        self.assertEqual(
            result["smoke_indicator"]["status"],
            "not_available_in_source_snapshot",
        )
        self.assertEqual(
            result["smoke_indicator"]["available_pm25_parameter_codes"], []
        )

    def test_source_provenance_records_absent_pm25_without_proxy_inference(self):
        with tempfile.TemporaryDirectory() as temporary:
            source_dir = Path(temporary)
            aqs_path = source_dir / "aqs_analysis_ready.parquet"
            (source_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "download_id": "pinned-snapshot",
                        "parameter_codes": ["42601", "88101", "63302"],
                        "returned_parameter_codes": ["42601", "63301"],
                        "missing_parameter_codes": ["88101", "63302"],
                    }
                ),
                encoding="utf-8",
            )

            result = summarize_aqs_source_provenance(aqs_path)

        self.assertEqual(result["manifest_status"], "available")
        self.assertEqual(result["download_id"], "pinned-snapshot")
        self.assertEqual(result["returned_parameter_codes"], ["42601", "63301"])
        self.assertEqual(
            result["smoke_indicator"]["status"],
            "not_available_in_source_snapshot",
        )
        self.assertFalse(result["smoke_indicator"]["added_to_diagnostics"])
        self.assertEqual(
            result["monitor_specific_aqs_qa"]["status"],
            "not_retrieved_unverified",
        )

    def test_source_provenance_does_not_claim_snapshot_coverage_without_manifest(self):
        result = summarize_aqs_source_provenance(
            Path("missing-source") / "aqs_analysis_ready.parquet"
        )

        self.assertEqual(result["manifest_status"], "unavailable")
        self.assertEqual(
            result["smoke_indicator"]["status"],
            "not_assessed_source_manifest_unavailable",
        )

    def test_default_sza_bounds_match_documented_tuv_support_points(self):
        self.assertEqual(DEFAULT_TUV_QC_SZA_MIN_DEG, 21.0)
        self.assertEqual(DEFAULT_TUV_QC_SZA_MAX_DEG, 49.8)

    @patch("leighton_relationship_analysis.calculate_solar_zenith_angle")
    def test_documented_tuv_support_bounds_are_inclusive(self, mock_sza):
        index = pd.date_range("2025-05-21 10:00", periods=4, freq="h")
        mock_sza.return_value = pd.Series([20.9, 21.0, 49.8, 49.9], index=index)
        data = pd.DataFrame(
            {
                "Temp": [70.0] * 4,
                "UV": [20.0] * 4,
                "O3": [0.05] * 4,
                "NO": [1.0] * 4,
                "NO2": [5.0] * 4,
                "datetime_utc": pd.date_range(
                    "2025-05-21 17:00Z", periods=4, freq="h"
                ),
            },
            index=index,
        )

        result = calculate_leighton_ratio(data)

        self.assertEqual(
            result["tuv_sza_extrapolated"].tolist(),
            [True, False, False, True],
        )

    def test_stale_analysis_artifacts_are_cleared_across_period_modes(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            monthly = output / "monthly"
            monthly.mkdir()
            stale = [
                output / "leighton_ratio_2025_available_observations.parquet",
                output / "hourly_diagnostics_2025_available_observations.parquet",
                output
                / "oxidative_regime_summary_2025_available_observations.csv",
                output / "monthly_summary.csv",
                monthly / "2025-05_leighton_ratio_timeseries.png",
                monthly / "2024-05_log10_leighton_ratio_timeseries.png",
            ]
            for path in stale:
                path.write_text("stale", encoding="utf-8")
            unrelated = output / "review_notes.md"
            unrelated.write_text("keep", encoding="utf-8")

            clear_stale_analysis_artifacts(output)

            self.assertTrue(all(not path.exists() for path in stale))
            self.assertFalse(monthly.exists())
            self.assertEqual(unrelated.read_text(encoding="utf-8"), "keep")

    def test_available_year_label_does_not_claim_complete_full_year(self):
        self.assertEqual(
            period_label(AnalysisConfig(year=2025, month=None)),
            "2025 available observations",
        )

    def test_reversed_sza_qc_range_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "minimum must not exceed maximum"):
            AnalysisConfig(tuv_qc_sza_min_deg=60.0, tuv_qc_sza_max_deg=20.0)

    def test_primary_no_cutoff_is_strict_at_point_20_ppb(self):
        no_values = pd.Series([0.1999, 0.20, 0.2001, 0.50])

        selected = apply_no_threshold(no_values, 0.20, ">")

        self.assertEqual(selected.tolist(), [False, False, True, True])
        self.assertEqual(AnalysisConfig().minimum_no_ppb, 0.20)
        self.assertEqual(AnalysisConfig().minimum_no_operator, ">")

    def test_no_cutoff_operator_is_configurable(self):
        no_values = pd.Series([0.20, 0.21])

        selected = apply_no_threshold(no_values, 0.20, ">=")

        self.assertEqual(selected.tolist(), [True, True])

    def test_no_threshold_sensitivity_uses_strict_cutoffs(self):
        data = pd.DataFrame(
            {
                "NO": [0.0, 0.05, 0.10, 0.20, 0.50, 1.0, 2.0],
                "LR": [10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0],
            }
        )

        result = summarize_no_threshold_sensitivity(data)

        self.assertEqual(result["operator"].unique().tolist(), [">"])
        self.assertEqual(result["count"].tolist(), [6, 5, 4, 3, 2, 1])
        point_two = result.loc[result["minimum_no_ppb"].eq(0.20)].iloc[0]
        self.assertEqual(point_two["median_lr"], 5.0)
        self.assertEqual(point_two["p90_lr"], 5.8)
        self.assertEqual(point_two["max_lr"], 6.0)

    def test_calculation_retains_rows_with_missing_audit_columns(self):
        data = pd.DataFrame(
            {
                "Temp": [70.0],
                "UV": [20.0],
                "O3": [0.05],
                "NO": [1.0],
                "NO2": [5.0],
                "NO_qualifier": [pd.NA],
                "datetime_utc": pd.to_datetime(["2025-05-01 19:00Z"]),
            },
            index=pd.DatetimeIndex(["2025-05-01 12:00"]),
        )

        result = calculate_leighton_ratio(data)

        self.assertEqual(len(result), 1)
        self.assertTrue(result["LR"].notna().all())
        self.assertTrue(result["NO_qualifier"].isna().all())

    def test_tuv_sza_transfer_function_matches_email_equation(self):
        uv = pd.Series([20.0, 20.0])
        sza = pd.Series([0.0, 45.0])

        j_no2, coefficient = calculate_tuv_j_no2(uv, sza)

        self.assertAlmostEqual(coefficient.iloc[0], TUV_SZA_INTERCEPT_M2_W_S)
        self.assertAlmostEqual(
            coefficient.iloc[1],
            TUV_SZA_INTERCEPT_M2_W_S
            + 45.0 * TUV_SZA_SLOPE_M2_W_S_DEG,
        )
        self.assertAlmostEqual(j_no2.iloc[1], coefficient.iloc[1] * 20.0)

    def test_solar_zenith_is_near_zenith_at_equinox_equatorial_noon(self):
        timestamps = pd.Series(pd.to_datetime(["2025-03-20 12:00Z"]))

        sza = calculate_solar_zenith_angle(timestamps, 0.0, 0.0)

        self.assertLess(sza.iloc[0], 3.0)

    def test_calculation_preserves_sza_and_applied_coefficient(self):
        data = pd.DataFrame(
            {
                "Temp": [70.0],
                "UV": [20.0],
                "O3": [0.05],
                "NO": [1.0],
                "NO2": [5.0],
                "datetime_utc": pd.to_datetime(["2025-05-21 19:00Z"]),
            },
            index=pd.DatetimeIndex(["2025-05-21 12:00"]),
        )

        result = calculate_leighton_ratio(data)

        coefficient = result["j_conversion_coefficient_m2_w_s"].iloc[0]
        self.assertAlmostEqual(result["J"].iloc[0], coefficient * 20.0)
        self.assertAlmostEqual(
            result["log10_LR"].iloc[0],
            math.log10(result["LR"].iloc[0]),
        )
        self.assertTrue(result["solar_zenith_angle_deg"].between(0, 90).all())
        self.assertEqual(
            result["j_calibration_method"].iloc[0],
            "provisional_tuv_sza_linear",
        )
        self.assertIn("tuv_sza_extrapolated", result)

    def test_email_confirmed_jpl_20_no_o3_rate_constant_is_used(self):
        data = pd.DataFrame(
            {
                "Temp": [(298.0 - 273.15) * 9.0 / 5.0 + 32.0],
                "UV": [20.0],
                "O3": [0.05],
                "NO": [1.0],
                "NO2": [5.0],
                "datetime_utc": pd.to_datetime(["2025-05-21 19:00Z"]),
            },
            index=pd.DatetimeIndex(["2025-05-21 12:00"]),
        )

        result = calculate_leighton_ratio(data)

        expected = JPL_NO_O3_PREFACTOR * math.exp(
            -JPL_NO_O3_ACTIVATION_OVER_R_K / 298.0
        )
        self.assertAlmostEqual(result["K"].iloc[0], expected)
        self.assertEqual(
            result["k_no_o3_method"].iloc[0],
            "JPL_20_arrhenius",
        )

    def test_excess_oxidation_and_radical_diagnostics_match_email_equations(self):
        temperature_f = (298.0 - 273.15) * 9.0 / 5.0 + 32.0
        data = pd.DataFrame(
            {
                "Temp": [temperature_f, temperature_f],
                "UV": [20.0, 20.0],
                "O3": [0.05, 0.05],
                "NO": [1.0, 1.0],
                "NO2": [10.0, 2.0],
                "datetime_utc": pd.to_datetime(
                    ["2025-05-21 19:00Z", "2025-05-21 20:00Z"]
                ),
            },
            index=pd.DatetimeIndex(
                ["2025-05-21 12:00", "2025-05-21 13:00"]
            ),
        )

        result = calculate_leighton_ratio(data)

        expected_ro2_rate = GENERIC_RO2_NO_PREFACTOR * math.exp(
            GENERIC_RO2_NO_ACTIVATION_OVER_R_K / 298.0
        )
        expected_ho2_rate = JPL_HO2_NO_PREFACTOR * math.exp(
            JPL_HO2_NO_ACTIVATION_OVER_R_K / 298.0
        )
        self.assertTrue(result["k_ro2_no_cm3_molecule_s"].eq(expected_ro2_rate).all())
        self.assertTrue(result["k_ho2_no_cm3_molecule_s"].eq(expected_ho2_rate).all())
        pd.testing.assert_series_equal(
            result["P_excess_molecules_cm3_s"],
            result["P_total_molecules_cm3_s"] - result["P_o3_molecules_cm3_s"],
            check_names=False,
        )
        pd.testing.assert_series_equal(
            result["fractional_excess_oxidation"],
            1.0 - 1.0 / result["LR"],
            check_names=False,
        )
        self.assertGreater(result["P_excess_molecules_cm3_s"].iloc[0], 0.0)
        self.assertLess(result["P_excess_molecules_cm3_s"].iloc[1], 0.0)
        self.assertTrue(pd.notna(result["ROx_equiv_molecules_cm3"].iloc[0]))
        self.assertTrue(pd.isna(result["ROx_equiv_molecules_cm3"].iloc[1]))
        self.assertTrue(pd.notna(result["HO2_inferred_molecules_cm3"].iloc[0]))
        self.assertTrue(pd.isna(result["HO2_inferred_molecules_cm3"].iloc[1]))
        self.assertLess(
            result["HO2_inferred_signed_molecules_cm3"].iloc[1],
            0.0,
        )
        self.assertEqual(
            result["LR_robustly_gt_1"].tolist(),
            (
                result["LR"]
                - result["LR_quantified_absolute_uncertainty"]
                > 1.0
            ).tolist(),
        )

    def test_sza_extrapolation_is_flagged_without_dropping_rows(self):
        data = pd.DataFrame(
            {
                "Temp": [70.0, 70.0],
                "UV": [20.0, 20.0],
                "O3": [0.05, 0.05],
                "NO": [1.0, 1.0],
                "NO2": [5.0, 5.0],
                "datetime_utc": pd.to_datetime(
                    ["2025-06-21 19:00Z", "2025-12-21 16:00Z"]
                ),
            },
            index=pd.DatetimeIndex(["2025-06-21 12:00", "2025-12-21 09:00"]),
        )

        result = calculate_leighton_ratio(
            data,
            AnalysisConfig(
                tuv_qc_sza_min_deg=20.0,
                tuv_qc_sza_max_deg=50.0,
            ),
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result["tuv_sza_extrapolated"].tolist(), [True, True])
        self.assertTrue(result["J"].notna().all())

    def test_provisional_uncertainty_is_row_level_fourteen_point_nine_percent(self):
        data = pd.DataFrame(
            {
                "Temp": [70.0, 70.0],
                "UV": [20.0, 20.0],
                "O3": [0.05, 0.05],
                "NO": [1.0, 0.5],
                "NO2": [5.0, 5.0],
                "datetime_utc": pd.to_datetime(
                    ["2025-05-21 19:00Z", "2025-05-21 20:00Z"]
                ),
            },
            index=pd.DatetimeIndex(["2025-05-21 12:00", "2025-05-21 13:00"]),
        )

        result = calculate_leighton_ratio(data)

        self.assertTrue(
            result["LR_quantified_relative_uncertainty"].eq(0.149).all()
        )
        expected = result["LR"].abs() * 0.149
        pd.testing.assert_series_equal(
            result["LR_quantified_absolute_uncertainty"],
            expected,
            check_names=False,
        )

    def test_hourly_diagnostics_has_requested_schema_and_derived_nox(self):
        data = pd.DataFrame(
            {
                "datetime_utc": pd.to_datetime(["2025-06-01 19:00Z"]),
                "NO": [1.25],
                "NO2": [4.75],
                "O3": [0.05],
                "UV": [20.0],
                "SR_W_m2": [750.0],
                "J": [0.004],
                "solar_zenith_angle_deg": [25.0],
                "Temp": [70.0],
                "clearing_index": [850],
                "LR": [1.5],
                "log10_LR": [math.log10(1.5)],
                "k_no_o3_cm3_molecule_s": [2.0e-14],
                "P_o3_molecules_cm3_s": [4.0e8],
                "P_total_molecules_cm3_s": [6.0e8],
                "P_excess_molecules_cm3_s": [2.0e8],
                "fractional_excess_oxidation": [1.0 / 3.0],
                "k_ro2_no_cm3_molecule_s": [9.0e-12],
                "ROx_equiv_molecules_cm3": [1.0e9],
                "k_ho2_no_cm3_molecule_s": [8.0e-12],
                "HO2_inferred_signed_molecules_cm3": [1.1e9],
                "HO2_inferred_molecules_cm3": [1.1e9],
                "LR_gt_1p5": [False],
                "LR_gt_2": [False],
                "LR_robustly_gt_1": [True],
                "tuv_sza_extrapolated": [False],
            },
            index=pd.DatetimeIndex(
                ["2025-06-01 12:00"], name="datetime_local_standard"
            ),
        )

        result = build_hourly_diagnostics(data)

        self.assertEqual(
            result.columns.tolist(),
            [
                "datetime_local_standard",
                "datetime_utc",
                "NO",
                "NO2",
                "O3",
                "NOx",
                "UV",
                "SR_W_m2",
                "J",
                "solar_zenith_angle_deg",
                "Temp",
                "clearing_index",
                "LR",
                "oxidative_regime",
                "log10_LR",
                "k_no_o3_cm3_molecule_s",
                "P_o3_molecules_cm3_s",
                "P_total_molecules_cm3_s",
                "P_excess_molecules_cm3_s",
                "fractional_excess_oxidation",
                "k_ro2_no_cm3_molecule_s",
                "ROx_equiv_molecules_cm3",
                "k_ho2_no_cm3_molecule_s",
                "HO2_inferred_signed_molecules_cm3",
                "HO2_inferred_molecules_cm3",
                "LR_gt_1p5",
                "LR_gt_2",
                "LR_robustly_gt_1",
            ],
        )
        self.assertEqual(result.loc[0, "NOx"], 6.0)
        self.assertEqual(result.loc[0, "clearing_index"], 850)
        self.assertEqual(result.loc[0, "oxidative_regime"], "1.5 <= LR < 2")

    def test_hourly_diagnostics_marks_unavailable_clearing_index_as_missing(self):
        data = pd.DataFrame(
            {
                "datetime_utc": pd.to_datetime(["2025-06-01 19:00Z"]),
                "NO": [1.0],
                "NO2": [2.0],
                "O3": [0.05],
                "UV": [20.0],
                "SR_W_m2": [750.0],
                "J": [0.004],
                "solar_zenith_angle_deg": [25.0],
                "Temp": [70.0],
                "LR": [1.5],
                "log10_LR": [math.log10(1.5)],
                "k_no_o3_cm3_molecule_s": [2.0e-14],
                "P_o3_molecules_cm3_s": [4.0e8],
                "P_total_molecules_cm3_s": [6.0e8],
                "P_excess_molecules_cm3_s": [2.0e8],
                "fractional_excess_oxidation": [1.0 / 3.0],
                "k_ro2_no_cm3_molecule_s": [9.0e-12],
                "ROx_equiv_molecules_cm3": [1.0e9],
                "k_ho2_no_cm3_molecule_s": [8.0e-12],
                "HO2_inferred_signed_molecules_cm3": [1.1e9],
                "HO2_inferred_molecules_cm3": [1.1e9],
                "LR_gt_1p5": [False],
                "LR_gt_2": [False],
                "LR_robustly_gt_1": [True],
            },
            index=pd.DatetimeIndex(
                ["2025-06-01 12:00"], name="datetime_local_standard"
            ),
        )

        result = build_hourly_diagnostics(data)

        self.assertTrue(result["clearing_index"].isna().all())

    def test_run_analysis_writes_period_named_processed_and_diagnostics_files(self):
        index = pd.DatetimeIndex(
            ["2024-06-01 12:00"], name="datetime_local_standard"
        )
        calculated = pd.DataFrame(
            {
                "datetime_utc": pd.to_datetime(["2024-06-01 19:00Z"]),
                "NO": [1.0],
                "NO2": [2.0],
                "O3": [0.05],
                "UV": [20.0],
                "SR_W_m2": [750.0],
                "J": [0.004],
                "solar_zenith_angle_deg": [25.0],
                "Temp": [70.0],
                "LR": [1.5],
                "log10_LR": [math.log10(1.5)],
                "k_no_o3_cm3_molecule_s": [2.0e-14],
                "P_o3_molecules_cm3_s": [4.0e8],
                "P_total_molecules_cm3_s": [6.0e8],
                "P_excess_molecules_cm3_s": [2.0e8],
                "fractional_excess_oxidation": [1.0 / 3.0],
                "k_ro2_no_cm3_molecule_s": [9.0e-12],
                "ROx_equiv_molecules_cm3": [1.0e9],
                "k_ho2_no_cm3_molecule_s": [8.0e-12],
                "HO2_inferred_signed_molecules_cm3": [1.1e9],
                "HO2_inferred_molecules_cm3": [1.1e9],
                "LR_gt_1p5": [False],
                "LR_gt_2": [False],
                "LR_robustly_gt_1": [True],
                "tuv_sza_extrapolated": [False],
            },
            index=index,
        )
        accounting = {
            "uv_alignment_candidates": [
                {"shift_hours": 0, "paired_rows": 24, "uv_sr_pearson_r": 1.0}
            ],
            "uv_selected_shift_hours": 0,
        }

        with tempfile.TemporaryDirectory() as temporary, patch(
            "leighton_relationship_analysis.load_selected_measurements",
            return_value=(calculated, accounting),
        ), patch(
            "leighton_relationship_analysis.calculate_leighton_ratio",
            return_value=calculated,
        ), patch(
            "leighton_relationship_analysis.plot_ratio_timeseries"
        ), patch(
            "leighton_relationship_analysis.plot_log_ratio_timeseries"
        ), patch(
            "leighton_relationship_analysis.plot_ho2_diagnostic"
        ) as mock_ho2_plot, patch(
            "leighton_relationship_analysis.plot_lr_relationship"
        ), patch(
            "leighton_relationship_analysis.plot_ratio_distributions"
        ), patch(
            "leighton_relationship_analysis.plot_temperature_correction"
        ), patch(
            "leighton_relationship_analysis.plot_uv_alignment_diagnostic"
        ):
            output = Path(temporary)
            summary = run_analysis(
                Path("unused-aqs.parquet"),
                Path("unused-uv.csv"),
                output,
                AnalysisConfig(year=2024, month=6),
            )

            processed = output / "leighton_ratio_2024_06.parquet"
            diagnostics_path = output / "hourly_diagnostics_2024_06.parquet"
            regime_summary_path = (
                output / "oxidative_regime_summary_2024_06.csv"
            )
            self.assertTrue(processed.is_file())
            self.assertTrue(diagnostics_path.is_file())
            self.assertTrue(regime_summary_path.is_file())
            self.assertFalse((output / "leighton_ratio_may_2025.parquet").exists())
            diagnostics = pd.read_parquet(diagnostics_path)
            self.assertEqual(len(diagnostics), 1)
            self.assertEqual(diagnostics.loc[0, "NOx"], 3.0)
            self.assertTrue(pd.isna(diagnostics.loc[0, "clearing_index"]))
            self.assertEqual(
                summary["hourly_diagnostics"]["path"],
                diagnostics_path.name,
            )
            self.assertEqual(
                summary["source_provenance"]["manifest_status"], "unavailable"
            )
            self.assertEqual(
                summary["uncertainty"]["monitor_specific_aqs_qa_status"],
                "not_retrieved_unverified",
            )
            self.assertIn("partial", summary["uncertainty"]["scope"])
            self.assertEqual(
                summary["ho2_diagnostic"]["path"],
                "ho2_inferred_diagnostic.png",
            )
            self.assertEqual(summary["ho2_diagnostic"]["positive_excess_rows"], 1)
            mock_ho2_plot.assert_called_once()
            plot_data, plot_config, plot_destination = mock_ho2_plot.call_args.args
            pd.testing.assert_frame_equal(plot_data, calculated)
            self.assertEqual(plot_config, AnalysisConfig(year=2024, month=6))
            self.assertEqual(
                plot_destination,
                output / "ho2_inferred_diagnostic.png",
            )
            self.assertEqual(
                summary["oxidative_regimes"]["path"],
                regime_summary_path.name,
            )
            self.assertEqual(
                summary["oxidative_regimes"]["counts"]["1.5 <= LR < 2"],
                1,
            )

    def test_time_windows_are_non_overlapping_and_exhaustive(self):
        data = pd.DataFrame(
            {"LR": [1.0, 2.0, 3.0, 4.0]},
            index=pd.DatetimeIndex(
                [
                    "2025-05-01 08:00",
                    "2025-05-01 10:00",
                    "2025-05-01 16:00",
                    "2025-05-01 17:00",
                ]
            ),
        )

        daytime, outside = split_time_windows(data, AnalysisConfig())

        self.assertEqual(len(daytime), 2)
        self.assertEqual(len(outside), 2)
        self.assertTrue(daytime.index.intersection(outside.index).empty)

    def test_distribution_plot_handles_empty_outside_window(self):
        daytime = pd.DataFrame(
            {"LR": [1.0, 2.0]},
            index=pd.DatetimeIndex(["2025-05-01 10:00", "2025-05-01 11:00"]),
        )
        outside = daytime.iloc[0:0]
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "distribution.png"
            plot_ratio_distributions(
                daytime,
                outside,
                AnalysisConfig(apply_sr_ci=True),
                output,
            )
            self.assertTrue(output.exists())

    def test_ho2_diagnostic_plots_positive_and_signed_series(self):
        data = pd.DataFrame(
            {
                "HO2_inferred_molecules_cm3": [1.2e9, pd.NA, 2.4e9],
                "HO2_inferred_signed_molecules_cm3": [1.2e9, -0.8e9, 2.4e9],
            },
            index=pd.date_range("2025-05-01 10:00", periods=3, freq="h"),
        )

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "ho2_inferred_diagnostic.png"
            plot_ho2_diagnostic(data, AnalysisConfig(), output)

            self.assertTrue(output.is_file())
            self.assertGreater(output.stat().st_size, 0)

    def test_ho2_diagnostic_handles_no_positive_excess_values(self):
        data = pd.DataFrame(
            {
                "HO2_inferred_molecules_cm3": [pd.NA, pd.NA],
                "HO2_inferred_signed_molecules_cm3": [-1.0e9, 0.0],
            },
            index=pd.date_range("2025-05-01 10:00", periods=2, freq="h"),
        )

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "ho2_inferred_diagnostic.png"
            plot_ho2_diagnostic(data, AnalysisConfig(), output)

            self.assertTrue(output.is_file())
            self.assertGreater(output.stat().st_size, 0)

    def test_uv_alignment_selects_shift_with_best_solar_agreement(self):
        timestamps = pd.date_range("2025-05-01", periods=48, freq="h")
        uv_values = pd.Series(
            [((index * 17) % 43) + (index % 5) for index in range(48)],
            dtype=float,
        )
        uv = pd.DataFrame(
            {
                "uv_datetime": timestamps,
                "UV": uv_values,
            }
        )
        # Solar radiation at AQS time t corresponds to UV stamped one hour
        # later, so the correct shift applied to UV timestamps is -1 hour.
        aqs = pd.DataFrame(
            {
                "datetime_local_standard": timestamps,
                "SR": uv_values.shift(-1),
            }
        )

        selected, diagnostics = evaluate_uv_alignment(
            aqs,
            uv,
            AnalysisConfig(uv_alignment_max_shift=2),
        )

        self.assertEqual(selected, -1)
        self.assertEqual(len(diagnostics), 5)

    def test_explicit_uv_shift_is_honored(self):
        timestamps = pd.date_range("2025-05-01", periods=48, freq="h")
        aqs = pd.DataFrame(
            {
                "datetime_local_standard": timestamps,
                "SR": range(48),
            }
        )
        uv = pd.DataFrame(
            {
                "uv_datetime": timestamps,
                "UV": range(48),
            }
        )

        selected, diagnostics = evaluate_uv_alignment(
            aqs,
            uv,
            AnalysisConfig(uv_hour_shift=0),
        )

        self.assertEqual(selected, 0)
        self.assertEqual(len(diagnostics), 1)


if __name__ == "__main__":
    unittest.main()
