import unittest
import math
from pathlib import Path
import tempfile

import pandas as pd

from leighton_relationship_analysis import (
    AnalysisConfig,
    JPL_NO_O3_ACTIVATION_OVER_R_K,
    JPL_NO_O3_PREFACTOR,
    JPL_NO_O3_REFERENCE_TEMPERATURE_K,
    JPL_NO_O3_TEMPERATURE_EXPONENT,
    TUV_SZA_INTERCEPT_M2_W_S,
    TUV_SZA_SLOPE_M2_W_S_DEG,
    apply_no_threshold,
    calculate_leighton_ratio,
    calculate_solar_zenith_angle,
    calculate_tuv_j_no2,
    evaluate_uv_alignment,
    plot_ratio_distributions,
    period_label,
    split_time_windows,
    summarize_no_threshold_sensitivity,
)


class LeightonAnalysisTests(unittest.TestCase):
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

    def test_jpl_19_5_non_arrhenius_rate_constant_is_used(self):
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

        expected = (
            JPL_NO_O3_PREFACTOR
            * (298.0 / JPL_NO_O3_REFERENCE_TEMPERATURE_K)
            ** JPL_NO_O3_TEMPERATURE_EXPONENT
            * math.exp(-JPL_NO_O3_ACTIVATION_OVER_R_K / 298.0)
        )
        self.assertAlmostEqual(result["K"].iloc[0], expected)
        self.assertEqual(
            result["k_no_o3_method"].iloc[0],
            "JPL_19-5_C19_non_arrhenius",
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
