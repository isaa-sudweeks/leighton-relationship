import unittest
from pathlib import Path
import tempfile

import pandas as pd

from leighton_relationship_analysis import (
    AnalysisConfig,
    calculate_leighton_ratio,
    evaluate_uv_alignment,
    plot_ratio_distributions,
    split_time_windows,
)


class LeightonAnalysisTests(unittest.TestCase):
    def test_calculation_retains_rows_with_missing_audit_columns(self):
        data = pd.DataFrame(
            {
                "Temp": [70.0],
                "UV": [20.0],
                "O3": [0.05],
                "NO": [1.0],
                "NO2": [5.0],
                "NO_qualifier": [pd.NA],
            },
            index=pd.DatetimeIndex(["2025-05-01 12:00"]),
        )

        result = calculate_leighton_ratio(data)

        self.assertEqual(len(result), 1)
        self.assertTrue(result["LR"].notna().all())
        self.assertTrue(result["NO_qualifier"].isna().all())

    def test_time_windows_are_non_overlapping_and_exhaustive(self):
        data = pd.DataFrame(
            {"LR": [1.0, 2.0, 3.0, 4.0]},
            index=pd.DatetimeIndex(
                [
                    "2025-05-01 08:00",
                    "2025-05-01 09:00",
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
