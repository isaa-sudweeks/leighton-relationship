import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pandas as pd

from condition_statistics import (
    CONDITION_REPORT_FILENAMES,
    RELATIONSHIPS,
    calculate_condition_statistics,
    generate_condition_report,
)


def diagnostics_frame(rows: int = 5) -> pd.DataFrame:
    values = np.arange(1.0, rows + 1.0)
    return pd.DataFrame(
        {
            "LR": values,
            "ROx_equiv_molecules_cm3": values * 1e8,
            "NO": values,
            "NO2": values + 1.0,
            "O3": values / 100.0,
            "NOx": values + 2.0,
            "J": values / 1000.0,
            "UV": values * 10.0,
            "SR_W_m2": values * 100.0,
            "solar_zenith_angle_deg": values * 5.0,
            "Temp": values + 70.0,
            "clearing_index": values * 100.0,
        }
    )


class ConditionStatisticsTests(unittest.TestCase):
    def test_requested_relationships_are_complete_and_nonduplicated(self):
        self.assertEqual(len(RELATIONSHIPS), 16)
        self.assertEqual(len({item.filename for item in RELATIONSHIPS}), 16)
        lr_predictors = {
            item.predictor for item in RELATIONSHIPS if item.response == "LR"
        }
        self.assertEqual(
            lr_predictors,
            {
                "NO",
                "NO2",
                "O3",
                "NOx",
                "J",
                "UV",
                "SR_W_m2",
                "solar_zenith_angle_deg",
                "Temp",
                "clearing_index",
            },
        )

    def test_statistics_use_pairwise_finite_rows(self):
        diagnostics = diagnostics_frame()
        diagnostics.loc[1, "NO"] = np.nan
        diagnostics.loc[2, "NO"] = np.inf

        result = calculate_condition_statistics(diagnostics)
        row = result.loc[
            (result["response"] == "LR") & (result["predictor"] == "NO")
        ].iloc[0]

        self.assertEqual(row["total_rows"], 5)
        self.assertEqual(row["pairwise_valid_n"], 3)
        self.assertEqual(row["excluded_nonfinite_or_missing_n"], 2)
        self.assertEqual(row["status"], "ok")
        self.assertAlmostEqual(row["pearson_r"], 1.0)
        self.assertAlmostEqual(row["spearman_rho"], 1.0)
        self.assertTrue(np.isfinite(row["pearson_p_value"]))
        self.assertTrue(np.isfinite(row["spearman_p_value"]))

    def test_constant_and_insufficient_inputs_have_explicit_status(self):
        diagnostics = diagnostics_frame()
        diagnostics["NO"] = 4.0
        diagnostics.loc[2:, "clearing_index"] = np.nan

        result = calculate_condition_statistics(diagnostics)
        constant = result.loc[
            (result["response"] == "LR") & (result["predictor"] == "NO")
        ].iloc[0]
        insufficient = result.loc[
            (result["response"] == "LR")
            & (result["predictor"] == "clearing_index")
        ].iloc[0]

        self.assertEqual(constant["status"], "constant_predictor")
        self.assertTrue(np.isnan(constant["pearson_r"]))
        self.assertEqual(insufficient["pairwise_valid_n"], 2)
        self.assertEqual(insufficient["status"], "insufficient_pairs")
        self.assertTrue(np.isnan(insufficient["spearman_p_value"]))

    def test_missing_required_column_is_reported(self):
        diagnostics = diagnostics_frame().drop(columns=["UV"])

        with self.assertRaisesRegex(ValueError, "UV"):
            calculate_condition_statistics(diagnostics)

    def test_generate_report_writes_all_declared_artifacts_and_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            output_dir = Path(temporary)
            metadata = generate_condition_report(
                diagnostics_frame(),
                output_dir,
                context="test period",
                source_name="hourly.parquet",
            )

            self.assertEqual(metadata["relationships"], 16)
            self.assertEqual(metadata["status_counts"], {"ok": 16})
            for filename in CONDITION_REPORT_FILENAMES:
                path = output_dir / filename
                self.assertTrue(path.is_file(), filename)
                self.assertGreater(path.stat().st_size, 0)
            stored = json.loads(
                (output_dir / "condition_report_metadata.json").read_text()
            )
            self.assertEqual(stored["source"], "hourly.parquet")
            table = pd.read_csv(output_dir / "condition_correlations.csv")
            self.assertEqual(len(table), 16)


if __name__ == "__main__":
    unittest.main()
