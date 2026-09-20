import unittest

import numpy as np
import pandas as pd

from oxidative_regimes import (
    OXIDATIVE_REGIME_LABELS,
    OXIDATIVE_REGIME_VARIABLE_UNITS,
    classify_oxidative_regime,
    summarize_oxidative_regimes,
)


def _diagnostic_frame(lr: list[float]) -> pd.DataFrame:
    rows = len(lr)
    return pd.DataFrame(
        {
            "LR": lr,
            "NO": np.arange(1.0, rows + 1.0),
            "NO2": np.arange(2.0, rows + 2.0),
            "O3": np.repeat(0.04, rows),
            "NOx": np.arange(3.0, rows + 3.0),
            "J": np.repeat(0.004, rows),
            "UV": np.repeat(20.0, rows),
            "SR_W_m2": np.repeat(750.0, rows),
            "solar_zenith_angle_deg": np.repeat(30.0, rows),
            "Temp": np.repeat(70.0, rows),
            "clearing_index": np.repeat(900.0, rows),
        }
    )


class OxidativeRegimeTests(unittest.TestCase):
    def test_classification_boundaries_and_missing_lr(self):
        lr = pd.Series(
            [0.999, 1.0, 1.499, 1.5, 1.999, 2.0, np.nan, np.inf]
        )

        result = classify_oxidative_regime(lr)

        self.assertEqual(
            result.astype("object").iloc[:6].tolist(),
            [
                "LR < 1",
                "1 <= LR < 1.5",
                "1 <= LR < 1.5",
                "1.5 <= LR < 2",
                "1.5 <= LR < 2",
                "LR >= 2",
            ],
        )
        self.assertTrue(result.iloc[6:].isna().all())
        self.assertEqual(tuple(result.cat.categories), OXIDATIVE_REGIME_LABELS)

    def test_summary_reports_counts_missingness_units_and_statistics(self):
        data = _diagnostic_frame([0.8, 0.9, 1.2, 1.7, 2.3])
        data.loc[1, "NO"] = np.nan
        data.loc[0:1, "clearing_index"] = np.nan

        result = summarize_oxidative_regimes(data)

        self.assertEqual(
            len(result),
            len(OXIDATIVE_REGIME_LABELS)
            * len(OXIDATIVE_REGIME_VARIABLE_UNITS),
        )
        no_low = result.loc[
            (result["oxidative_regime"] == "LR < 1")
            & (result["variable"] == "NO")
        ].iloc[0]
        self.assertEqual(no_low["unit"], "ppb")
        self.assertEqual(no_low["regime_rows"], 2)
        self.assertEqual(no_low["valid_count"], 1)
        self.assertEqual(no_low["missing_count"], 1)
        self.assertEqual(no_low["mean"], 1.0)
        self.assertTrue(pd.isna(no_low["std"]))

        ci_low = result.loc[
            (result["oxidative_regime"] == "LR < 1")
            & (result["variable"] == "clearing_index")
        ].iloc[0]
        self.assertEqual(ci_low["valid_count"], 0)
        self.assertEqual(ci_low["missing_count"], 2)
        self.assertTrue(pd.isna(ci_low["median"]))

    def test_summary_emits_empty_regimes(self):
        result = summarize_oxidative_regimes(_diagnostic_frame([0.8]))
        high = result.loc[result["oxidative_regime"] == "LR >= 2"]

        self.assertEqual(len(high), len(OXIDATIVE_REGIME_VARIABLE_UNITS))
        self.assertTrue((high["regime_rows"] == 0).all())
        self.assertTrue((high["valid_count"] == 0).all())

    def test_summary_rejects_missing_requested_variables(self):
        with self.assertRaisesRegex(ValueError, "clearing_index"):
            summarize_oxidative_regimes(
                _diagnostic_frame([1.0]).drop(columns="clearing_index")
            )


if __name__ == "__main__":
    unittest.main()
