import math
import unittest

import pandas as pd

from lr_uncertainty import (
    APOGEE_SU200_MAY21_RELATIVE_UNCERTAINTIES,
    CURRENTLY_UNQUANTIFIED_TERMS,
    add_quantified_lr_uncertainty,
    apogee_su200_may21_relative_uncertainty,
    combine_independent_relative_uncertainties,
    propagate_leighton_relative_uncertainty,
)


class LeightonUncertaintyTests(unittest.TestCase):
    def test_apogee_terms_combine_to_five_point_five_percent(self):
        expected = math.sqrt(5**2 + 0.5**2 + 1**2 + 2**2) / 100
        self.assertAlmostEqual(apogee_su200_may21_relative_uncertainty(), expected)
        self.assertAlmostEqual(expected, 0.055, places=3)
        self.assertEqual(len(APOGEE_SU200_MAY21_RELATIVE_UNCERTAINTIES), 4)

    def test_independent_lr_terms_propagate_by_quadrature(self):
        budget = propagate_leighton_relative_uncertainty(
            {"J": 0.03, "NO2": 0.04},
            unquantified_terms=("NO", "O3", "K"),
        )
        self.assertAlmostEqual(
            budget.combined_quantified_relative_uncertainty, 0.05
        )
        self.assertAlmostEqual(budget.absolute_uncertainty(2.0), 0.10)
        self.assertEqual(budget.unquantified_terms, ("NO", "O3", "K"))

    def test_default_budget_identifies_unquantified_terms(self):
        budget = propagate_leighton_relative_uncertainty({"Apogee": 0.055})
        self.assertEqual(budget.unquantified_terms, CURRENTLY_UNQUANTIFIED_TERMS)
        self.assertIn("NO measurement", budget.unquantified_terms)
        self.assertIn("NO2 quantum yield", budget.unquantified_terms)

    def test_row_level_sources_preserve_missing_coverage(self):
        frame = pd.DataFrame(
            {
                "LR": [2.0, 4.0, 1.0],
                "j_u": [0.03, 0.03, math.nan],
                "no_u": [0.04, math.nan, math.nan],
            }
        )
        result = add_quantified_lr_uncertainty(
            frame, {"J": "j_u", "NO": "no_u"}
        )
        self.assertAlmostEqual(
            result.loc[0, "LR_quantified_relative_uncertainty"], 0.05
        )
        self.assertAlmostEqual(
            result.loc[0, "LR_quantified_absolute_uncertainty"], 0.10
        )
        self.assertAlmostEqual(
            result.loc[1, "LR_quantified_absolute_uncertainty"], 0.12
        )
        self.assertTrue(
            math.isnan(result.loc[2, "LR_quantified_relative_uncertainty"])
        )
        self.assertEqual(result["LR_uncertainty_component_count"].tolist(), [2, 1, 0])

    def test_invalid_uncertainty_is_rejected(self):
        with self.assertRaises(ValueError):
            combine_independent_relative_uncertainties([-0.01])
        with self.assertRaises(ValueError):
            combine_independent_relative_uncertainties([float("nan")])


if __name__ == "__main__":
    unittest.main()
