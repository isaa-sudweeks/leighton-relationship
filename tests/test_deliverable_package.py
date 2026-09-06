import unittest

from scripts.package_callum_deliverable import expected_artifacts, matched_configuration


class DeliverablePackageTests(unittest.TestCase):
    def test_expected_artifacts_use_configured_period_and_diagnostics_path(self):
        artifacts = expected_artifacts(
            {
                "configuration": {"year": 2024, "month": 6},
                "hourly_diagnostics": {
                    "path": "hourly_diagnostics_2024_06.parquet"
                },
            }
        )

        self.assertIn("leighton_ratio_2024_06.parquet", artifacts)
        self.assertIn("hourly_diagnostics_2024_06.parquet", artifacts)
        self.assertNotIn("leighton_ratio_may_2025.parquet", artifacts)

    def test_expected_artifacts_support_available_year_outputs(self):
        artifacts = expected_artifacts(
            {
                "configuration": {"year": 2025, "month": None},
                "hourly_diagnostics": {
                    "path": "hourly_diagnostics_2025_available_observations.parquet"
                },
            }
        )

        self.assertIn(
            "leighton_ratio_2025_available_observations.parquet", artifacts
        )
        self.assertIn(
            "hourly_diagnostics_2025_available_observations.parquet", artifacts
        )

    def test_accepts_only_sr_ci_switch_difference(self):
        primary = {"configuration": {"year": 2025, "apply_sr_ci": False}}
        sensitivity = {"configuration": {"year": 2025, "apply_sr_ci": True}}

        matched, differences = matched_configuration(primary, sensitivity)

        self.assertTrue(matched)
        self.assertEqual(differences, {})

    def test_rejects_mismatched_scientific_configuration(self):
        primary = {
            "configuration": {
                "minimum_no_ppb": 0.2,
                "apply_sr_ci": False,
            }
        }
        sensitivity = {
            "configuration": {
                "minimum_no_ppb": 0.1,
                "apply_sr_ci": True,
            }
        }

        matched, differences = matched_configuration(primary, sensitivity)

        self.assertFalse(matched)
        self.assertEqual(
            differences["minimum_no_ppb"], {"primary": 0.2, "sr_ci": 0.1}
        )


if __name__ == "__main__":
    unittest.main()
