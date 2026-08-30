import unittest

from scripts.package_callum_deliverable import matched_configuration


class DeliverablePackageTests(unittest.TestCase):
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
