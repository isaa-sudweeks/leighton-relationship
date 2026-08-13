from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import pandas as pd

from Scrapers import scraper


def sample_record(
    *,
    parameter_code="42601",
    poc=3,
    measurement=0.2,
    detection_limit=0.1,
    method_code="200",
):
    return {
        "state_code": "49",
        "county_code": "035",
        "site_number": "3006",
        "parameter_code": parameter_code,
        "parameter": "Nitric oxide (NO)",
        "poc": poc,
        "method_code": method_code,
        "method": "Example method",
        "sample_measurement": measurement,
        "units_of_measure": "Parts per billion",
        "detection_limit": detection_limit,
        "uncertainty": None,
        "qualifier": ["EXAMPLE"],
        "date_local": "2025-05-01",
        "time_local": "12:00",
        "date_gmt": "2025-05-01",
        "time_gmt": "19:00",
    }


class AQSScraperTests(unittest.TestCase):
    def test_parameter_batches_respect_api_limit(self):
        batches = scraper.parameter_batches(scraper.PARAMS)

        self.assertEqual([len(batch) for batch in batches], [5, 1])
        self.assertEqual(
            [code for batch in batches for code in batch],
            list(scraper.PARAMS),
        )

    def test_request_aqs_rejects_payload_level_failure(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "Header": [{"status": "Failed", "error": "Too many parameters"}],
            "Data": [],
        }
        session = Mock()
        session.get.return_value = response

        with self.assertRaisesRegex(scraper.AQSAPIError, "Too many parameters"):
            scraper.request_aqs(session, "sampleData/bySite", {"param": "bad"})

    def test_request_error_does_not_echo_credentials(self):
        session = Mock()
        session.get.side_effect = scraper.requests.ConnectionError(
            "failed URL with key=secret"
        )

        with self.assertRaises(scraper.AQSAPIError) as raised:
            scraper.request_aqs(
                session,
                "sampleData/bySite",
                {"email": "person@example.com", "key": "secret"},
            )

        self.assertNotIn("secret", str(raised.exception))
        self.assertNotIn("person@example.com", str(raised.exception))

    def test_normalize_preserves_monitor_identity_and_types(self):
        frame = scraper.normalize_records(
            [sample_record()],
            downloaded_at=datetime(2025, 6, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(frame.loc[0, "monitor_key"], "49-035-3006-42601-3-200")
        self.assertEqual(frame.loc[0, "parameter_abbreviation"], "NO")
        self.assertEqual(str(frame["poc"].dtype), "Int64")
        self.assertEqual(str(frame["sample_measurement"].dtype), "Float64")
        self.assertEqual(str(frame["datetime_utc"].dtype), "datetime64[ns, UTC]")
        self.assertEqual(frame.loc[0, "qualifier"], '["EXAMPLE"]')

    def test_inventory_never_combines_pocs(self):
        samples = scraper.normalize_records(
            [
                sample_record(poc=1, measurement=0.05, detection_limit=0.1),
                sample_record(poc=3, measurement=0.2, detection_limit=0.1),
            ]
        )

        inventory = scraper.build_monitor_inventory(samples)

        self.assertEqual(len(inventory), 2)
        by_poc = inventory.set_index("poc")
        self.assertEqual(by_poc.loc[1, "below_mdl_count"], 1)
        self.assertEqual(by_poc.loc[3, "at_or_above_mdl_count"], 1)

    def test_explicit_selection_does_not_average_pocs(self):
        samples = scraper.normalize_records(
            [
                sample_record(poc=1, measurement=0.05),
                sample_record(poc=3, measurement=0.2),
            ]
        )

        selected = scraper.select_poc_records(samples, {"NO": 3})

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected.loc[0, "poc"], 3)
        self.assertEqual(selected.loc[0, "sample_measurement"], 0.2)

    def test_save_download_records_missing_requested_parameters(self):
        samples = scraper.normalize_records([sample_record()])
        with tempfile.TemporaryDirectory() as temporary:
            output = scraper.save_download(
                samples,
                output_root=Path(temporary),
                site_name="Test Site",
                state_code="49",
                county_code="035",
                site_id="3006",
                start=pd.Timestamp("2025-05-01").date(),
                end=pd.Timestamp("2025-05-01").date(),
            )
            manifest = scraper.json.loads(
                (output / "manifest.json").read_text(encoding="utf-8")
            )
            round_trip = pd.read_parquet(output / "aqs_samples.parquet")

        self.assertEqual(manifest["returned_parameter_codes"], ["42601"])
        self.assertIn("63302", manifest["missing_parameter_codes"])
        self.assertEqual(str(round_trip["datetime_utc"].dtype), "datetime64[ns, UTC]")

    def test_analysis_table_cleans_without_discarding_source_values(self):
        records = [
            sample_record(poc=2, measurement=0.04, detection_limit=0.05),
            {
                **sample_record(
                    parameter_code="42602",
                    poc=3,
                    measurement=2.0,
                    detection_limit=0.1,
                    method_code="256",
                ),
                "qualifier": "QX - Does not meet QC criteria.",
            },
        ]
        samples = scraper.normalize_records(records)

        analysis, accounting = scraper.build_analysis_table(
            samples,
            {"NO": 2, "NO2": 3},
        )

        self.assertTrue(pd.isna(analysis.loc[0, "NO"]))
        self.assertEqual(analysis.loc[0, "NO_raw"], 0.04)
        self.assertTrue(analysis.loc[0, "NO_below_mdl"])
        self.assertTrue(pd.isna(analysis.loc[0, "NO2"]))
        self.assertEqual(analysis.loc[0, "NO2_raw"], 2.0)
        self.assertTrue(analysis.loc[0, "NO2_quality_flagged"])
        self.assertEqual(accounting["usable_rows"].sum(), 0)

    def test_informational_qualifier_remains_usable(self):
        record = sample_record(
            parameter_code="44201",
            poc=1,
            measurement=0.05,
            detection_limit=0.005,
            method_code="087",
        )
        record["qualifier"] = "IT - Wildfire-U. S."
        samples = scraper.normalize_records([record])

        analysis, _ = scraper.build_analysis_table(samples, {"O3": 1})

        self.assertEqual(analysis.loc[0, "O3"], 0.05)
        self.assertFalse(analysis.loc[0, "O3_quality_flagged"])


if __name__ == "__main__":
    unittest.main()
