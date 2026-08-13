from datetime import date
import unittest
from unittest.mock import patch

import pandas as pd

import sr_ci_filter


class SrCiFilterTests(unittest.TestCase):
    def test_disabled_filter_returns_input_unchanged(self):
        data = pd.DataFrame(
            {"SR_Synoptic": [100.0]},
            index=pd.DatetimeIndex(["2025-05-01 12:00"], name="datetime"),
        )

        result = sr_ci_filter.apply_sr_ci_filters(data, enabled=False)

        self.assertIs(result, data)

    def test_enabled_filter_matches_improved_isopleths_cutoffs(self):
        data = pd.DataFrame(
            {"SR_Synoptic": [709.0, 710.0, 900.0, 900.0]},
            index=pd.DatetimeIndex(
                [
                    "2025-05-01 12:00",
                    "2025-05-01 13:00",
                    "2025-05-02 12:00",
                    "2025-05-03 12:00",
                ],
                name="datetime",
            ),
        )
        history = pd.DataFrame(
            {
                "air_shed": [5, 5, 5],
                "issue_time_local": pd.to_datetime(
                    [
                        "2025-05-01 12:00",
                        "2025-05-02 12:00",
                        "2025-05-03 12:00",
                    ],
                    utc=True,
                ),
                "valid_date": [
                    date(2025, 5, 1),
                    date(2025, 5, 2),
                    date(2025, 5, 3),
                ],
                "period_label": ["TODAY", "TODAY", "TODAY"],
                "clearing_index": [1000, 1001, 999],
            }
        )

        with patch.object(
            sr_ci_filter,
            "build_clearing_index_history",
            return_value=history,
        ):
            result = sr_ci_filter.apply_sr_ci_filters(data, enabled=True)

        self.assertEqual(
            result.index.tolist(),
            [
                pd.Timestamp("2025-05-01 13:00"),
                pd.Timestamp("2025-05-03 12:00"),
            ],
        )
        self.assertEqual(result["clearing_index"].tolist(), [1000, 999])

    def test_qualified_clearing_index_lower_bound_is_excluded(self):
        data = pd.DataFrame(
            {"SR_Synoptic": [800.0]},
            index=pd.DatetimeIndex(["2025-05-01 12:00"], name="datetime"),
        )
        history = pd.DataFrame(
            {
                "air_shed": [5],
                "issue_time_local": pd.to_datetime(
                    ["2025-05-01 03:00"], utc=True
                ),
                "valid_date": [date(2025, 5, 1)],
                "period_label": ["TODAY"],
                "clearing_index": [1000],
                "clearing_index_token": ["1000+"],
                "clearing_index_is_lower_bound": [True],
            }
        )

        with patch.object(
            sr_ci_filter,
            "build_clearing_index_history",
            return_value=history,
        ):
            result = sr_ci_filter.apply_sr_ci_filters(data, enabled=True)

        self.assertTrue(result.empty)


if __name__ == "__main__":
    unittest.main()
