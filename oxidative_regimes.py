"""Oxidative-regime classification and descriptive summaries.

This module is intentionally descriptive.  It assigns observations to the
four requested Leighton-ratio intervals and summarizes measured or derived
variables without attaching atmospheric interpretations to the intervals.
"""

from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd


OXIDATIVE_REGIME_LABELS = (
    "LR < 1",
    "1 <= LR < 1.5",
    "1.5 <= LR < 2",
    "LR >= 2",
)

OXIDATIVE_REGIME_VARIABLE_UNITS = {
    "NO": "ppb",
    "NO2": "ppb",
    "O3": "ppm",
    "NOx": "ppb",
    "J": "s^-1",
    "UV": "W m^-2",
    "SR_W_m2": "W m^-2",
    "solar_zenith_angle_deg": "degrees",
    "Temp": "degrees F",
    "clearing_index": "dimensionless",
}


def classify_oxidative_regime(lr: pd.Series) -> pd.Series:
    """Classify finite LR values into the four requested half-open intervals.

    Missing and non-finite LR values remain unclassified rather than being
    silently assigned to an interval.
    """

    numeric_lr = pd.to_numeric(lr, errors="coerce")
    numeric_lr = numeric_lr.where(np.isfinite(numeric_lr))
    classified = pd.cut(
        numeric_lr,
        bins=[-np.inf, 1.0, 1.5, 2.0, np.inf],
        labels=OXIDATIVE_REGIME_LABELS,
        right=False,
        ordered=True,
    )
    return pd.Series(classified, index=lr.index, name="oxidative_regime")


def summarize_oxidative_regimes(
    data: pd.DataFrame,
    variable_units: Mapping[str, str] = OXIDATIVE_REGIME_VARIABLE_UNITS,
) -> pd.DataFrame:
    """Return a tidy per-regime descriptive-statistics table.

    Every requested regime-variable pair is emitted, including empty regimes
    and variables with no valid observations. ``regime_rows`` counts all rows
    assigned to the regime; ``valid_count`` and ``missing_count`` make each
    variable's availability explicit.
    """

    if "LR" not in data:
        raise ValueError("Oxidative-regime summaries require an LR column")

    missing = sorted(set(variable_units).difference(data.columns))
    if missing:
        raise ValueError(
            "Oxidative-regime summary is missing requested variables: "
            + ", ".join(missing)
        )

    regimes = classify_oxidative_regime(data["LR"])
    rows: list[dict[str, object]] = []
    for regime in OXIDATIVE_REGIME_LABELS:
        regime_mask = regimes.eq(regime)
        regime_rows = int(regime_mask.sum())
        for variable, unit in variable_units.items():
            values = pd.to_numeric(
                data.loc[regime_mask, variable], errors="coerce"
            ).replace([np.inf, -np.inf], np.nan)
            valid = values.dropna()
            valid_count = int(valid.count())
            rows.append(
                {
                    "oxidative_regime": regime,
                    "variable": variable,
                    "unit": unit,
                    "regime_rows": regime_rows,
                    "valid_count": valid_count,
                    "missing_count": regime_rows - valid_count,
                    "mean": float(valid.mean()) if valid_count else np.nan,
                    "std": float(valid.std(ddof=1)) if valid_count > 1 else np.nan,
                    "min": float(valid.min()) if valid_count else np.nan,
                    "p25": float(valid.quantile(0.25)) if valid_count else np.nan,
                    "median": float(valid.median()) if valid_count else np.nan,
                    "p75": float(valid.quantile(0.75)) if valid_count else np.nan,
                    "max": float(valid.max()) if valid_count else np.nan,
                }
            )

    return pd.DataFrame(rows)
