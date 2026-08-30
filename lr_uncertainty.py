"""Uncertainty utilities for the Leighton ratio.

The functions in this module report a *quantified partial uncertainty*.  They
do not imply a complete uncertainty budget: concentration, temperature/rate
constant, quantum-yield, and absorption-cross-section uncertainties have not
yet been supplied for this project.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Mapping, Sequence

import pandas as pd


APOGEE_SU200_MAY21_RELATIVE_UNCERTAINTIES = {
    "calibration": 0.05,
    "repeatability": 0.005,
    "non_linearity": 0.01,
    "cosine_response": 0.02,
}

# These remain explicit so a partial instrument budget cannot be mistaken for
# a complete uncertainty estimate for LR = J * NO2 / (K * O3 * NO).
CURRENTLY_UNQUANTIFIED_TERMS = (
    "NO measurement",
    "NO2 measurement",
    "O3 measurement",
    "temperature and K",
    "NO2 quantum yield",
    "NO2 absorption cross section",
    "long-term sensor drift for the measurement date",
)


def combine_independent_relative_uncertainties(
    relative_uncertainties: Sequence[float],
) -> float:
    """Combine independent one-sigma-like relative uncertainties by RSS.

    Inputs and output are fractions (for example, ``0.05`` means 5%).  The
    caller is responsible for establishing that the supplied terms are
    independent and use a compatible uncertainty convention.
    """

    values = [float(value) for value in relative_uncertainties]
    if not values:
        raise ValueError("At least one relative uncertainty is required")
    if any(not 0 <= value < float("inf") for value in values):
        raise ValueError("Relative uncertainties must be finite and non-negative")
    return sqrt(sum(value**2 for value in values))


def apogee_su200_may21_relative_uncertainty() -> float:
    """Return the combined supplied SU-200-SS uncertainty for May 21."""

    return combine_independent_relative_uncertainties(
        APOGEE_SU200_MAY21_RELATIVE_UNCERTAINTIES.values()
    )


@dataclass(frozen=True)
class LeightonUncertaintyBudget:
    """A partial uncertainty budget, separated from unquantified terms."""

    quantified_relative_uncertainties: Mapping[str, float]
    combined_quantified_relative_uncertainty: float
    unquantified_terms: tuple[str, ...]

    def absolute_uncertainty(self, leighton_ratio: float) -> float:
        """Return the absolute partial uncertainty for one LR value."""

        return abs(float(leighton_ratio)) * self.combined_quantified_relative_uncertainty


def propagate_leighton_relative_uncertainty(
    quantified_relative_uncertainties: Mapping[str, float],
    *,
    unquantified_terms: Sequence[str] = CURRENTLY_UNQUANTIFIED_TERMS,
) -> LeightonUncertaintyBudget:
    """Propagate independent quantified terms for ``LR = J*NO2/(K*O3*NO)``.

    For independent multiplicative and divisive inputs, relative uncertainty
    contributions add in quadrature.  Mapping keys are retained as provenance,
    so inputs can be broad variables (``"NO"``) or narrower components
    (``"Apogee calibration"``).  No missing term is assigned an invented value.
    """

    quantified = {
        str(name): float(value)
        for name, value in quantified_relative_uncertainties.items()
    }
    combined = combine_independent_relative_uncertainties(quantified.values())
    return LeightonUncertaintyBudget(
        quantified_relative_uncertainties=quantified,
        combined_quantified_relative_uncertainty=combined,
        unquantified_terms=tuple(unquantified_terms),
    )


def add_quantified_lr_uncertainty(
    frame: pd.DataFrame,
    relative_uncertainty_columns: Mapping[str, str],
    *,
    lr_column: str = "LR",
) -> pd.DataFrame:
    """Add row-level partial LR uncertainty columns to a copied DataFrame.

    ``relative_uncertainty_columns`` maps provenance labels to columns holding
    fractional relative uncertainties.  Available values are combined per row;
    missing values remain unquantified.  ``LR_uncertainty_component_count``
    makes varying row-level coverage visible.
    """

    if lr_column not in frame:
        raise KeyError(f"Missing LR column: {lr_column}")
    if not relative_uncertainty_columns:
        raise ValueError("At least one uncertainty column is required")
    missing = [
        column
        for column in relative_uncertainty_columns.values()
        if column not in frame
    ]
    if missing:
        raise KeyError(f"Missing uncertainty columns: {missing}")

    result = frame.copy()
    uncertainty = result[list(relative_uncertainty_columns.values())].astype(float)
    if (uncertainty < 0).any().any():
        raise ValueError("Relative uncertainties must be non-negative")

    counts = uncertainty.notna().sum(axis=1)
    combined = uncertainty.pow(2).sum(axis=1, min_count=1).pow(0.5)
    result["LR_quantified_relative_uncertainty"] = combined
    result["LR_quantified_uncertainty_percent"] = combined * 100.0
    result["LR_quantified_absolute_uncertainty"] = (
        result[lr_column].abs() * combined
    )
    result["LR_uncertainty_component_count"] = counts
    return result
