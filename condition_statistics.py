"""Generate condition plots and pairwise correlation statistics.

The report consumes the compact hourly diagnostics schema written by
``leighton_relationship_analysis.py``.  It is intentionally descriptive: it
does not assign atmospheric causes to the observed relationships.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr


@dataclass(frozen=True)
class RelationshipSpec:
    """One requested response-predictor relationship."""

    response: str
    response_label: str
    response_units: str
    predictor: str
    predictor_label: str
    predictor_units: str
    filename: str


PREDICTORS = {
    "NO": ("NO", "ppb", "no"),
    "NO2": ("NO2", "ppb", "no2"),
    "O3": ("O3", "ppm", "o3"),
    "NOx": ("NOx", "ppb", "nox"),
    "J": ("J(NO2)", "s^-1", "j"),
    "UV": ("UV", "W m^-2", "uv"),
    "SR_W_m2": ("Solar radiation", "W m^-2", "solar_radiation"),
    "solar_zenith_angle_deg": ("Solar zenith angle", "degrees", "sza"),
    "Temp": ("Temperature", "degrees F", "temperature"),
    "clearing_index": ("Clearing index", "dimensionless", "clearing_index"),
}


def _relationship_specs() -> tuple[RelationshipSpec, ...]:
    lr_predictors = tuple(PREDICTORS)
    rox_predictors = ("NOx", "Temp", "UV", "SR_W_m2", "J", "clearing_index")
    specifications: list[RelationshipSpec] = []
    for response, response_label, response_units, prefix, predictors in (
        ("LR", "Leighton ratio", "dimensionless", "lr", lr_predictors),
        (
            "ROx_equiv_molecules_cm3",
            "Equivalent RO2",
            "molecule cm^-3",
            "rox_equiv",
            rox_predictors,
        ),
    ):
        for predictor in predictors:
            predictor_label, predictor_units, suffix = PREDICTORS[predictor]
            specifications.append(
                RelationshipSpec(
                    response=response,
                    response_label=response_label,
                    response_units=response_units,
                    predictor=predictor,
                    predictor_label=predictor_label,
                    predictor_units=predictor_units,
                    filename=f"{prefix}_vs_{suffix}.png",
                )
            )
    return tuple(specifications)


RELATIONSHIPS = _relationship_specs()
STATISTICS_FILENAME = "condition_correlations.csv"
METADATA_FILENAME = "condition_report_metadata.json"
CONDITION_REPORT_FILENAMES = (
    STATISTICS_FILENAME,
    METADATA_FILENAME,
    *(specification.filename for specification in RELATIONSHIPS),
)


def _pairwise_finite(
    data: pd.DataFrame,
    predictor: str,
    response: str,
) -> pd.DataFrame:
    """Return numeric rows where both requested values are finite."""

    pair = pd.DataFrame(
        {
            predictor: pd.to_numeric(data[predictor], errors="coerce"),
            response: pd.to_numeric(data[response], errors="coerce"),
        }
    )
    finite = np.isfinite(pair[predictor]) & np.isfinite(pair[response])
    return pair.loc[finite]


def _status_for_pair(pair: pd.DataFrame, specification: RelationshipSpec) -> str:
    if len(pair) < 3:
        return "insufficient_pairs"
    predictor_constant = pair[specification.predictor].nunique(dropna=False) < 2
    response_constant = pair[specification.response].nunique(dropna=False) < 2
    if predictor_constant and response_constant:
        return "constant_predictor_and_response"
    if predictor_constant:
        return "constant_predictor"
    if response_constant:
        return "constant_response"
    return "ok"


def calculate_condition_statistics(
    diagnostics: pd.DataFrame,
    relationships: Iterable[RelationshipSpec] = RELATIONSHIPS,
) -> pd.DataFrame:
    """Calculate pairwise-valid Pearson and Spearman statistics.

    Non-numeric, missing, positive-infinite, and negative-infinite values are
    excluded independently for every relationship.  Correlations and p-values
    are reported only with at least three pairs and variation in both inputs.
    """

    relationships = tuple(relationships)
    required = {
        column
        for specification in relationships
        for column in (specification.response, specification.predictor)
    }
    missing = sorted(required.difference(diagnostics.columns))
    if missing:
        raise ValueError(
            "Condition report is missing required diagnostics columns: "
            + ", ".join(missing)
        )

    rows: list[dict[str, object]] = []
    for specification in relationships:
        pair = _pairwise_finite(
            diagnostics,
            specification.predictor,
            specification.response,
        )
        status = _status_for_pair(pair, specification)
        pearson_r = pearson_p = spearman_rho = spearman_p = np.nan
        if status == "ok":
            pearson = pearsonr(
                pair[specification.predictor], pair[specification.response]
            )
            spearman = spearmanr(
                pair[specification.predictor], pair[specification.response]
            )
            pearson_r = float(pearson.statistic)
            pearson_p = float(pearson.pvalue)
            spearman_rho = float(spearman.statistic)
            spearman_p = float(spearman.pvalue)

        rows.append(
            {
                "response": specification.response,
                "response_label": specification.response_label,
                "response_units": specification.response_units,
                "predictor": specification.predictor,
                "predictor_label": specification.predictor_label,
                "predictor_units": specification.predictor_units,
                "total_rows": len(diagnostics),
                "pairwise_valid_n": len(pair),
                "excluded_nonfinite_or_missing_n": len(diagnostics) - len(pair),
                "status": status,
                "pearson_r": pearson_r,
                "pearson_p_value": pearson_p,
                "spearman_rho": spearman_rho,
                "spearman_p_value": spearman_p,
                "plot": specification.filename,
            }
        )
    return pd.DataFrame(rows)


def _axis_label(label: str, units: str) -> str:
    return label if units == "dimensionless" else f"{label} ({units})"


def _style_axis(axis: plt.Axes) -> None:
    axis.grid(True, color="#E5E7EB", linewidth=0.8, alpha=0.8)
    axis.spines["top"].set_visible(False)
    axis.spines["right"].set_visible(False)
    axis.spines["left"].set_color("#9CA3AF")
    axis.spines["bottom"].set_color("#9CA3AF")
    axis.tick_params(colors="#374151")


def plot_condition_relationship(
    diagnostics: pd.DataFrame,
    specification: RelationshipSpec,
    destination: Path,
    context: str | None = None,
) -> None:
    """Plot one requested relationship using only its pairwise-valid rows."""

    pair = _pairwise_finite(
        diagnostics,
        specification.predictor,
        specification.response,
    )
    status = _status_for_pair(pair, specification)
    fig, axis = plt.subplots(figsize=(8.6, 6.2))
    fig.subplots_adjust(left=0.13, right=0.98, top=0.84, bottom=0.14)
    if pair.empty:
        axis.text(
            0.5,
            0.5,
            "No pairwise-valid observations",
            transform=axis.transAxes,
            ha="center",
            va="center",
            color="#4B5563",
            fontsize=11,
        )
    else:
        plotted_response = pair[specification.response]
        if specification.response == "ROx_equiv_molecules_cm3":
            plotted_response = plotted_response / 1e9
        axis.scatter(
            pair[specification.predictor],
            plotted_response,
            s=25,
            color="#2563EB",
            edgecolors="#1E3A8A",
            linewidths=0.35,
            alpha=0.58,
        )
    if specification.response == "LR":
        if not pair.empty:
            axis.axhline(1.0, color="#374151", linewidth=1.3, linestyle="--")
        if pair.empty or pair[specification.response].min() >= 0:
            axis.set_ylim(bottom=0)
    elif not pair.empty and pair[specification.response].min() >= 0:
        axis.set_ylim(bottom=0)

    title = f"{specification.response_label} vs {specification.predictor_label}"
    if context:
        title += f" — {context}"
    axis.set_title(
        title,
        loc="left",
        fontsize=15,
        fontweight="bold",
        color="#111827",
        pad=18,
    )
    excluded = len(diagnostics) - len(pair)
    subtitle = f"n={len(pair)} pairwise-valid hourly observations"
    if excluded:
        subtitle += f" · {excluded} excluded as missing/non-finite"
    if status != "ok":
        subtitle += f" · statistics status: {status}"
    axis.text(
        0,
        1.01,
        subtitle,
        transform=axis.transAxes,
        fontsize=9.5,
        color="#4B5563",
        va="bottom",
    )
    axis.set_xlabel(
        _axis_label(specification.predictor_label, specification.predictor_units)
    )
    response_units = specification.response_units
    if specification.response == "ROx_equiv_molecules_cm3":
        response_units = "10^9 molecule cm^-3"
    axis.set_ylabel(_axis_label(specification.response_label, response_units))
    _style_axis(axis)
    fig.savefig(destination, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def generate_condition_report(
    diagnostics: pd.DataFrame,
    output_dir: Path,
    context: str | None = None,
    source_name: str | None = None,
) -> dict[str, object]:
    """Write every requested plot, the statistics CSV, and method metadata."""

    output_dir.mkdir(parents=True, exist_ok=True)
    statistics = calculate_condition_statistics(diagnostics)
    statistics_path = output_dir / STATISTICS_FILENAME
    statistics.to_csv(statistics_path, index=False)
    for specification in RELATIONSHIPS:
        plot_condition_relationship(
            diagnostics,
            specification,
            output_dir / specification.filename,
            context=context,
        )

    status_counts = {
        str(status): int(count)
        for status, count in statistics["status"].value_counts().items()
    }
    metadata: dict[str, object] = {
        "schema_version": 1,
        "purpose": (
            "Descriptive condition plots and pairwise correlations; "
            "no scientific interpretation"
        ),
        "source": source_name,
        "context": context,
        "input_rows": len(diagnostics),
        "relationships": len(RELATIONSHIPS),
        "statistics": STATISTICS_FILENAME,
        "plots": [specification.filename for specification in RELATIONSHIPS],
        "status_counts": status_counts,
        "missing_value_policy": (
            "Each relationship independently excludes rows where either value "
            "is missing, non-numeric, or non-finite."
        ),
        "statistics_policy": (
            "Pearson r and two-sided p-value plus Spearman rho and two-sided "
            "p-value are reported only for at least three pairwise-valid rows "
            "with non-constant predictor and response."
        ),
    }
    (output_dir / METADATA_FILENAME).write_text(
        json.dumps(metadata, indent=2) + "\n",
        encoding="utf-8",
    )
    return metadata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("diagnostics", type=Path, help="hourly diagnostics Parquet")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--context", help="optional period/site label for plot titles")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    diagnostics = pd.read_parquet(args.diagnostics)
    metadata = generate_condition_report(
        diagnostics,
        args.output_dir,
        context=args.context,
        source_name=str(args.diagnostics),
    )
    print(
        f"Wrote {metadata['relationships']} condition plots and statistics "
        f"for {metadata['input_rows']} rows to {args.output_dir}"
    )


if __name__ == "__main__":
    main()
