"""Reproducible Hawthorne Leighton-ratio analysis.

This script replaces the archived calculation notebook. It loads the typed,
POC-resolved AQS analysis table, joins the Hawthorne UV series, performs the
same kinetic and concentration calculations, and exports reviewed figures and
processed data.

The J(NO2) calibration constants and fixed pressure reproduce the archived
notebook and remain scientific assumptions to validate independently.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from sr_ci_filter import apply_sr_ci_filters


DEFAULT_AQS_PATH = Path(
    "data/downloads/Hawthorne/20240101_20260728_20260729T004631Z/"
    "aqs_analysis_ready.parquet"
)
DEFAULT_UV_PATH = Path("data/UV Data/UV Data All Time HW LP RB.csv")
DEFAULT_OUTPUT_DIR = Path("output/leighton_analysis")

# Archived-notebook assumptions.
REACTION_PREFACTOR = 3.0e-12
ACTIVATION_OVER_R = 1500.0
J_CALIBRATION_CONSTANT = 7.784e-5
UV_SENSOR_AREA_CM2 = 2.84
J_SCALE_FACTOR = 10.0
PRESSURE_PA = 87_000.0
BOLTZMANN_J_PER_K = 1.380649e-23
F298 = 1.07
G_TEMPERATURE = 130.0
LANGLEY_PER_MINUTE_TO_W_M2 = 41_840.0 / 60.0

REQUIRED_MEASUREMENTS = ["UV", "NO", "NO2", "O3", "Temp"]
EXPECTED_POCS = {"NO": 2, "NO2": 3, "O3": 1, "SR": 1, "Temp": 1}


@dataclass(frozen=True)
class AnalysisConfig:
    year: int = 2025
    month: int = 5
    minimum_uv: float = 10.0
    daytime_start: str = "09:00"
    daytime_end: str = "16:00"
    apply_sr_ci: bool = False
    sr_threshold_w_m2: float = 710.0
    clearing_index_threshold: int = 1000
    target_airshed: str = "Northern Wasatch Front"
    uv_hour_shift: int | None = None
    uv_alignment_max_shift: int = 3


def evaluate_uv_alignment(
    aqs: pd.DataFrame,
    uv: pd.DataFrame,
    config: AnalysisConfig,
) -> tuple[int, list[dict[str, float | int]]]:
    """Select the UV clock shift that best agrees with collocated AQS SR.

    A shift is applied to the UV timestamp before matching it to AQS local
    standard time. For example, -1 maps a UV value stamped 13:00 to the AQS
    row stamped 12:00. The automatic choice maximizes the Pearson correlation
    between UV and collocated solar radiation for the requested month.
    """
    period = aqs.loc[
        (aqs["datetime_local_standard"].dt.year == config.year)
        & (aqs["datetime_local_standard"].dt.month == config.month)
    ]
    diagnostics: list[dict[str, float | int]] = []
    shifts = (
        [config.uv_hour_shift]
        if config.uv_hour_shift is not None
        else range(-config.uv_alignment_max_shift, config.uv_alignment_max_shift + 1)
    )
    for shift in shifts:
        shifted = uv.copy()
        shifted["datetime_local_standard"] = (
            shifted["uv_datetime"] + pd.to_timedelta(int(shift), unit="h")
        )
        paired = period.merge(
            shifted[["datetime_local_standard", "UV"]],
            on="datetime_local_standard",
            how="inner",
        ).dropna(subset=["UV", "SR"])
        correlation = paired["UV"].corr(paired["SR"])
        diagnostics.append(
            {
                "shift_hours": int(shift),
                "paired_rows": int(len(paired)),
                "uv_sr_pearson_r": (
                    float(correlation) if pd.notna(correlation) else float("nan")
                ),
            }
        )

    usable = [
        row
        for row in diagnostics
        if row["paired_rows"] >= 24 and np.isfinite(row["uv_sr_pearson_r"])
    ]
    if not usable:
        raise ValueError(
            "Insufficient paired UV and AQS solar-radiation data to align timestamps"
        )
    selected = max(usable, key=lambda row: row["uv_sr_pearson_r"])
    return int(selected["shift_hours"]), diagnostics


def load_selected_measurements(
    aqs_path: Path,
    uv_path: Path,
    config: AnalysisConfig,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Load selected POCs, join UV, and apply declared analysis filters."""
    aqs = pd.read_parquet(aqs_path)
    if aqs["datetime_utc"].duplicated().any():
        raise ValueError("AQS analysis table contains duplicate UTC timestamps")

    for parameter, expected_poc in EXPECTED_POCS.items():
        column = f"{parameter}_poc"
        observed = set(aqs[column].dropna().astype(int).unique())
        if observed != {expected_poc}:
            raise ValueError(
                f"{parameter} expected POC {expected_poc}, observed {sorted(observed)}"
            )

    aqs["datetime_utc"] = pd.to_datetime(aqs["datetime_utc"], utc=True)
    aqs["datetime_local_standard"] = pd.to_datetime(
        aqs["datetime_local_standard"]
    )
    uv = pd.read_csv(uv_path)
    uv = uv.rename(columns={uv.columns[0]: "uv_datetime", "HW": "UV"})
    uv["uv_datetime"] = pd.to_datetime(
        uv["uv_datetime"], format="mixed", errors="coerce"
    )
    uv = uv[["uv_datetime", "UV"]].dropna(subset=["uv_datetime"])

    accounting = {"aqs_rows": len(aqs)}
    selected_shift, alignment_diagnostics = evaluate_uv_alignment(aqs, uv, config)
    accounting["uv_alignment_method"] = (
        "explicit_shift"
        if config.uv_hour_shift is not None
        else "maximum_UV_SR_Pearson_correlation"
    )
    accounting["uv_selected_shift_hours"] = selected_shift
    accounting["uv_alignment_candidates"] = alignment_diagnostics

    uv["datetime_local_standard"] = (
        uv["uv_datetime"] + pd.to_timedelta(int(selected_shift), unit="h")
    )
    data = aqs.merge(
        uv[["datetime_local_standard", "uv_datetime", "UV"]],
        on="datetime_local_standard",
        how="inner",
    )
    data = data.set_index("datetime_local_standard").sort_index()
    accounting["exact_uv_matches"] = len(data)

    in_period = (data.index.year == config.year) & (
        data.index.month == config.month
    )
    data = data.loc[in_period].copy()
    accounting["period_rows"] = len(data)

    data["UV"] = data["UV"].where(data["UV"] > config.minimum_uv)
    accounting["uv_threshold_rows"] = int(data["UV"].notna().sum())

    data = data.dropna(subset=REQUIRED_MEASUREMENTS)
    accounting["base_analysis_rows"] = len(data)
    if data.empty:
        raise ValueError("No rows remain after the declared analysis filters")

    data["SR_W_m2"] = data["SR"] * LANGLEY_PER_MINUTE_TO_W_M2
    if config.apply_sr_ci:
        accounting["sr_threshold_rows"] = int(
            data["SR_W_m2"].ge(config.sr_threshold_w_m2).sum()
        )
        data = apply_sr_ci_filters(
            data,
            enabled=True,
            sr_column="SR_W_m2",
            sr_threshold=config.sr_threshold_w_m2,
            clearing_index_threshold=config.clearing_index_threshold,
            target_airshed=config.target_airshed,
        )
        accounting["sr_ci_filtered_rows"] = len(data)
        if data.empty:
            raise ValueError("No rows remain after SR and clearing-index filters")

    accounting["analysis_rows"] = len(data)
    return data, accounting


def calculate_leighton_ratio(data: pd.DataFrame) -> pd.DataFrame:
    """Reproduce the archived notebook's physical calculations."""
    result = data.copy()
    result["Temp_K"] = (result["Temp"] - 32.0) * (5.0 / 9.0) + 273.15
    result["K"] = REACTION_PREFACTOR * np.exp(
        -ACTIVATION_OVER_R / result["Temp_K"]
    )
    result["f_T"] = F298 * np.exp(
        G_TEMPERATURE * (1.0 / result["Temp_K"] - 1.0 / 298.0)
    )
    result["J"] = (
        J_CALIBRATION_CONSTANT
        * result["UV"]
        / UV_SENSOR_AREA_CM2
        * J_SCALE_FACTOR
    )

    number_density = (
        PRESSURE_PA / (BOLTZMANN_J_PER_K * result["Temp_K"])
    ) / 1e6
    result["air_number_density_cm3"] = number_density
    result["O3_molecules_cm3"] = result["O3"] * 1e-6 * number_density
    result["NO_molecules_cm3"] = result["NO"] * 1e-9 * number_density
    result["NO2_molecules_cm3"] = result["NO2"] * 1e-9 * number_density
    result["LR"] = (
        result["J"] * result["NO2_molecules_cm3"]
    ) / (
        result["K"]
        * result["O3_molecules_cm3"]
        * result["NO_molecules_cm3"]
    )

    result["LR"] = result["LR"].replace([np.inf, -np.inf], np.nan)
    result = result.dropna(subset=["LR"])
    if result.empty:
        raise ValueError("No finite Leighton-ratio values were calculated")
    return result


def split_time_windows(
    data: pd.DataFrame,
    config: AnalysisConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create non-overlapping clock-time groups."""
    daytime = data.between_time(
        config.daytime_start,
        config.daytime_end,
        inclusive="both",
    )
    outside = data.loc[~data.index.isin(daytime.index)]
    if len(daytime) + len(outside) != len(data):
        raise AssertionError("Time-window split did not preserve every row exactly once")
    return daytime, outside


def _style_axis(axis: plt.Axes) -> None:
    axis.spines[["top", "right"]].set_visible(False)
    axis.grid(axis="y", color="#D1D5DB", linewidth=0.7, alpha=0.65)
    axis.tick_params(colors="#374151")


def plot_ratio_timeseries(
    data: pd.DataFrame,
    config: AnalysisConfig,
    destination: Path,
) -> None:
    """Plot the calculated ratio by observation time."""
    fig, axis = plt.subplots(figsize=(11, 6.2))
    fig.subplots_adjust(left=0.09, right=0.985, top=0.84, bottom=0.18)
    axis.scatter(
        data.index,
        data["LR"],
        color="#D97706",
        edgecolor="#92400E",
        linewidth=0.45,
        s=42,
        alpha=0.82,
        label="Hourly observation",
    )
    axis.axhline(
        1.0,
        color="#374151",
        linewidth=1.4,
        linestyle="--",
        label="Unity reference",
    )
    axis.set_title(
        f"Leighton Ratio at Hawthorne — {config.year}-{config.month:02d}",
        loc="left",
        fontsize=16,
        fontweight="bold",
        color="#111827",
        pad=18,
    )
    filter_text = f"UV > {config.minimum_uv:g}"
    if config.apply_sr_ci:
        filter_text += (
            f" · SR ≥ {config.sr_threshold_w_m2:g} W/m²"
            f" · CI ≤ {config.clearing_index_threshold}"
        )
    axis.text(
        0,
        1.01,
        (
            f"n={len(data)} hourly values · NO POC 2 · NO₂ POC 3 · "
            f"{filter_text}"
        ),
        transform=axis.transAxes,
        fontsize=10,
        color="#4B5563",
        va="bottom",
    )
    axis.set_ylabel("Leighton ratio (dimensionless)")
    axis.set_xlabel("Local Standard Time")
    axis.set_ylim(bottom=0)
    locator = mdates.AutoDateLocator(minticks=5, maxticks=10)
    axis.xaxis.set_major_locator(locator)
    axis.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    axis.legend(frameon=False, loc="upper left")
    _style_axis(axis)
    fig.text(
        0.985,
        0.025,
        "J(NO₂) calibration and fixed 87 kPa pressure reproduce archived assumptions.",
        ha="right",
        va="bottom",
        fontsize=8,
        color="#6B7280",
    )
    fig.savefig(destination, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_ratio_distributions(
    daytime: pd.DataFrame,
    outside: pd.DataFrame,
    config: AnalysisConfig,
    destination: Path,
) -> None:
    """Plot non-overlapping time-window distributions."""
    nonempty = [frame for frame in [daytime, outside] if not frame.empty]
    maximum = max(frame["LR"].max() for frame in nonempty)
    bins = np.linspace(0, np.ceil(maximum), 22)
    fig, axes = plt.subplots(
        1,
        2,
        figsize=(11, 5.4),
        sharex=True,
        sharey=True,
        constrained_layout=True,
    )
    panels = [
        (
            axes[0],
            daytime,
            "#D97706",
            f"{config.daytime_start}–{config.daytime_end}",
        ),
        (axes[1], outside, "#2563EB", "Outside clock window"),
    ]
    for axis, frame, color, label in panels:
        if frame.empty:
            axis.text(
                0.5,
                0.5,
                "No retained observations",
                transform=axis.transAxes,
                ha="center",
                va="center",
                fontsize=12,
                color="#6B7280",
            )
            axis.set_title(f"{label}\nn=0", fontsize=12, color="#111827")
            axis.set_xlabel("Leighton ratio")
            _style_axis(axis)
            continue
        axis.hist(
            frame["LR"],
            bins=bins,
            color=color,
            edgecolor="#111827",
            linewidth=0.6,
            alpha=0.78,
        )
        axis.axvline(1.0, color="#374151", linewidth=1.4, linestyle="--")
        median = frame["LR"].median()
        axis.axvline(median, color=color, linewidth=2.0)
        axis.set_title(
            f"{label}\nn={len(frame)}, median={median:.2f}",
            fontsize=12,
            color="#111827",
        )
        axis.set_xlabel("Leighton ratio")
        _style_axis(axis)
    axes[0].set_ylabel("Hourly observations")
    fig.suptitle(
        "Leighton-Ratio Distributions by Non-Overlapping Clock Window",
        fontsize=15,
        fontweight="bold",
        color="#111827",
    )
    fig.savefig(destination, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_temperature_correction(data: pd.DataFrame, destination: Path) -> None:
    """Plot the temperature correction factor retained from the notebook."""
    ordered = data.sort_index()
    smoothed = ordered["f_T"].rolling(window=25, center=True, min_periods=8).mean()
    fig, axis = plt.subplots(figsize=(11, 5.4), constrained_layout=True)
    axis.scatter(
        ordered.index,
        ordered["f_T"],
        color="#9CA3AF",
        s=30,
        alpha=0.65,
        label="Hourly value",
    )
    axis.plot(
        ordered.index,
        smoothed,
        color="#2563EB",
        linewidth=2.0,
        label="25-observation rolling mean",
    )
    axis.set_title(
        "Temperature Correction Factor at Hawthorne",
        loc="left",
        fontsize=15,
        fontweight="bold",
        color="#111827",
    )
    axis.set_xlabel("Local Standard Time")
    axis.set_ylabel("f(T)")
    locator = mdates.AutoDateLocator(minticks=5, maxticks=10)
    axis.xaxis.set_major_locator(locator)
    axis.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    axis.legend(frameon=False)
    _style_axis(axis)
    fig.savefig(destination, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_uv_alignment_diagnostic(
    diagnostics: list[dict[str, float | int]],
    selected_shift: int,
    config: AnalysisConfig,
    destination: Path,
) -> None:
    """Plot UV–SR agreement for each tested integer-hour alignment."""
    shifts = [int(row["shift_hours"]) for row in diagnostics]
    correlations = [float(row["uv_sr_pearson_r"]) for row in diagnostics]
    counts = [int(row["paired_rows"]) for row in diagnostics]
    colors = ["#D97706" if shift == selected_shift else "#93C5FD" for shift in shifts]

    fig, axis = plt.subplots(figsize=(8.5, 5.4))
    fig.subplots_adjust(left=0.12, right=0.98, top=0.78, bottom=0.18)
    bars = axis.bar(
        shifts,
        correlations,
        color=colors,
        edgecolor="#1F2937",
        linewidth=0.7,
    )
    axis.bar_label(
        bars,
        labels=[f"{value:.3f}" for value in correlations],
        padding=4,
        fontsize=9,
        color="#374151",
    )
    fig.suptitle(
        "UV Timestep Alignment Against AQS Solar Radiation",
        x=0.12,
        y=0.96,
        ha="left",
        fontsize=15,
        fontweight="bold",
        color="#111827",
    )
    fig.text(
        0.12,
        0.86,
        (
            f"{config.year}-{config.month:02d} hourly pairs · Pearson r · "
            f"selected shift={selected_shift:+d} h · "
            f"candidate n={min(counts)}–{max(counts)}"
        ),
        fontsize=10,
        color="#4B5563",
        va="center",
    )
    axis.set_xlabel("Shift applied to UV timestamp before AQS local-standard join (hours)")
    axis.set_ylabel("UV–SR Pearson correlation")
    axis.set_ylim(0, 1.08)
    axis.set_xticks(shifts)
    _style_axis(axis)
    fig.savefig(destination, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def summarize(
    data: pd.DataFrame,
    daytime: pd.DataFrame,
    outside: pd.DataFrame,
    accounting: dict[str, object],
    config: AnalysisConfig,
) -> dict[str, object]:
    """Create a machine-readable replacement for notebook print statements."""
    def statistics(frame: pd.DataFrame) -> dict[str, float | int | None]:
        if frame.empty:
            return {
                "count": 0,
                "median": None,
                "p10": None,
                "p90": None,
                "minimum": None,
                "maximum": None,
            }
        return {
            "count": len(frame),
            "median": float(frame["LR"].median()),
            "p10": float(frame["LR"].quantile(0.10)),
            "p90": float(frame["LR"].quantile(0.90)),
            "minimum": float(frame["LR"].min()),
            "maximum": float(frame["LR"].max()),
        }

    return {
        "configuration": {
            "year": config.year,
            "month": config.month,
            "minimum_uv": config.minimum_uv,
            "daytime_start": config.daytime_start,
            "daytime_end": config.daytime_end,
            "poc_selections": EXPECTED_POCS,
            "apply_sr_ci": config.apply_sr_ci,
            "sr_threshold_w_m2": config.sr_threshold_w_m2,
            "clearing_index_threshold": config.clearing_index_threshold,
            "target_airshed": config.target_airshed,
            "uv_hour_shift": config.uv_hour_shift,
            "uv_alignment_max_shift": config.uv_alignment_max_shift,
        },
        "row_accounting": accounting,
        "all_observations": statistics(data),
        "daytime_window": statistics(daytime),
        "outside_window": statistics(outside),
        "scientific_assumptions_to_validate": {
            "j_calibration_constant": J_CALIBRATION_CONSTANT,
            "uv_sensor_area_cm2": UV_SENSOR_AREA_CM2,
            "j_scale_factor": J_SCALE_FACTOR,
            "fixed_pressure_pa": PRESSURE_PA,
            "timestamp_join": (
                "UV timestamp shifted by the selected integer-hour lag, then "
                "matched exactly to AQS Local Standard Time"
            ),
        },
    }


def run_analysis(
    aqs_path: Path,
    uv_path: Path,
    output_dir: Path,
    config: AnalysisConfig,
) -> dict[str, object]:
    """Run the full analysis and export data, summaries, and figures."""
    output_dir.mkdir(parents=True, exist_ok=True)
    data, accounting = load_selected_measurements(aqs_path, uv_path, config)
    data = calculate_leighton_ratio(data)
    accounting["finite_ratio_rows"] = len(data)
    daytime, outside = split_time_windows(data, config)

    processed_path = output_dir / "leighton_ratio_may_2025.parquet"
    summary_path = output_dir / "summary.json"
    timeseries_path = output_dir / "leighton_ratio_timeseries.png"
    distributions_path = output_dir / "leighton_ratio_distributions.png"
    correction_path = output_dir / "temperature_correction.png"
    alignment_path = output_dir / "uv_alignment_diagnostic.png"

    data.reset_index().to_parquet(processed_path, index=False)
    summary = summarize(data, daytime, outside, accounting, config)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    plot_ratio_timeseries(data, config, timeseries_path)
    plot_ratio_distributions(daytime, outside, config, distributions_path)
    plot_temperature_correction(data, correction_path)
    plot_uv_alignment_diagnostic(
        accounting["uv_alignment_candidates"],
        accounting["uv_selected_shift_hours"],
        config,
        alignment_path,
    )

    print(f"Saved {len(data)} analyzed rows to {processed_path}")
    print(
        "Leighton ratio: "
        f"median={data['LR'].median():.3f}, "
        f"p10={data['LR'].quantile(0.10):.3f}, "
        f"p90={data['LR'].quantile(0.90):.3f}"
    )
    print(
        f"Clock windows: {len(daytime)} rows from "
        f"{config.daytime_start}–{config.daytime_end}; "
        f"{len(outside)} outside"
    )
    print(f"Saved figures to {output_dir}")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate and plot the Hawthorne Leighton ratio"
    )
    parser.add_argument("--aqs", type=Path, default=DEFAULT_AQS_PATH)
    parser.add_argument("--uv", type=Path, default=DEFAULT_UV_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--month", type=int, default=5)
    parser.add_argument("--minimum-uv", type=float, default=10.0)
    parser.add_argument(
        "--apply-sr-ci",
        action="store_true",
        help="apply collocated AQS SR and Northern Wasatch Front CI filters",
    )
    parser.add_argument("--sr-threshold", type=float, default=710.0)
    parser.add_argument("--ci-threshold", type=int, default=1000)
    parser.add_argument(
        "--airshed",
        default="Northern Wasatch Front",
    )
    parser.add_argument(
        "--uv-hour-shift",
        type=int,
        default=None,
        help=(
            "integer hours added to UV timestamps before the AQS local-standard "
            "join; omit to select the best shift from -3 through +3 using UV–SR"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = AnalysisConfig(
        year=args.year,
        month=args.month,
        minimum_uv=args.minimum_uv,
        apply_sr_ci=args.apply_sr_ci,
        sr_threshold_w_m2=args.sr_threshold,
        clearing_index_threshold=args.ci_threshold,
        target_airshed=args.airshed,
        uv_hour_shift=args.uv_hour_shift,
    )
    run_analysis(args.aqs, args.uv, args.output_dir, config)


if __name__ == "__main__":
    main()
