"""Reproducible Hawthorne Leighton-ratio analysis.

This script replaces the archived calculation notebook. It loads the typed,
POC-resolved AQS analysis table, joins the Hawthorne UV series, performs the
same kinetic and concentration calculations, and exports reviewed figures and
processed data.

The J(NO2) calculation uses Callum Flowerday's provisional NCAR TUV transfer
function from the August 14 and August 18, 2026 implementation emails. The
fixed pressure remains an independently unvalidated scientific assumption.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Literal

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lr_uncertainty import (
    APOGEE_SU200_MAY21_RELATIVE_UNCERTAINTIES,
    CURRENTLY_UNQUANTIFIED_TERMS,
    add_quantified_lr_uncertainty,
    apogee_su200_may21_relative_uncertainty,
)
from sr_ci_filter import apply_sr_ci_filters


DEFAULT_AQS_PATH = Path(
    "data/downloads/Hawthorne/20240101_20260728_20260729T004631Z/"
    "aqs_analysis_ready.parquet"
)
DEFAULT_UV_PATH = Path("data/UV Data/UV Data All Time HW LP RB.csv")
DEFAULT_OUTPUT_DIR = Path("output/leighton_analysis")

# Kinetic and concentration assumptions retained from the archived notebook.
REACTION_PREFACTOR = 3.0e-12
ACTIVATION_OVER_R = 1500.0
PRESSURE_PA = 87_000.0
BOLTZMANN_J_PER_K = 1.380649e-23
F298 = 1.07
G_TEMPERATURE = 130.0
LANGLEY_PER_MINUTE_TO_W_M2 = 41_840.0 / 60.0

# Provisional state-UV transfer function from Callum Flowerday's August 14,
# 2026 email, with the August 18 correction that it replaces the complete
# archived ``7.784e-5 * UV / 2.84 * 10`` expression. UV is in W m^-2, solar
# zenith angle is in degrees, and J(NO2) is in s^-1.
TUV_SZA_INTERCEPT_M2_W_S = 1.538e-4
TUV_SZA_SLOPE_M2_W_S_DEG = 1.951e-6
J_CALIBRATION_SOURCE = "Callum Flowerday emails dated 2026-08-14 and 2026-08-18"

# Hawthorne AQS site 49-035-3006 coordinates from the source AQS snapshot.
HAWTHORNE_LATITUDE_DEG = 40.736389
HAWTHORNE_LONGITUDE_DEG = -111.872222

REQUIRED_MEASUREMENTS = ["UV", "NO", "NO2", "O3", "Temp"]
EXPECTED_POCS = {"NO": 2, "NO2": 3, "O3": 1, "SR": 1, "Temp": 1}
NO_SENSITIVITY_THRESHOLDS_PPB = (0.0, 0.05, 0.10, 0.20, 0.50, 1.0)


@dataclass(frozen=True)
class AnalysisConfig:
    year: int = 2025
    month: int = 5
    minimum_uv: float = 10.0
    daytime_start: str = "10:00"
    daytime_end: str = "16:00"
    minimum_no_ppb: float = 0.20
    minimum_no_operator: Literal[">", ">="] = ">"
    site_latitude_deg: float = HAWTHORNE_LATITUDE_DEG
    site_longitude_deg: float = HAWTHORNE_LONGITUDE_DEG
    apply_sr_ci: bool = False
    sr_threshold_w_m2: float = 710.0
    clearing_index_threshold: int = 1000
    target_airshed: str = "Northern Wasatch Front"
    uv_hour_shift: int | None = None
    uv_alignment_max_shift: int = 3


def calculate_solar_zenith_angle(
    datetime_utc: pd.Series,
    latitude_deg: float,
    longitude_deg: float,
) -> pd.Series:
    """Calculate solar zenith angle with the NOAA solar equations."""
    timestamps = pd.to_datetime(datetime_utc, utc=True, errors="coerce")
    if timestamps.isna().any():
        raise ValueError("SZA calculation requires valid UTC timestamps")

    day_of_year = timestamps.dt.dayofyear.to_numpy(dtype=float)
    fractional_hour = (
        timestamps.dt.hour.to_numpy(dtype=float)
        + timestamps.dt.minute.to_numpy(dtype=float) / 60.0
        + timestamps.dt.second.to_numpy(dtype=float) / 3600.0
    )
    fractional_year = (
        2.0
        * np.pi
        / 365.0
        * (day_of_year - 1.0 + (fractional_hour - 12.0) / 24.0)
    )
    equation_of_time_minutes = 229.18 * (
        0.000075
        + 0.001868 * np.cos(fractional_year)
        - 0.032077 * np.sin(fractional_year)
        - 0.014615 * np.cos(2.0 * fractional_year)
        - 0.040849 * np.sin(2.0 * fractional_year)
    )
    declination_rad = (
        0.006918
        - 0.399912 * np.cos(fractional_year)
        + 0.070257 * np.sin(fractional_year)
        - 0.006758 * np.cos(2.0 * fractional_year)
        + 0.000907 * np.sin(2.0 * fractional_year)
        - 0.002697 * np.cos(3.0 * fractional_year)
        + 0.00148 * np.sin(3.0 * fractional_year)
    )
    true_solar_time_minutes = np.mod(
        fractional_hour * 60.0
        + equation_of_time_minutes
        + 4.0 * longitude_deg,
        1440.0,
    )
    hour_angle_rad = np.deg2rad(true_solar_time_minutes / 4.0 - 180.0)
    latitude_rad = np.deg2rad(latitude_deg)
    cosine_zenith = (
        np.sin(latitude_rad) * np.sin(declination_rad)
        + np.cos(latitude_rad)
        * np.cos(declination_rad)
        * np.cos(hour_angle_rad)
    )
    zenith_deg = np.rad2deg(np.arccos(np.clip(cosine_zenith, -1.0, 1.0)))
    return pd.Series(zenith_deg, index=datetime_utc.index, dtype=float)


def calculate_tuv_j_no2(
    uv_w_m2: pd.Series,
    solar_zenith_angle_deg: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    """Apply Callum's provisional SZA-dependent TUV transfer function."""
    coefficient = (
        TUV_SZA_INTERCEPT_M2_W_S
        + TUV_SZA_SLOPE_M2_W_S_DEG * solar_zenith_angle_deg
    )
    return coefficient * uv_w_m2, coefficient


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
    apply_primary_no_filter: bool = True,
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

    in_primary_window = data.between_time(
        config.daytime_start,
        config.daytime_end,
        inclusive="both",
    )
    accounting["primary_time_window_rows"] = len(in_primary_window)
    no_mask = apply_no_threshold(
        in_primary_window["NO"],
        config.minimum_no_ppb,
        config.minimum_no_operator,
    )
    accounting["primary_no_threshold_rows"] = int(no_mask.sum())
    data = (
        in_primary_window.loc[no_mask].copy()
        if apply_primary_no_filter
        else in_primary_window.copy()
    )
    if data.empty:
        raise ValueError(
            "No rows remain after the primary time-window and NO filters"
        )

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


def apply_no_threshold(
    no_ppb: pd.Series,
    threshold_ppb: float,
    operator: Literal[">", ">="],
) -> pd.Series:
    """Return the declared NO-threshold mask without modifying source values."""
    if operator == ">":
        return no_ppb.gt(threshold_ppb)
    if operator == ">=":
        return no_ppb.ge(threshold_ppb)
    raise ValueError(f"Unsupported minimum NO operator: {operator!r}")


def summarize_no_threshold_sensitivity(
    data: pd.DataFrame,
    thresholds_ppb: tuple[float, ...] = NO_SENSITIVITY_THRESHOLDS_PPB,
) -> pd.DataFrame:
    """Summarize LR under strict NO cutoffs for a reproducible sensitivity table."""
    rows = []
    for threshold in thresholds_ppb:
        selected = data.loc[data["NO"].gt(threshold), "LR"]
        rows.append(
            {
                "minimum_no_ppb": threshold,
                "operator": ">",
                "count": int(selected.count()),
                "median_lr": float(selected.median()) if not selected.empty else None,
                "p90_lr": float(selected.quantile(0.90)) if not selected.empty else None,
                "max_lr": float(selected.max()) if not selected.empty else None,
            }
        )
    return pd.DataFrame(rows)


def calculate_leighton_ratio(
    data: pd.DataFrame,
    config: AnalysisConfig | None = None,
) -> pd.DataFrame:
    """Calculate J(NO2), concentrations, and the Leighton ratio."""
    config = config or AnalysisConfig()
    result = data.copy()
    result["Temp_K"] = (result["Temp"] - 32.0) * (5.0 / 9.0) + 273.15
    result["K"] = REACTION_PREFACTOR * np.exp(
        -ACTIVATION_OVER_R / result["Temp_K"]
    )
    result["f_T"] = F298 * np.exp(
        G_TEMPERATURE * (1.0 / result["Temp_K"] - 1.0 / 298.0)
    )
    if "datetime_utc" not in result:
        raise ValueError("J(NO2) calculation requires datetime_utc")
    result["solar_zenith_angle_deg"] = calculate_solar_zenith_angle(
        result["datetime_utc"],
        config.site_latitude_deg,
        config.site_longitude_deg,
    )
    result["J"], result["j_conversion_coefficient_m2_w_s"] = calculate_tuv_j_no2(
        result["UV"],
        result["solar_zenith_angle_deg"],
    )
    result["j_calibration_method"] = "provisional_tuv_sza_linear"

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
    result["log10_LR"] = np.log10(result["LR"])
    result["apogee_su200_may21_relative_uncertainty"] = (
        apogee_su200_may21_relative_uncertainty()
    )
    result = add_quantified_lr_uncertainty(
        result,
        {
            "Apogee SU-200-SS supplied May 21 components": (
                "apogee_su200_may21_relative_uncertainty"
            )
        },
    )
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
    axis.errorbar(
        data.index,
        data["LR"],
        yerr=data["LR_quantified_absolute_uncertainty"],
        fmt="o",
        color="#D97706",
        ecolor="#F59E0B",
        markeredgecolor="#92400E",
        markeredgewidth=0.45,
        markersize=6.5,
        elinewidth=0.8,
        capsize=2,
        alpha=0.82,
        label="Hourly observation with partial quantified uncertainty",
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
    filter_text = (
        f"UV > {config.minimum_uv:g} W/m² · NO {config.minimum_no_operator} "
        f"{config.minimum_no_ppb:g} ppb · "
        f"{config.daytime_start}–{config.daytime_end} LST"
    )
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
        "Error bars show only the quantified Apogee contribution (5.5%); full LR uncertainty is unavailable.",
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
            "time_window_inclusive": "both",
            "time_standard": "local_standard_time",
            "minimum_no_ppb": config.minimum_no_ppb,
            "minimum_no_operator": config.minimum_no_operator,
            "site_latitude_deg": config.site_latitude_deg,
            "site_longitude_deg": config.site_longitude_deg,
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
            "j_calibration_method": "provisional_tuv_sza_linear",
            "j_calibration_source": J_CALIBRATION_SOURCE,
            "uv_units": "W m^-2",
            "solar_zenith_angle_units": "degrees",
            "j_units": "s^-1",
            "tuv_sza_intercept_m2_w_s": TUV_SZA_INTERCEPT_M2_W_S,
            "tuv_sza_slope_m2_w_s_deg": TUV_SZA_SLOPE_M2_W_S_DEG,
            "sza_method": "NOAA fractional-year solar-position approximation",
            "state_uv_area_or_scale_correction": "none",
            "fixed_pressure_pa": PRESSURE_PA,
            "timestamp_join": (
                "UV timestamp shifted by the selected integer-hour lag, then "
                "matched exactly to AQS Local Standard Time"
            ),
        },
        "uncertainty": {
            "status": "partial_quantified_uncertainty",
            "sensor_model": "Apogee SU-200-SS",
            "geometry": "normal May 21 geometry supplied by Callum",
            "quantified_relative_components": (
                APOGEE_SU200_MAY21_RELATIVE_UNCERTAINTIES
            ),
            "combined_quantified_relative_uncertainty": (
                apogee_su200_may21_relative_uncertainty()
            ),
            "combination_method": "root_sum_of_squares_assuming_independence",
            "unquantified_terms": CURRENTLY_UNQUANTIFIED_TERMS,
            "plot_interval_label": "partial quantified uncertainty",
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
    candidate_data, accounting = load_selected_measurements(
        aqs_path,
        uv_path,
        config,
        apply_primary_no_filter=False,
    )
    candidate_data = calculate_leighton_ratio(candidate_data, config)
    sensitivity = summarize_no_threshold_sensitivity(candidate_data)
    primary_mask = apply_no_threshold(
        candidate_data["NO"],
        config.minimum_no_ppb,
        config.minimum_no_operator,
    )
    data = candidate_data.loc[primary_mask].copy()
    accounting["primary_no_threshold_rows"] = len(data)
    accounting["analysis_rows"] = len(data)
    if data.empty:
        raise ValueError("No rows remain after the primary NO filter")
    accounting["finite_ratio_rows"] = len(data)
    daytime, outside = split_time_windows(data, config)

    processed_path = output_dir / "leighton_ratio_may_2025.parquet"
    summary_path = output_dir / "summary.json"
    timeseries_path = output_dir / "leighton_ratio_timeseries.png"
    distributions_path = output_dir / "leighton_ratio_distributions.png"
    correction_path = output_dir / "temperature_correction.png"
    alignment_path = output_dir / "uv_alignment_diagnostic.png"
    no_sensitivity_path = output_dir / "no_threshold_sensitivity.csv"

    data.reset_index().to_parquet(processed_path, index=False)
    sensitivity.to_csv(no_sensitivity_path, index=False)
    summary = summarize(data, daytime, outside, accounting, config)
    summary["no_threshold_sensitivity"] = sensitivity.replace(
        {np.nan: None}
    ).to_dict(orient="records")
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
    parser.add_argument("--minimum-no", type=float, default=0.20)
    parser.add_argument(
        "--minimum-no-operator",
        choices=(">", ">="),
        default=">",
        help="comparison used for the primary NO cutoff (default: strict >)",
    )
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
        minimum_no_ppb=args.minimum_no,
        minimum_no_operator=args.minimum_no_operator,
        apply_sr_ci=args.apply_sr_ci,
        sr_threshold_w_m2=args.sr_threshold,
        clearing_index_threshold=args.ci_threshold,
        target_airshed=args.airshed,
        uv_hour_shift=args.uv_hour_shift,
    )
    run_analysis(args.aqs, args.uv, args.output_dir, config)


if __name__ == "__main__":
    main()
