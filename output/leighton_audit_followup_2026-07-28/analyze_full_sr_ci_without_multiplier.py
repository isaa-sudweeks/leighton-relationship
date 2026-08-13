"""Analyze the full available Hawthorne overlap with SR/CI filtering.

The calculation follows the current repository pipeline, uses the full exact
timestamp overlap between the immutable Hawthorne AQS snapshot and Hawthorne
UV series, applies UV > 10, SR >= 710 W/m^2, and Northern Wasatch Front
clearing index <= 1000, and removes the unverified 10/2.84 multiplier from LR.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from leighton_relationship_analysis import (
    LANGLEY_PER_MINUTE_TO_W_M2,
    calculate_leighton_ratio,
)
from sr_ci_filter import build_clearing_index_history, resolve_air_shed_id


ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = Path(__file__).resolve().parent
AQS_PATH = ROOT / (
    "data/downloads/Hawthorne/20240101_20260728_20260729T004631Z/"
    "aqs_analysis_ready.parquet"
)
UV_PATH = ROOT / "data/UV Data/UV Data All Time HW LP RB.csv"

MINIMUM_UV = 10.0
SR_THRESHOLD_W_M2 = 710.0
CI_THRESHOLD = 1000
TARGET_AIRSHED = "Northern Wasatch Front"
J_MULTIPLIER_TO_REMOVE = 10.0 / 2.84
CI_HISTORY_CACHE = OUTPUT_DIR / "full_available_ci_history_air_shed_5.parquet"


def load_uv() -> pd.DataFrame:
    uv = pd.read_csv(UV_PATH)
    uv = uv.rename(columns={uv.columns[0]: "uv_datetime", "HW": "UV"})
    uv["uv_datetime"] = pd.to_datetime(
        uv["uv_datetime"], format="mixed", errors="coerce"
    )
    uv["UV"] = pd.to_numeric(uv["UV"], errors="coerce")
    uv = uv[["uv_datetime", "UV"]].dropna(subset=["uv_datetime"])
    if uv["uv_datetime"].duplicated().any():
        raise ValueError("UV input contains duplicate timestamps")
    return uv


def alignment_diagnostics(
    aqs: pd.DataFrame, uv: pd.DataFrame
) -> tuple[int, list[dict[str, float | int]]]:
    diagnostics: list[dict[str, float | int]] = []
    for shift in range(-3, 4):
        shifted = uv.copy()
        shifted["datetime_local_standard"] = (
            shifted["uv_datetime"] + pd.to_timedelta(shift, unit="h")
        )
        paired = aqs[["datetime_local_standard", "SR"]].merge(
            shifted[["datetime_local_standard", "UV"]],
            on="datetime_local_standard",
            how="inner",
            validate="one_to_one",
        )
        paired = paired.dropna(subset=["UV", "SR"])
        diagnostics.append(
            {
                "shift_hours": shift,
                "paired_rows": len(paired),
                "uv_sr_pearson_r": float(paired["UV"].corr(paired["SR"])),
            }
        )
    selected = max(diagnostics, key=lambda row: row["uv_sr_pearson_r"])
    return int(selected["shift_hours"]), diagnostics


def load_and_filter_base() -> tuple[pd.DataFrame, dict[str, object]]:
    aqs = pd.read_parquet(AQS_PATH)
    aqs["datetime_local_standard"] = pd.to_datetime(
        aqs["datetime_local_standard"]
    )
    if aqs["datetime_utc"].duplicated().any():
        raise ValueError("AQS input contains duplicate UTC timestamps")
    if aqs["datetime_local_standard"].duplicated().any():
        raise ValueError("AQS input contains duplicate local-standard timestamps")

    uv = load_uv()
    shift, diagnostics = alignment_diagnostics(aqs, uv)
    shifted = uv.copy()
    shifted["datetime_local_standard"] = (
        shifted["uv_datetime"] + pd.to_timedelta(shift, unit="h")
    )

    merged = aqs.merge(
        shifted[["datetime_local_standard", "uv_datetime", "UV"]],
        on="datetime_local_standard",
        how="inner",
        validate="one_to_one",
    ).set_index("datetime_local_standard")
    merged = merged.sort_index()

    accounting: dict[str, object] = {
        "aqs_rows": len(aqs),
        "aqs_start": str(aqs["datetime_local_standard"].min()),
        "aqs_end": str(aqs["datetime_local_standard"].max()),
        "uv_rows": len(uv),
        "uv_start": str(uv["uv_datetime"].min()),
        "uv_end": str(uv["uv_datetime"].max()),
        "exact_overlap_rows": len(merged),
        "overlap_start": str(merged.index.min()),
        "overlap_end": str(merged.index.max()),
        "uv_alignment_candidates": diagnostics,
        "uv_selected_shift_hours": shift,
    }

    merged["UV"] = merged["UV"].where(merged["UV"] > MINIMUM_UV)
    accounting["uv_threshold_rows"] = int(merged["UV"].notna().sum())

    required = ["UV", "NO", "NO2", "O3", "Temp", "SR"]
    base = merged.dropna(subset=required).copy()
    accounting["complete_required_rows"] = len(base)

    base["SR_W_m2"] = base["SR"] * LANGLEY_PER_MINUTE_TO_W_M2
    base = base[base["SR_W_m2"].ge(SR_THRESHOLD_W_M2)].copy()
    accounting["sr_threshold_rows"] = len(base)
    if base.empty:
        raise ValueError("No rows remain after the SR threshold")
    return base, accounting


def add_clearing_index(
    data: pd.DataFrame, accounting: dict[str, object]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    local_zone = ZoneInfo("America/Denver")
    start_local = datetime.combine(data.index.date.min(), datetime.min.time())
    end_local = datetime.combine(data.index.date.max(), datetime.max.time())
    start_utc = (start_local - timedelta(days=2)).replace(
        tzinfo=local_zone
    ).astimezone(ZoneInfo("UTC"))
    end_utc = (end_local + timedelta(days=2)).replace(
        tzinfo=local_zone
    ).astimezone(ZoneInfo("UTC"))

    history_source = "downloaded_iem_afos_archive"
    if CI_HISTORY_CACHE.exists():
        cached = pd.read_parquet(CI_HISTORY_CACHE)
        cached_dates = pd.to_datetime(cached["valid_date"]).dt.date
        if (
            cached_dates.min() <= data.index.date.min()
            and cached_dates.max() >= data.index.date.max()
        ):
            history = cached
            history_source = "cached_prior_download"
        else:
            history = build_clearing_index_history(
                start_utc, end_utc, "America/Denver"
            )
    else:
        history = build_clearing_index_history(
            start_utc, end_utc, "America/Denver"
        )
    air_shed_id = resolve_air_shed_id(TARGET_AIRSHED)
    history = history[history["air_shed"].eq(air_shed_id)].copy()
    if history.empty:
        raise RuntimeError("No Northern Wasatch Front CI records were returned")
    history["ci_valid_date"] = pd.to_datetime(history["valid_date"]).dt.date
    history = history.drop_duplicates("ci_valid_date", keep="last")

    work = data.copy()
    work["ci_valid_date"] = work.index.date
    merged = (
        work.reset_index()
        .merge(
            history[
                [
                    "ci_valid_date",
                    "clearing_index",
                    "clearing_index_token",
                    "clearing_index_is_lower_bound",
                    "issue_time_local",
                ]
            ],
            on="ci_valid_date",
            how="left",
            validate="many_to_one",
        )
        .set_index("datetime_local_standard")
        .sort_index()
    )

    missing_ci = merged["clearing_index"].isna()
    qualified = (
        merged["clearing_index_is_lower_bound"]
        .astype("boolean")
        .fillna(False)
    )
    above_ci = merged["clearing_index"].gt(CI_THRESHOLD)
    accounting["ci_history_rows_air_shed_5"] = len(history)
    accounting["ci_history_source"] = history_source
    accounting["ci_history_start"] = str(history["ci_valid_date"].min())
    accounting["ci_history_end"] = str(history["ci_valid_date"].max())
    accounting["missing_ci_rows_after_sr"] = int(missing_ci.sum())
    accounting["qualified_ci_rows_after_sr"] = int(qualified.sum())
    accounting["above_ci_threshold_rows"] = int(above_ci.fillna(False).sum())

    retained = merged[
        ~missing_ci
        & ~qualified
        & merged["clearing_index"].le(CI_THRESHOLD)
    ].copy()
    accounting["sr_ci_rows"] = len(retained)
    if retained.empty:
        raise ValueError("No rows remain after the clearing-index filter")
    return retained, history


def plot_results(data: pd.DataFrame, summary: dict[str, object]) -> Path:
    monthly = (
        data["LR_without_10_over_2_84"]
        .resample("MS")
        .agg(["mean", "median", "count"])
    )

    fig, (top, bottom) = plt.subplots(
        2,
        1,
        figsize=(13, 9.2),
        sharex=True,
        gridspec_kw={"height_ratios": [2.1, 1.0], "hspace": 0.12},
    )
    fig.subplots_adjust(left=0.09, right=0.98, top=0.83, bottom=0.13)

    top.scatter(
        data.index,
        data["LR_without_10_over_2_84"],
        s=24,
        color="#D97706",
        edgecolor="#78350F",
        linewidth=0.35,
        alpha=0.68,
        label="Hourly observation",
        zorder=3,
    )
    top.axhline(
        1.0,
        color="#374151",
        linestyle="--",
        linewidth=1.4,
        label="Unity reference (LR = 1)",
    )
    top.axhline(
        summary["mean"],
        color="#2563EB",
        linewidth=1.8,
        label=f"Overall mean = {summary['mean']:.3f}",
    )
    top.axhline(
        summary["median"],
        color="#6D28D9",
        linewidth=1.6,
        linestyle=":",
        label=f"Overall median = {summary['median']:.3f}",
    )
    top.set_yscale("log")
    top.set_ylabel("Leighton ratio (log scale)")
    top.legend(frameon=False, loc="upper right", ncol=2)

    bottom.plot(
        monthly.index,
        monthly["mean"],
        color="#2563EB",
        marker="o",
        linewidth=2.0,
        label="Monthly mean",
    )
    bottom.plot(
        monthly.index,
        monthly["median"],
        color="#6D28D9",
        marker="s",
        linewidth=1.8,
        linestyle="--",
        label="Monthly median",
    )
    bottom.axhline(1.0, color="#374151", linestyle="--", linewidth=1.2)
    bottom.set_ylabel("Monthly LR")
    bottom.set_xlabel("Local Standard Time")
    bottom.set_ylim(bottom=0)
    bottom.legend(frameon=False, loc="upper right", ncol=2)

    for axis in (top, bottom):
        axis.spines[["top", "right"]].set_visible(False)
        axis.grid(axis="y", color="#D1D5DB", linewidth=0.7, alpha=0.65)
        axis.tick_params(colors="#374151")

    locator = mdates.MonthLocator(interval=2)
    bottom.xaxis.set_major_locator(locator)
    bottom.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    padding = pd.Timedelta(5, unit="D")
    bottom.set_xlim(data.index.min() - padding, data.index.max() + padding)

    fig.suptitle(
        "Full Available SR/CI-Filtered Leighton Ratio Without 10/2.84",
        x=0.09,
        y=0.965,
        ha="left",
        fontsize=17,
        fontweight="bold",
        color="#111827",
    )
    fig.text(
        0.09,
        0.895,
        (
            f"Hawthorne · {summary['start']} to {summary['end']} · "
            f"n={summary['count']} hourly observations · "
            f"mean={summary['mean']:.3f} · median={summary['median']:.3f}"
        ),
        fontsize=10.5,
        color="#4B5563",
    )
    fig.text(
        0.98,
        0.025,
        (
            "Filters: UV > 10, SR ≥ 710 W/m², Northern Wasatch Front CI ≤ 1000; "
            "qualified CI lower bounds excluded. UV-to-J(NO₂) calibration remains unverified."
        ),
        ha="right",
        va="bottom",
        fontsize=8.4,
        color="#6B7280",
    )

    destination = OUTPUT_DIR / (
        "full_available_sr_ci_leighton_ratio_without_10_over_2_84.png"
    )
    fig.savefig(destination, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return destination


def main() -> None:
    base, accounting = load_and_filter_base()
    filtered, history = add_clearing_index(base, accounting)
    result = calculate_leighton_ratio(filtered)
    result["LR_without_10_over_2_84"] = (
        result["LR"] / J_MULTIPLIER_TO_REMOVE
    )

    adjusted = result["LR_without_10_over_2_84"]
    retained_hours = sorted(int(value) for value in result.index.hour.unique())
    summary = {
        "count": len(result),
        "start": result.index.min().strftime("%Y-%m-%d %H:%M"),
        "end": result.index.max().strftime("%Y-%m-%d %H:%M"),
        "mean": float(adjusted.mean()),
        "median": float(adjusted.median()),
        "p10": float(adjusted.quantile(0.10)),
        "p90": float(adjusted.quantile(0.90)),
        "minimum": float(adjusted.min()),
        "maximum": float(adjusted.max()),
        "standard_deviation": float(adjusted.std()),
        "retained_local_standard_hours": retained_hours,
        "rows_outside_06_to_20": int(
            ((result.index.hour < 6) | (result.index.hour > 20)).sum()
        ),
        "calculation": "LR_without_10_over_2_84 = stored-formula LR / (10/2.84)",
        "accounting": accounting,
    }

    result.reset_index().to_parquet(
        OUTPUT_DIR / "full_available_sr_ci_without_multiplier.parquet",
        index=False,
    )
    history.to_parquet(CI_HISTORY_CACHE, index=False)
    (OUTPUT_DIR / "full_available_sr_ci_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    plot_path = plot_results(result, summary)

    print(json.dumps(summary, indent=2))
    print(f"Saved plot: {plot_path}")


if __name__ == "__main__":
    main()
