"""Plot the SR/CI-filtered LR after removing the 10/2.84 multiplier."""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
INPUT = ROOT / (
    "output/leighton_analysis_sr_ci_uv_aligned/"
    "leighton_ratio_may_2025.parquet"
)
OUTPUT = Path(__file__).resolve().with_name(
    "sr_ci_leighton_ratio_without_10_over_2_84.png"
)


def main() -> None:
    data = pd.read_parquet(INPUT).sort_values("datetime_local_standard")
    data["LR_without_10_over_2_84"] = data["LR"] / (10.0 / 2.84)

    median = float(data["LR_without_10_over_2_84"].median())
    count = len(data)

    fig, axis = plt.subplots(figsize=(12, 6.8))
    fig.subplots_adjust(left=0.09, right=0.98, top=0.80, bottom=0.18)

    axis.scatter(
        data["datetime_local_standard"],
        data["LR_without_10_over_2_84"],
        s=54,
        color="#D97706",
        edgecolor="#78350F",
        linewidth=0.65,
        alpha=0.88,
        label="Hourly observation",
        zorder=3,
    )
    axis.axhline(
        1.0,
        color="#374151",
        linewidth=1.5,
        linestyle="--",
        label="Unity reference (LR = 1)",
        zorder=2,
    )
    axis.axhline(
        median,
        color="#2563EB",
        linewidth=1.8,
        label=f"Median = {median:.3f}",
        zorder=2,
    )

    axis.set_title(
        "SR/CI-Filtered Leighton Ratio Without the 10/2.84 Multiplier",
        loc="left",
        fontsize=16,
        fontweight="bold",
        color="#111827",
        pad=34,
    )
    axis.text(
        0,
        1.025,
        (
            f"Hawthorne · May 2025 · n={count} hourly observations · "
            "SR ≥ 710 W/m² · clearing index ≤ 1000"
        ),
        transform=axis.transAxes,
        fontsize=10.5,
        color="#4B5563",
        va="bottom",
    )
    axis.set_xlabel("Local Standard Time")
    axis.set_ylabel("Leighton ratio (dimensionless)")
    axis.set_ylim(bottom=0)

    locator = mdates.AutoDateLocator(minticks=6, maxticks=10)
    axis.xaxis.set_major_locator(locator)
    axis.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))

    axis.spines[["top", "right"]].set_visible(False)
    axis.grid(axis="y", color="#D1D5DB", linewidth=0.8, alpha=0.7)
    axis.tick_params(colors="#374151")
    axis.legend(frameon=False, loc="upper right")

    fig.text(
        0.98,
        0.025,
        (
            "Sensitivity plot: adjusted LR = stored LR ÷ (10/2.84). "
            "The UV-to-J(NO₂) calibration remains unverified."
        ),
        ha="right",
        va="bottom",
        fontsize=8.5,
        color="#6B7280",
    )
    fig.savefig(OUTPUT, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)

    print(f"Saved {OUTPUT}")
    print(f"Rows={count}; median={median:.12f}")


if __name__ == "__main__":
    main()
