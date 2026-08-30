#!/usr/bin/env python3
"""Validate and inventory matched primary/SR-CI deliverables for Callum.

This script does not run the scientific analysis or interpret its results.  It
checks that two already-generated output directories use matching settings,
apart from the intentionally different ``apply_sr_ci`` switch, and writes a
SHA-256 manifest for the files that would be attached.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any


EXPECTED_ARTIFACTS = (
    "summary.json",
    "leighton_ratio_may_2025.parquet",
    "no_threshold_sensitivity.csv",
    "leighton_ratio_timeseries.png",
    "leighton_ratio_distributions.png",
    "temperature_correction.png",
    "uv_alignment_diagnostic.png",
)
SOURCE_AUDIT_ARTIFACTS = (
    "manifest.json",
    "analysis_manifest.json",
    "analysis_row_accounting.parquet",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_summary(directory: Path) -> dict[str, Any]:
    path = directory / "summary.json"
    if not path.is_file():
        raise ValueError(f"Missing required summary: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def matched_configuration(
    primary: dict[str, Any], sensitivity: dict[str, Any]
) -> tuple[bool, dict[str, dict[str, Any]]]:
    primary_config = dict(primary.get("configuration", {}))
    sensitivity_config = dict(sensitivity.get("configuration", {}))
    primary_switch = primary_config.pop("apply_sr_ci", None)
    sensitivity_switch = sensitivity_config.pop("apply_sr_ci", None)
    differences = {
        key: {"primary": primary_config.get(key), "sr_ci": sensitivity_config.get(key)}
        for key in sorted(set(primary_config) | set(sensitivity_config))
        if primary_config.get(key) != sensitivity_config.get(key)
    }
    valid = primary_switch is False and sensitivity_switch is True and not differences
    if primary_switch is not False or sensitivity_switch is not True:
        differences["apply_sr_ci"] = {
            "primary": primary_switch,
            "sr_ci": sensitivity_switch,
        }
    return valid, differences


def build_manifest(
    primary_dir: Path,
    sensitivity_dir: Path,
    source_snapshot_dir: Path | None = None,
) -> dict[str, Any]:
    primary_summary = load_summary(primary_dir)
    sensitivity_summary = load_summary(sensitivity_dir)
    matched, differences = matched_configuration(primary_summary, sensitivity_summary)
    if not matched:
        raise ValueError(
            "Primary and SR/CI outputs are not a matched pair: "
            + json.dumps(differences, sort_keys=True)
        )

    artifacts: list[dict[str, Any]] = []
    missing: list[str] = []
    for label, directory in (("primary", primary_dir), ("sr_ci", sensitivity_dir)):
        for name in EXPECTED_ARTIFACTS:
            path = directory / name
            if not path.is_file():
                missing.append(f"{label}/{name}")
                continue
            artifacts.append(
                {
                    "analysis": label,
                    "name": name,
                    "path": str(path.resolve()),
                    "bytes": path.stat().st_size,
                    "sha256": sha256(path),
                }
            )
    if source_snapshot_dir is not None:
        for name in SOURCE_AUDIT_ARTIFACTS:
            path = source_snapshot_dir / name
            if not path.is_file():
                missing.append(f"aqs_source_audit/{name}")
                continue
            artifacts.append(
                {
                    "analysis": "aqs_source_audit",
                    "name": name,
                    "path": str(path.resolve()),
                    "bytes": path.stat().st_size,
                    "sha256": sha256(path),
                }
            )
    if missing:
        raise ValueError("Missing expected artifacts: " + ", ".join(missing))

    return {
        "schema_version": 1,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "purpose": "Callum review package; no chemistry interpretation included",
        "matched_configuration": True,
        "primary_configuration": primary_summary["configuration"],
        "sr_ci_configuration": sensitivity_summary["configuration"],
        "artifacts": artifacts,
        "limitations": [
            "The state UV CSV has no row-level quality flags or calibration manifest.",
            "AQS cleaning and row accounting are documented in the source snapshot.",
            "Scientific interpretation is deferred to Callum and Jaron.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--primary-dir", type=Path, required=True)
    parser.add_argument("--sr-ci-dir", type=Path, required=True)
    parser.add_argument(
        "--source-snapshot-dir",
        type=Path,
        help="AQS snapshot containing source, analysis, and row-accounting manifests",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    manifest = build_manifest(
        args.primary_dir, args.sr_ci_dir, args.source_snapshot_dir
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote manifest for {len(manifest['artifacts'])} files to {args.output}")


if __name__ == "__main__":
    main()
