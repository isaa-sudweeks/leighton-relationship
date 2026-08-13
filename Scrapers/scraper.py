"""Download typed, monitor-resolved observations from the EPA AQS API.

The immutable download product is a long-form Parquet table.  It deliberately
retains POC, method, detection-limit, qualifier, and timestamp fields instead
of averaging monitors into a wide CSV.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPOSITORY_ROOT / ".env")

BASE_URL = "https://aqs.epa.gov/data/api"
STATE_CODE = "49"
TIMEOUT = 40
MAX_PARAMETERS_PER_REQUEST = 5

PARAMS = {
    "44201": "O3",
    "42601": "NO",
    "42602": "NO2",
    "63301": "SR",
    "62101": "Temp",
    "63302": "UV",
}

CODE_COLUMNS = {
    "state_code",
    "county_code",
    "site_number",
    "parameter_code",
    "sample_duration_code",
    "units_of_measure_code",
    "method_code",
}
INTEGER_COLUMNS = {"poc"}
NUMERIC_COLUMNS = {
    "sample_measurement",
    "detection_limit",
    "uncertainty",
    "latitude",
    "longitude",
}
INFORMATIONAL_QUALIFIER_PREFIXES = (
    "IT -",  # wildfire informational flag
    "XS -",  # ozone standard-reference traceability annotation
)


class AQSAPIError(RuntimeError):
    """Raised when AQS returns an HTTP- or payload-level error."""


def get_session() -> requests.Session:
    """Create an HTTPS session with bounded retry behavior."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        read=5,
        connect=5,
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def parameter_batches(
    parameter_codes: Iterable[str],
    batch_size: int = MAX_PARAMETERS_PER_REQUEST,
) -> list[tuple[str, ...]]:
    """Split parameter codes into AQS-compliant request batches."""
    if batch_size < 1 or batch_size > MAX_PARAMETERS_PER_REQUEST:
        raise ValueError(
            f"batch_size must be between 1 and {MAX_PARAMETERS_PER_REQUEST}"
        )
    codes = tuple(dict.fromkeys(str(code) for code in parameter_codes))
    return [codes[i : i + batch_size] for i in range(0, len(codes), batch_size)]


def _header_status(payload: Mapping[str, Any]) -> tuple[str, str]:
    header = payload.get("Header", [])
    if isinstance(header, list) and header:
        item = header[0] if isinstance(header[0], Mapping) else {}
    elif isinstance(header, Mapping):
        item = header
    else:
        item = {}
    return str(item.get("status", "")).strip(), str(item.get("error", "")).strip()


def request_aqs(
    session: requests.Session,
    endpoint: str,
    params: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Request AQS data and validate both HTTP and AQS Header status."""
    try:
        response = session.get(
            f"{BASE_URL}/{endpoint}",
            params=dict(params),
            timeout=TIMEOUT,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise AQSAPIError(
            f"AQS network request failed for {endpoint}: {type(exc).__name__}"
        ) from exc
    try:
        payload = response.json()
    except ValueError as exc:
        raise AQSAPIError(f"AQS returned invalid JSON for {endpoint}") from exc

    status, error = _header_status(payload)
    if status.lower().startswith("no data"):
        return []
    if status.lower() != "success":
        detail = error or status or "missing AQS Header status"
        raise AQSAPIError(f"AQS request failed for {endpoint}: {detail}")

    data = payload.get("Data", [])
    if not isinstance(data, list):
        raise AQSAPIError(f"AQS returned a non-list Data payload for {endpoint}")
    return data


def _base_params(email: str, key: str) -> dict[str, str]:
    return {"email": email, "key": key}


def get_sites(
    email: str,
    key: str,
    state_code: str,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch and deduplicate all sites in a state."""
    session = session or get_session()
    counties = request_aqs(
        session,
        "list/countiesByState",
        {**_base_params(email, key), "state": state_code},
    )
    all_sites: list[dict[str, Any]] = []
    for county in counties:
        county_code = str(county["code"]).zfill(3)
        sites = request_aqs(
            session,
            "list/sitesByCounty",
            {
                **_base_params(email, key),
                "state": state_code,
                "county": county_code,
            },
        )
        for site in sites:
            site = dict(site)
            site["county_code"] = county_code
            site["county_name"] = county.get("value_represented")
            all_sites.append(site)

    unique_sites = {
        f"{state_code}-{site['county_code']}-{site['code']}": site
        for site in all_sites
    }
    return list(unique_sites.values())


def check_site_monitors(
    email: str,
    key: str,
    state: str,
    county: str,
    site: str,
    *,
    bdate: str = "19800101",
    edate: str | None = None,
    session: requests.Session | None = None,
) -> tuple[bool, list[str]]:
    """Check for required monitors without violating the AQS param limit."""
    session = session or get_session()
    edate = edate or datetime.now().strftime("%Y%m%d")
    monitors: list[dict[str, Any]] = []
    for batch in parameter_batches(PARAMS):
        monitors.extend(
            request_aqs(
                session,
                "monitors/bySite",
                {
                    **_base_params(email, key),
                    "param": ",".join(batch),
                    "bdate": bdate,
                    "edate": edate,
                    "state": state,
                    "county": county,
                    "site": site,
                },
            )
        )
        time.sleep(1)

    found = sorted(
        {
            str(monitor["parameter_code"])
            for monitor in monitors
            if str(monitor.get("parameter_code")) in PARAMS
        }
    )
    return set(PARAMS).issubset(found), found


def fetch_sample_data(
    email: str,
    key: str,
    state: str,
    county: str,
    site: str,
    bdate: str,
    edate: str,
    *,
    parameter_codes: Sequence[str] = tuple(PARAMS),
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch finest-grain AQS sample data in valid parameter batches."""
    session = session or get_session()
    records: list[dict[str, Any]] = []
    for batch in parameter_batches(parameter_codes):
        records.extend(
            request_aqs(
                session,
                "sampleData/bySite",
                {
                    **_base_params(email, key),
                    "param": ",".join(batch),
                    "bdate": bdate,
                    "edate": edate,
                    "state": state,
                    "county": county,
                    "site": site,
                },
            )
        )
        time.sleep(1)
    return records


def _year_windows(start: date, end: date) -> Iterable[tuple[date, date]]:
    if end < start:
        raise ValueError("end date must not precede start date")
    for year in range(start.year, end.year + 1):
        yield max(start, date(year, 1, 1)), min(end, date(year, 12, 31))


def _jsonify_complex(value: Any) -> Any:
    if isinstance(value, (list, dict, tuple)):
        return json.dumps(value, sort_keys=True)
    return value


def normalize_records(
    records: Sequence[Mapping[str, Any]],
    *,
    downloaded_at: datetime | None = None,
) -> pd.DataFrame:
    """Normalize AQS records while retaining all source fields and types."""
    frame = pd.DataFrame.from_records(records)
    if frame.empty:
        return frame

    for column in frame.columns:
        if frame[column].dtype == "object":
            frame[column] = frame[column].map(_jsonify_complex)

    for column in CODE_COLUMNS.intersection(frame.columns):
        frame[column] = frame[column].astype("string")
    for column in INTEGER_COLUMNS.intersection(frame.columns):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    for column in NUMERIC_COLUMNS.intersection(frame.columns):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Float64")

    required_time_fields = {"date_local", "time_local", "date_gmt", "time_gmt"}
    missing = required_time_fields.difference(frame.columns)
    if missing:
        raise ValueError(f"AQS records are missing timestamp fields: {sorted(missing)}")

    frame["datetime_local_standard"] = pd.to_datetime(
        frame["date_local"].astype("string") + " " + frame["time_local"].astype("string"),
        errors="raise",
    )
    frame["datetime_utc"] = pd.to_datetime(
        frame["date_gmt"].astype("string") + " " + frame["time_gmt"].astype("string"),
        errors="raise",
        utc=True,
    )
    frame["parameter_abbreviation"] = (
        frame["parameter_code"].map(PARAMS).astype("string")
    )

    downloaded_at = downloaded_at or datetime.now(timezone.utc)
    if downloaded_at.tzinfo is None:
        downloaded_at = downloaded_at.replace(tzinfo=timezone.utc)
    frame["downloaded_at_utc"] = pd.Timestamp(downloaded_at).tz_convert("UTC")

    site_parts = [
        frame.get("state_code", pd.Series("", index=frame.index)).astype("string"),
        frame.get("county_code", pd.Series("", index=frame.index)).astype("string"),
        frame.get("site_number", pd.Series("", index=frame.index)).astype("string"),
    ]
    frame["site_key"] = site_parts[0].str.cat(site_parts[1:], sep="-")

    monitor_parts = [
        frame["site_key"],
        frame["parameter_code"],
        frame.get("poc", pd.Series(pd.NA, index=frame.index, dtype="Int64"))
        .astype("string")
        .fillna("NA"),
        frame.get("method_code", pd.Series("", index=frame.index))
        .astype("string")
        .fillna(""),
    ]
    frame["monitor_key"] = monitor_parts[0].str.cat(monitor_parts[1:], sep="-")

    preferred = [
        "datetime_utc",
        "datetime_local_standard",
        "site_key",
        "monitor_key",
        "state_code",
        "county_code",
        "site_number",
        "parameter_code",
        "parameter_abbreviation",
        "parameter",
        "poc",
        "method_code",
        "method",
        "sample_measurement",
        "units_of_measure",
        "detection_limit",
        "uncertainty",
        "qualifier",
        "date_gmt",
        "time_gmt",
        "date_local",
        "time_local",
        "downloaded_at_utc",
    ]
    ordered = [column for column in preferred if column in frame.columns]
    ordered.extend(column for column in frame.columns if column not in ordered)
    return frame.loc[:, ordered].sort_values(
        ["datetime_utc", "parameter_code", "poc", "method_code"],
        kind="stable",
        ignore_index=True,
    )


def build_monitor_inventory(samples: pd.DataFrame) -> pd.DataFrame:
    """Summarize monitor coverage and detection performance without mixing POCs."""
    if samples.empty:
        return pd.DataFrame()

    work = samples.copy()
    work["_measurement_present"] = work["sample_measurement"].notna()
    work["_positive_mdl"] = work["detection_limit"].gt(0)
    work["_below_mdl"] = (
        work["_measurement_present"]
        & work["_positive_mdl"]
        & work["sample_measurement"].lt(work["detection_limit"])
    )
    work["_at_or_above_mdl"] = (
        work["_measurement_present"]
        & work["_positive_mdl"]
        & work["sample_measurement"].ge(work["detection_limit"])
    )

    group_columns = [
        column
        for column in [
            "site_key",
            "parameter_code",
            "parameter_abbreviation",
            "parameter",
            "poc",
            "method_code",
            "method",
            "units_of_measure",
            "detection_limit",
        ]
        if column in work.columns
    ]
    inventory = (
        work.groupby(group_columns, dropna=False, observed=True)
        .agg(
            first_observation_utc=("datetime_utc", "min"),
            last_observation_utc=("datetime_utc", "max"),
            observation_count=("sample_measurement", "size"),
            measured_count=("_measurement_present", "sum"),
            below_mdl_count=("_below_mdl", "sum"),
            at_or_above_mdl_count=("_at_or_above_mdl", "sum"),
            minimum_measurement=("sample_measurement", "min"),
            median_measurement=("sample_measurement", "median"),
            maximum_measurement=("sample_measurement", "max"),
        )
        .reset_index()
    )
    positive_mdl_count = (
        inventory["below_mdl_count"] + inventory["at_or_above_mdl_count"]
    )
    inventory["fraction_at_or_above_mdl"] = (
        inventory["at_or_above_mdl_count"] / positive_mdl_count.where(positive_mdl_count > 0)
    ).astype("Float64")
    return inventory.sort_values(
        ["parameter_code", "poc", "method_code", "detection_limit"],
        kind="stable",
        ignore_index=True,
    )


def select_poc_records(
    samples: pd.DataFrame,
    selections: Mapping[str, int],
) -> pd.DataFrame:
    """Select explicit POCs by parameter code or abbreviation.

    Parameters absent from ``selections`` are omitted. This function never
    averages observations from different POCs.
    """
    selected: list[pd.DataFrame] = []
    for parameter, poc in selections.items():
        by_code = samples["parameter_code"].eq(str(parameter))
        by_name = samples["parameter_abbreviation"].eq(str(parameter))
        subset = samples[(by_code | by_name) & samples["poc"].eq(int(poc))]
        if subset.empty:
            raise ValueError(f"No records found for parameter {parameter!r}, POC {poc}")
        selected.append(subset)
    if not selected:
        return samples.iloc[0:0].copy()
    return pd.concat(selected, ignore_index=True).sort_values(
        ["datetime_utc", "parameter_code"], kind="stable", ignore_index=True
    )


def build_analysis_table(
    samples: pd.DataFrame,
    selections: Mapping[str, int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build a typed wide table from explicit, non-mixed POC selections.

    The notebook-facing parameter columns are null unless the source value is
    present, at or above a positive MDL, and free of non-informational
    qualifiers. Source measurements and all cleaning flags remain beside them.
    """
    selected = select_poc_records(samples, selections).copy()
    selected["_below_mdl"] = (
        selected["detection_limit"].gt(0)
        & selected["sample_measurement"].notna()
        & selected["sample_measurement"].lt(selected["detection_limit"])
    )
    qualifier = selected["qualifier"].astype("string")
    selected["_quality_flagged"] = qualifier.notna() & ~qualifier.str.startswith(
        INFORMATIONAL_QUALIFIER_PREFIXES,
        na=False,
    )
    selected["_usable"] = (
        selected["sample_measurement"].notna()
        & ~selected["_below_mdl"]
        & ~selected["_quality_flagged"]
    )
    selected["_clean_measurement"] = selected["sample_measurement"].where(
        selected["_usable"]
    )

    duplicate_key = ["datetime_utc", "parameter_abbreviation"]
    duplicates = selected.duplicated(duplicate_key, keep=False)
    if duplicates.any():
        examples = selected.loc[
            duplicates,
            duplicate_key + ["poc", "method_code"],
        ].head(5)
        raise ValueError(
            "Selected POCs still contain multiple observations per UTC timestamp: "
            f"{examples.to_dict(orient='records')}"
        )

    timestamps = (
        selected[["datetime_utc", "datetime_local_standard"]]
        .drop_duplicates()
        .sort_values("datetime_utc", kind="stable")
        .set_index("datetime_utc")
    )
    wide = timestamps.copy()
    for parameter in selections:
        subset = selected[
            selected["parameter_abbreviation"].eq(parameter)
            | selected["parameter_code"].eq(parameter)
        ].set_index("datetime_utc")
        abbreviation = str(subset["parameter_abbreviation"].iloc[0])
        wide[abbreviation] = subset["_clean_measurement"]
        wide[f"{abbreviation}_raw"] = subset["sample_measurement"]
        wide[f"{abbreviation}_below_mdl"] = subset["_below_mdl"].astype("boolean")
        wide[f"{abbreviation}_quality_flagged"] = subset[
            "_quality_flagged"
        ].astype("boolean")
        wide[f"{abbreviation}_qualifier"] = subset["qualifier"].astype("string")
        wide[f"{abbreviation}_detection_limit"] = subset["detection_limit"]
        wide[f"{abbreviation}_poc"] = subset["poc"]
        wide[f"{abbreviation}_method_code"] = subset["method_code"]

    abbreviations = [
        str(value)
        for value in selected["parameter_abbreviation"].dropna().unique()
    ]
    wide["core_chemistry_complete"] = wide[
        [name for name in ["NO", "NO2", "O3", "Temp"] if name in abbreviations]
    ].notna().all(axis=1)
    wide = wide.reset_index()

    accounting = (
        selected.groupby(
            [
                "parameter_abbreviation",
                "parameter_code",
                "poc",
                "method_code",
                "detection_limit",
            ],
            dropna=False,
            observed=True,
        )
        .agg(
            source_rows=("sample_measurement", "size"),
            measured_rows=("sample_measurement", "count"),
            below_mdl_rows=("_below_mdl", "sum"),
            quality_flagged_rows=("_quality_flagged", "sum"),
            usable_rows=("_usable", "sum"),
        )
        .reset_index()
        .sort_values(["parameter_code", "poc"], kind="stable", ignore_index=True)
    )
    accounting["usable_fraction_of_measured"] = (
        accounting["usable_rows"] / accounting["measured_rows"]
    ).astype("Float64")
    return wide, accounting


def save_analysis_table(
    samples: pd.DataFrame,
    *,
    output_dir: Path,
    selections: Mapping[str, int],
) -> tuple[Path, Path]:
    """Create analysis-ready and row-accounting Parquet files."""
    analysis, accounting = build_analysis_table(samples, selections)
    analysis_path = output_dir / "aqs_analysis_ready.parquet"
    accounting_path = output_dir / "analysis_row_accounting.parquet"
    _atomic_parquet(analysis, analysis_path)
    _atomic_parquet(accounting, accounting_path)

    manifest_path = output_dir / "analysis_manifest.json"
    _atomic_json(
        {
            "schema_version": 1,
            "source_file": "aqs_samples.parquet",
            "analysis_file": analysis_path.name,
            "row_accounting_file": accounting_path.name,
            "poc_selections": {key: int(value) for key, value in selections.items()},
            "cleaning_rules": {
                "missing_measurement": "set notebook-facing value to null",
                "positive_mdl": "set values below the row-level MDL to null",
                "qualifiers": (
                    "set non-informational qualified values to null; retain "
                    f"prefixes {list(INFORMATIONAL_QUALIFIER_PREFIXES)}"
                ),
                "source_retention": (
                    "retain raw measurement, MDL, qualifier, POC, method, and flags"
                ),
            },
            "analysis_rows": len(analysis),
            "complete_core_chemistry_rows": int(
                analysis["core_chemistry_complete"].sum()
            ),
        },
        manifest_path,
    )
    return analysis_path, accounting_path


def sanitize_filename(name: str) -> str:
    return re.sub(r"[^\w\-_]", "_", name)


def _atomic_parquet(frame: pd.DataFrame, destination: Path) -> None:
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    try:
        frame.to_parquet(temporary, engine="pyarrow", index=False)
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_json(payload: Any, destination: Path) -> None:
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    try:
        temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()


def save_download(
    samples: pd.DataFrame,
    *,
    output_root: Path,
    site_name: str,
    state_code: str,
    county_code: str,
    site_id: str,
    start: date,
    end: date,
) -> Path:
    """Write an append-only Parquet download and its manifest."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    download_id = f"{start:%Y%m%d}_{end:%Y%m%d}_{timestamp}"
    output_dir = output_root / sanitize_filename(site_name) / download_id
    output_dir.mkdir(parents=True, exist_ok=False)

    samples_path = output_dir / "aqs_samples.parquet"
    inventory_path = output_dir / "monitor_inventory.parquet"
    manifest_path = output_dir / "manifest.json"
    inventory = build_monitor_inventory(samples)
    returned_codes = sorted(
        samples["parameter_code"].dropna().astype(str).unique().tolist()
    )
    missing_codes = sorted(set(PARAMS).difference(returned_codes))

    _atomic_parquet(samples, samples_path)
    _atomic_parquet(inventory, inventory_path)
    _atomic_json(
        {
            "schema_version": 1,
            "download_id": download_id,
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
            "site_name": site_name,
            "state_code": state_code,
            "county_code": county_code,
            "site_id": site_id,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "parameter_codes": list(PARAMS),
            "returned_parameter_codes": returned_codes,
            "missing_parameter_codes": missing_codes,
            "parameter_batches": [
                list(batch) for batch in parameter_batches(PARAMS)
            ],
            "sample_rows": len(samples),
            "missing_measurement_rows": int(
                samples["sample_measurement"].isna().sum()
            ),
            "first_observation_utc": samples["datetime_utc"].min().isoformat(),
            "last_observation_utc": samples["datetime_utc"].max().isoformat(),
            "monitor_inventory_rows": len(inventory),
            "samples_file": samples_path.name,
            "monitor_inventory_file": inventory_path.name,
        },
        manifest_path,
    )
    return output_dir


def identify_valid_sites(
    email: str,
    key: str,
    state_code: str,
    *,
    output_file: Path = Path("data/valid_sites.json"),
) -> list[dict[str, Any]]:
    """Find sites that have historically monitored every requested parameter."""
    session = get_session()
    sites = get_sites(email, key, state_code, session=session)
    valid_sites: list[dict[str, Any]] = []
    for site_info in sites:
        site_id = str(site_info["code"])
        county_code = str(site_info["county_code"])
        has_all, _ = check_site_monitors(
            email,
            key,
            state_code,
            county_code,
            site_id,
            session=session,
        )
        if has_all:
            site_name = site_info.get("value_represented") or site_info.get(
                "local_site_name", f"Site_{site_id}"
            )
            site_info["clean_name"] = sanitize_filename(f"{site_name}_{site_id}")
            valid_sites.append(site_info)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    _atomic_json(valid_sites, output_file)
    return valid_sites


def process_single_site(
    email: str,
    key: str,
    state_code: str,
    county_code: str,
    site_id: str,
    *,
    site_name: str,
    start: date,
    end: date,
    output_root: Path = Path("data/downloads"),
) -> Path:
    """Download a site in yearly windows and save typed long-form Parquet."""
    session = get_session()
    records: list[dict[str, Any]] = []
    for window_start, window_end in _year_windows(start, end):
        print(f"Fetching {window_start} through {window_end}...")
        records.extend(
            fetch_sample_data(
                email,
                key,
                state_code,
                county_code,
                site_id,
                window_start.strftime("%Y%m%d"),
                window_end.strftime("%Y%m%d"),
                session=session,
            )
        )

    if not records:
        raise AQSAPIError("AQS returned no sample records for the requested selection")
    samples = normalize_records(records)
    output_dir = save_download(
        samples,
        output_root=output_root,
        site_name=site_name,
        state_code=state_code,
        county_code=county_code,
        site_id=site_id,
        start=start,
        end=end,
    )
    print(f"Saved {len(samples):,} monitor-resolved rows to {output_dir}")
    return output_dir


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def _parse_poc_selection(value: str) -> tuple[str, int]:
    try:
        parameter, poc = value.split("=", 1)
        if parameter not in PARAMS.values() and parameter not in PARAMS:
            raise ValueError
        return parameter, int(poc)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "expected PARAMETER=POC, for example NO=2"
        ) from exc


def _credentials() -> tuple[str, str]:
    email = os.getenv("API_EMAIL")
    key = os.getenv("API_KEY")
    if not email or not key:
        raise RuntimeError(
            "Set API_EMAIL and API_KEY in the environment or an untracked .env file"
        )
    return email, key


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download monitor-resolved Utah AQS data to Parquet"
    )
    parser.add_argument(
        "--action",
        choices=["identify", "process", "prepare"],
        required=True,
    )
    parser.add_argument("--site", help="AQS site ID")
    parser.add_argument("--county", help="three-digit AQS county code")
    parser.add_argument("--name", help="site name used in the output directory")
    parser.add_argument("--start-date", type=_parse_date, help="inclusive YYYY-MM-DD")
    parser.add_argument("--end-date", type=_parse_date, help="inclusive YYYY-MM-DD")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/downloads"),
        help="append-only download root",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        help="download directory containing aqs_samples.parquet",
    )
    parser.add_argument(
        "--poc",
        action="append",
        type=_parse_poc_selection,
        default=[],
        help="explicit POC selection such as --poc NO=2; may be repeated",
    )
    args = parser.parse_args()

    try:
        if args.action == "prepare":
            if args.input_dir is None or not args.poc:
                parser.error("prepare requires --input-dir and at least one --poc")
            samples = pd.read_parquet(args.input_dir / "aqs_samples.parquet")
            selections = dict(args.poc)
            analysis_path, accounting_path = save_analysis_table(
                samples,
                output_dir=args.input_dir,
                selections=selections,
            )
            print(f"Saved analysis table: {analysis_path}")
            print(f"Saved row accounting: {accounting_path}")
            return

        email, key = _credentials()
        if args.action == "identify":
            sites = identify_valid_sites(email, key, STATE_CODE)
            print(f"Found {len(sites)} sites with all requested parameters")
            return

        missing = [
            flag
            for flag, value in [
                ("--site", args.site),
                ("--county", args.county),
                ("--start-date", args.start_date),
                ("--end-date", args.end_date),
            ]
            if value is None
        ]
        if missing:
            parser.error(f"process requires {', '.join(missing)}")
        site_name = args.name or f"Site_{args.site}"
        process_single_site(
            email,
            key,
            STATE_CODE,
            str(args.county).zfill(3),
            str(args.site).zfill(4),
            site_name=site_name,
            start=args.start_date,
            end=args.end_date,
            output_root=args.output_root,
        )
    except (AQSAPIError, RuntimeError, requests.RequestException, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
