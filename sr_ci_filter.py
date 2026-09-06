"""Solar-radiation and clearing-index filters used by Improved Isopleths."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import pandas as pd


IEM_AFOS_ENDPOINT = "https://mesonet.agron.iastate.edu/cgi-bin/afos/retrieve.py"
SMF_PIL = "SMFSLC"
REQUEST_HEADERS = {"User-Agent": "Improved-Isopleths-clearing-index-notebook"}

AIRSHED_LOOKUP: Dict[int, str] = {
    1: "Northwest Desert",
    2: "Southwest Desert",
    3: "Virgin River",
    4: "Cache",
    5: "Northern Wasatch Front",
    6: "Wasatch Back",
    7: "Northern Slopes",
    8: "Southern Wasatch Front",
    9: "Uinta Basin",
    10: "San Pete River",
    11: "Book Cliffs",
    12: "Upper Colorado River",
    13: "Sevier River",
    14: "Lower Colorado River",
    15: "San Juan River",
    16: "Areas Above 6500 feet",
}

SAME_DAY_LABELS = {
    "TODAY",
    "TONIGHT",
    "THIS MORNING",
    "THIS AFTERNOON",
    "THIS EVENING",
    "REST OF TONIGHT",
}
NEXT_DAY_LABELS = {"TOMORROW", "TOMORROW NIGHT"}
WEEKDAY_TO_INT = {
    "MONDAY": 0,
    "TUESDAY": 1,
    "WEDNESDAY": 2,
    "THURSDAY": 3,
    "FRIDAY": 4,
    "SATURDAY": 5,
    "SUNDAY": 6,
}

ISSUE_LINE_PATTERN = re.compile(
    r"\d{3,4}\s+[AP]M\s+[A-Za-z]{3}\s+[A-Za-z]{3}\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{4}",
    re.IGNORECASE,
)
AIRSHED_BLOCK_PATTERN = re.compile(
    r"\.\.\.AIR SHED\s+(\d+)\.\.\.[\s\S]*?(?=(?:\.\.\.AIR SHED|\$\$))",
    re.MULTILINE,
)


def resolve_air_shed_id(target: int | str) -> int:
    if isinstance(target, int):
        if target in AIRSHED_LOOKUP:
            return target
        raise ValueError(f"Unknown air shed id: {target}")
    cleaned = str(target).strip().upper()
    for air_id, name in AIRSHED_LOOKUP.items():
        if name.upper() == cleaned:
            return air_id
    raise ValueError(
        f"Could not resolve air shed '{target}'. Available options: "
        f"{', '.join(AIRSHED_LOOKUP.values())}"
    )


def fetch_smoke_management_text(start: datetime, end: datetime) -> str:
    params = {
        "pil": SMF_PIL,
        "fmt": "text",
        "sdate": start.strftime("%Y-%m-%dT%H:%MZ"),
        "edate": end.strftime("%Y-%m-%dT%H:%MZ"),
        "limit": 9999,
        "order": "asc",
    }
    request = Request(
        f"{IEM_AFOS_ENDPOINT}?{urlencode(params)}",
        headers=REQUEST_HEADERS,
    )
    with urlopen(request, timeout=60) as response:
        text = response.read().decode("utf-8")
    return "" if "NO PRODUCTS FOUND" in text.upper() else text


def split_products(raw_text: str) -> Iterable[str]:
    for chunk in raw_text.split("\x03"):
        cleaned = chunk.lstrip("\x01\n\r").strip()
        if cleaned:
            yield cleaned


def parse_issue_time(product_text: str, timezone: str) -> datetime:
    for line in product_text.splitlines():
        match = ISSUE_LINE_PATTERN.search(line)
        if match:
            parts = match.group(0).split()
            issue_naive = datetime.strptime(
                f"{' '.join(parts[3:])} {parts[0]} {parts[1]}",
                "%a %b %d %Y %I%M %p",
            )
            return issue_naive.replace(tzinfo=ZoneInfo(timezone))
    raise ValueError("Unable to parse issue time for smoke management product.")


def parse_clearing_index_value(text_value: str) -> Optional[int]:
    cleaned = text_value.replace("+", "").strip()
    return int(cleaned) if cleaned.isdigit() else None


def normalize_period_label(label: str) -> str:
    return re.sub(r"[^A-Z ]", "", label.upper()).strip()


def resolve_valid_date(issue_dt: datetime, label: str) -> Optional[datetime.date]:
    normalized = normalize_period_label(label)
    if not normalized:
        return None
    if normalized in SAME_DAY_LABELS:
        offset = 0
    elif normalized in NEXT_DAY_LABELS:
        offset = 1
    else:
        target_weekday: Optional[int] = None
        for word in normalized.split():
            if word in WEEKDAY_TO_INT:
                target_weekday = WEEKDAY_TO_INT[word]
                break
        if target_weekday is None:
            return None
        days_ahead = (target_weekday - issue_dt.weekday()) % 7
        offset = 7 if days_ahead == 0 else days_ahead
    return (issue_dt + timedelta(days=offset)).date()


def parse_product(product_text: str, timezone: str) -> List[Dict[str, object]]:
    issue_time = parse_issue_time(product_text, timezone)
    records: List[Dict[str, object]] = []
    for match in AIRSHED_BLOCK_PATTERN.finditer(product_text):
        air_shed_id = int(match.group(1))
        period_label: Optional[str] = None
        for line in match.group(0).splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.endswith("..."):
                period_label = stripped.rstrip(".").strip()
                continue
            if stripped.startswith("Clearing Index") and period_label:
                ci_token = stripped.split()[-1]
                ci_value = parse_clearing_index_value(ci_token)
                valid_date = resolve_valid_date(issue_time, period_label)
                if ci_value is not None and valid_date is not None:
                    records.append(
                        {
                            "air_shed": air_shed_id,
                            "issue_time_local": issue_time,
                            "valid_date": valid_date,
                            "period_label": period_label,
                            "clearing_index": ci_value,
                            "clearing_index_token": ci_token,
                            "clearing_index_is_lower_bound": ci_token.endswith("+"),
                        }
                    )
    return records


def build_clearing_index_history(
    start: datetime, end: datetime, timezone: str
) -> pd.DataFrame:
    columns = [
        "air_shed",
        "issue_time_local",
        "valid_date",
        "period_label",
        "clearing_index",
        "clearing_index_token",
        "clearing_index_is_lower_bound",
    ]
    raw_text = fetch_smoke_management_text(start, end)
    if not raw_text:
        return pd.DataFrame(columns=columns)

    entries: List[Dict[str, object]] = []
    for product in split_products(raw_text):
        entries.extend(parse_product(product, timezone))
    if not entries:
        return pd.DataFrame(columns=columns)

    history = pd.DataFrame(entries).sort_values(
        ["air_shed", "valid_date", "issue_time_local"]
    )
    return (
        history.groupby(["air_shed", "valid_date"], as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )


def apply_sr_ci_filters(
    data: pd.DataFrame,
    *,
    enabled: bool,
    sr_column: str = "SR_Synoptic",
    sr_threshold: float = 710,
    clearing_index_threshold: int = 1000,
    target_airshed: int | str = "Northern Wasatch Front",
    local_timezone: str = "America/Denver",
    request_buffer_days: int = 2,
    clearing_index_history: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Apply the Improved Isopleths SR and clearing-index cutoffs.

    When ``enabled`` is false, the input is returned unchanged. When enabled,
    rows must have SR >= 710 W/m^2 and clearing index <= 1000 by default.
    """
    if not enabled:
        return data
    if sr_column not in data.columns:
        raise KeyError(f"Solar-radiation column '{sr_column}' was not found.")
    if not isinstance(data.index, pd.DatetimeIndex):
        raise TypeError("The dataframe must use a DatetimeIndex.")

    filtered = data[data[sr_column].ge(sr_threshold)].copy()
    sr_qualified_rows = len(filtered)
    if filtered.empty:
        filtered.attrs["sr_ci_row_accounting"] = {
            "input_rows": len(data),
            "sr_qualified_rows": 0,
            "missing_ci_rows": 0,
            "qualified_lower_bound_rows": 0,
            "retained_rows": 0,
        }
        print(f"SR/CI filter retained 0 of {len(data)} rows after SR >= {sr_threshold}.")
        return filtered

    local_zone = ZoneInfo(local_timezone)
    local_times = pd.Series(filtered.index, index=filtered.index)
    if local_times.dt.tz is None:
        local_times = local_times.dt.tz_localize(
            local_zone, nonexistent="shift_forward", ambiguous="NaT"
        )
    else:
        local_times = local_times.dt.tz_convert(local_zone)
    filtered = filtered.loc[local_times.notna()].copy()
    local_times = local_times.loc[local_times.notna()]
    filtered["ci_valid_date"] = local_times.dt.date.to_numpy()

    air_shed_id = resolve_air_shed_id(target_airshed)
    start_local = datetime.combine(
        filtered["ci_valid_date"].min(), datetime.min.time()
    ) - timedelta(days=request_buffer_days)
    end_local = datetime.combine(
        filtered["ci_valid_date"].max(), datetime.max.time()
    ) + timedelta(days=request_buffer_days)
    start_utc = start_local.replace(tzinfo=local_zone).astimezone(ZoneInfo("UTC"))
    end_utc = end_local.replace(tzinfo=local_zone).astimezone(ZoneInfo("UTC"))

    history = (
        clearing_index_history.copy()
        if clearing_index_history is not None
        else build_clearing_index_history(start_utc, end_utc, local_timezone)
    )
    history = history[history["air_shed"] == air_shed_id].copy()
    if history.empty:
        raise RuntimeError(
            "No clearing index records were returned for the requested period "
            "and air shed."
        )
    history["ci_valid_date"] = pd.to_datetime(history["valid_date"]).dt.date
    history = history.drop_duplicates(subset=["ci_valid_date"])
    if "clearing_index_token" not in history:
        history["clearing_index_token"] = history["clearing_index"].astype("string")
    if "clearing_index_is_lower_bound" not in history:
        history["clearing_index_is_lower_bound"] = False

    merged = (
        filtered.reset_index(names=filtered.index.name or "datetime")
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
        )
        .set_index(filtered.index.name or "datetime")
    )
    missing_ci = int(merged["clearing_index"].isna().sum())
    if missing_ci:
        print(
            f"Warning: {missing_ci} rows are missing clearing index data "
            "(outside archive window or air shed mismatch)."
        )

    qualified_ci = (
        merged["clearing_index_is_lower_bound"]
        .astype("boolean")
        .fillna(False)
        .astype(bool)
    )
    result = merged[
        merged["clearing_index"].le(clearing_index_threshold) & ~qualified_ci
    ].copy()
    excluded_qualified = int(qualified_ci.sum())
    if excluded_qualified:
        print(
            f"Excluded {excluded_qualified} rows with qualified clearing-index "
            "lower bounds such as '1000+'."
        )
    print(
        f"SR/CI filter retained {len(result)} of {len(data)} rows "
        f"(SR >= {sr_threshold} W/m^2; CI <= {clearing_index_threshold}; "
        f"air shed {air_shed_id})."
    )
    result.attrs["sr_ci_row_accounting"] = {
        "input_rows": len(data),
        "sr_qualified_rows": sr_qualified_rows,
        "missing_ci_rows": missing_ci,
        "qualified_lower_bound_rows": excluded_qualified,
        "retained_rows": len(result),
    }
    return result
