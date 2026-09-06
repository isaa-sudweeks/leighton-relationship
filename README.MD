# Leighton Relationship Research

This project investigates the Leighton relationship between various atmospheric parameters, specifically focusing on Solar Radiation (SR), UV data, and chemical concentrations (O3, NO, NO2).

## Data Sources & Methodology

### Current Status: UV Data
Initially, we are using **UV data** requested from Burt from the state. This dataset serves as our primary ground truth for the current phase of the relationship calculations.

### Future Work: SR Data & Scaling Factor
We are currently exploring methods to leverage **Solar Radiation (SR)** data as a proxy or supplementary data source. The plan is to develop and apply a **Scaling Factor** that would allow us to relate SR data to UV measurements effectively, enabling broader analysis across sites where UV data might be unavailable.

## Directory Structure

An overview of the repository folders and their contents:

- **`Scrapers/`**: Contains Python scripts for data collection.
  - `scraper.py`: Primary data scraping script for EPA AQS data (O3, NO, NO2, SR, UV, Temp).
  - `SynopticScraper.py`: Specialized scraper for fetching and fusing high-resolution Solar Radiation data from the Synoptic API.
- **`data/`**: Storage for all raw and processed datasets.
  - `Hawthorne/`, `Lindon/`, `Smithfield/`, `Vernal_4/`: Site-specific data directories.
  - `UV Data/`: Repository for UV data obtained from the state (requested via Burt).
  - `valid_sites.json`: Metadata and identification for sites meeting analysis criteria.
- **`Leighton_Relationship_Calculator.ipynb`**: The main research notebook used for processing data and calculating Leighton relationship parameters.
- **`.env`**: Configuration file for API keys and environment variables (not tracked in Git).

# AQS data download

The AQS scraper stores each download as an immutable, typed, long-form Parquet
snapshot. It preserves POC, method, method detection limit, qualifiers, and
both UTC and local-standard timestamps. It does not average different POCs.

Install the runtime dependencies:

```bash
python -m pip install -r requirements.txt
```

Put `API_EMAIL` and `API_KEY` in an untracked `.env` file, then download a
specific site and explicit date range. For example, Hawthorne is Utah site
`49-035-3006`:

```bash
python Scrapers/scraper.py \
  --action process \
  --county 035 \
  --site 3006 \
  --name Hawthorne \
  --start-date 2024-01-01 \
  --end-date 2026-07-28
```

Each run creates:

- `aqs_samples.parquet`: source-level observations with monitor provenance.
- `monitor_inventory.parquet`: coverage and above/below-MDL counts by POC,
  method, and detection limit.
- `manifest.json`: site, date range, request batching, and row counts.

Use `select_poc_records` only after reviewing the inventory. Parameters not
explicitly selected are omitted, and observations from different POCs are
never averaged.

`build_analysis_table` and `save_analysis_table` create a wide, typed table
after explicit POC selection. Notebook-facing values are null when missing,
below a positive row-level MDL, or marked with a non-informational qualifier.
The output retains the original value, MDL, qualifier, POC, method code, and
cleaning flags for auditing.

Reproduce the current Hawthorne selection with:

```bash
python Scrapers/scraper.py \
  --action prepare \
  --input-dir data/downloads/Hawthorne/20240101_20260728_20260729T004631Z \
  --poc NO=2 \
  --poc NO2=3 \
  --poc O3=1 \
  --poc SR=1 \
  --poc Temp=1
```

## Leighton-ratio analysis

The archived notebook has been replaced by a reproducible Python entry point:

```bash
python leighton_relationship_analysis.py
```

It writes the processed dataset for the configured period, a compact hourly
diagnostics table, a JSON summary, LR and log10(LR) time series, relationship
plots, non-overlapping clock-window distributions, and the temperature
correction plot under `output/leighton_analysis/`.

The primary calculation uses Hawthorne local standard time from 10:00 through
16:00 (inclusive) and requires strict NO > 0.20 ppb, following the August 14 and 18,
2026 implementation instructions. State UV values are treated as calibrated
W/m^2 irradiance. J(NO2) is calculated with the provisional NCAR TUV transfer
function

```text
C(SZA) = 1.538e-4 + 1.951e-6 * SZA
J(NO2) = C(SZA) * UV
```

where SZA is in degrees and J(NO2) is in s^-1. The output Parquet preserves
SZA, the applied coefficient, an explicit provisional-QC/extrapolation flag,
J(NO2), LR, and log10(LR); `summary.json` records the coefficient parameters
and provenance. Until the original TUV calibration grid is confirmed, the
default QC interval is the 19.74–50.47 degree SZA span covered by the reviewed
May 2025 development observations. It can be changed with `--tuv-sza-min` and
`--tuv-sza-max`; values outside it are retained and explicitly flagged as
provisional extrapolations. This replaces the complete legacy
`7.784e-5 * UV / 2.84 * 10` expression rather than adding another scale factor.
Each run also writes `no_threshold_sensitivity.csv` for strict cutoffs at 0,
0.05, 0.10, 0.20, 0.50, and 1.0 ppb.

The NO + O3 kinetic term uses the JPL Evaluation 19-5 C19 non-Arrhenius
recommendation over its stated 204–440 K range:

```text
k(T) = 3.32e-13 * (T / 298)^2.25 * exp(-850 / T)
```

Analyze all jointly available observations within a calendar year and write an
annual view plus monthly hourly plots with:

```bash
python leighton_relationship_analysis.py \
  --year 2025 \
  --full-year \
  --output-dir output/leighton_analysis_2025_unfiltered
```

The annual view is deliberately labeled "available observations," not "full
year": required-variable and QC filters can leave some months empty. The JSON
summary records the AQS/UV source limits, analyzed timestamp span, retained and
missing months, and SR/CI archive gaps.

`hourly_diagnostics_<year>_<month>.parquet` contains one row per retained
hour with local and UTC timestamps, NO, NO2, O3, derived NOx (`NO + NO2` in
ppb), UV (W/m2), solar radiation (W/m2), J(NO2) (s^-1), SZA (degrees),
temperature (degrees F), clearing index, LR, and log10(LR). NO and NO2 are in
ppb and O3 is in ppm, matching the source AQS table. Clearing index is null
when the optional SR/CI archive join was not requested; a null value must not
be interpreted as a measured index. The same units and derivation are recorded
in `summary.json` alongside the diagnostics filename and row count.

Each successful run removes period-specific artifacts left by an earlier run
in the same output directory before writing the new result. Unrelated files
are preserved.

Apply the collocated AQS solar-radiation and Northern Wasatch Front
clearing-index filters with:

```bash
python leighton_relationship_analysis.py \
  --year 2025 \
  --full-year \
  --apply-sr-ci \
  --ci-history output/leighton_audit_followup_2026-07-28/full_available_ci_history_air_shed_5.parquet \
  --output-dir output/leighton_analysis_2025_sr_ci
```

## Leighton-ratio uncertainty

`lr_uncertainty.py` records Callum's August 31 provisional relative budget:
UV measurement 5.5%, NO2 quantum yield 4%, NO2 absorption cross section 10%,
NO measurement 2%, NO2 measurement 2%, NO + O3 kinetics 8%, and O3 measurement
2%. These terms combine to 14.908% in quadrature; the requested rounded 14.9%
is used in plots and row-level outputs. Absolute uncertainty is calculated for
each observation as `abs(LR) * 0.149`, so every row retains its own LR-scaled
error bar. The component budget and unrounded RSS remain in `summary.json` for
auditability.
