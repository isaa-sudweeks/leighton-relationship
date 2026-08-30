# Repository Guidelines

## Project Structure & Module Organization

- `leighton_relationship_analysis.py` is the reproducible Leighton-ratio analysis entry point.
- `sr_ci_filter.py` implements solar-radiation and clearing-index filtering.
- `Scrapers/` contains the EPA AQS and Synoptic data collectors.
- `tests/` contains unit tests for the analysis, filters, and scraper behavior.
- `data/` holds downloaded, archived, and reference datasets. Treat timestamped folders under `data/downloads/` as immutable source snapshots.
- `output/` contains generated datasets, summaries, figures, and audit artifacts. Prefer the Python pipeline over notebooks for reproducible behavior.

## Setup, Test, and Development Commands

Create and activate a virtual environment, then install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Run all tests with `python -m unittest discover -s tests -v`; run one module with `python -m unittest tests.test_sr_ci_filter -v`. Execute the default analysis with `python leighton_relationship_analysis.py`, adding `--apply-sr-ci --output-dir output/leighton_analysis_sr_ci` for filtered results. See `README.MD` for scraper commands.

## Coding Style & Naming Conventions

Use four-space indentation and PEP 8 naming: `snake_case` for functions and variables, `PascalCase` for classes, and uppercase names for constants. Add type hints to public functions and use `pathlib.Path`. Preserve pandas column names used by datasets and tests. Keep scientific constants, units, thresholds, and provenance explicit. No formatter or linter is configured, so match nearby code and group imports conventionally.

## Testing Guidelines

Tests use the standard-library `unittest` framework and files named `test_*.py`. Add focused regression tests for calculation changes, timestamp alignment, filtering boundaries, POC selection, and credential-safe failures. Use temporary directories and mocks for filesystem or network behavior; routine tests must not call live APIs. The project has no formal coverage threshold, but new branches and scientific assumptions should be exercised.

## Data, Secrets, and Reproducibility

Store `API_EMAIL`, `API_KEY`, and other credentials in the ignored `.env`; never commit them or include them in exceptions. Do not overwrite raw downloads. Write derived results to a clearly named `output/` subdirectory and record parameter or calibration changes in summaries and documentation.

## Commit & Pull Request Guidelines

History generally uses short, imperative subjects, often with `feat:` (for example, `feat: implement solar radiation filtering`). Keep commits narrow and describe the scientific or data-contract effect. Pull requests should summarize the change, list validation commands, identify affected datasets/outputs, and call out changed assumptions or provenance. Include representative plots or before/after metrics when figures or numerical results change; link the relevant issue when one exists.
