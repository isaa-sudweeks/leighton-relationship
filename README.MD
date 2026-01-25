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
