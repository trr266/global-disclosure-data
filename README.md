# global-disclosure-data  
**Private firm disclosure around the world - data and descriptive analysis**

This repository contains the replication package for the paper  
*Private firm disclosure around the world - data and descriptive analysis*  
by **Christian Bernard, Ulf Brüggemann, and Jonas Materna**.  

The project provides evidence on the availability of financial statements across the world and documents stark differences in the share of disclosing firms (the *disclosure rate*) among private firms.

---

## Requirements

This project relies on **Orbis data** (versions: December 2024 and August 2019), accessible via the API provided by the TRR 266 *Accounting for Transparency* research center.

- **TRR 266 members**:  
  Create a file named `.global-disclosure-data.env` with your API credentials (a template is available as `_global-disclosure-data.env`).

- **Non-members**:  
  Access to Orbis data is possible via Moody's SFTP delivery system.  

A detailed list of required Orbis files can be found in:  
`code/01_pull_bvd_api_data.py`

---

## Setup

### Software
- R (latest version recommended)

### Required R Packages
The analysis relies on the following R packages:

`arrow`, `countrycode`, `data.table`, `DBI`, `dotenv`, `dplyr`, `duckdb`, `extrafont`,  
`fixest`, `ggplot2`, `glue`, `httr`, `jsonlite`, `logger`, `modelsummary`,  
`parallel`, `patchwork`, `purrr`, `readr`, `readxl`, `rlog`, `stringr`,  
`tibble`, `tidyr`, `utils`, `wbstats`

---

## Output and Workflow

To replicate the results, follow the scripts in the `code/` folder in chronological order:

1. **`01_pull_bvd_api_data.py`**  
   Downloads the required Orbis data from the TRR 266 API.

2. **`01b_pull_worldbank_data.R`**  
   Pulls Governance and Development Indicators from the World Bank's API.

3. **`01c_prep_la_porta_data.R`**  
   Prepares data on countries' legal origin (La Porta, López-de-Silanes, Shleifer, and Vishny (1999)).

4. **`02a_calc_transparency_scores.R`**  
   Calculates firm-level transparency scores following Kim & Olbert (2022).

5. **`02b_clean_transparency_scores.R`**  
   Retains only one financial statement per firm-year based on a series of prioritization rules  
   (e.g., consolidation level, reporting period length).

6. **`03_get_entity_level_data.R`**  
   Creates activity spells for each entity and merges firm-level data.

7. **`04_aggregate_data.R`**  
   Aggregates the data to country-year-legal form level.

8. **`05_sample_selection.R`**  
   Tracks the number of firms and countries dropping out in each sample selection step.

9. **`06_descriptives.R`**  
   Generates descriptive statistics for the final sample.

10. **`07a_construct_validity_number_firms.R`**  
    Construct validity test for the denominator of the disclosure rate (number of firms).

11. **`07b_construct_validity_disclosure_rate.R`**  
    Construct validity test for the numerator (number of financial statements).

12. **`08_main_analysis.R`**  
    Runs cross-country regressions and compares disclosure rates around the world.
