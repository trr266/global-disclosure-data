
##############################################
# Cleans bank financials
##############################################

suppressPackageStartupMessages(suppressWarnings({
  library(dplyr)
  library(duckdb)
  library(glue)
}))

# Load configuration
source("code/config.R")

# Utility functions
pdata <- function(pfile) {
  file.path(PARQUET_FOLDER, pfile) 
} 

connect_duckdb <- function(dbase = ":memory:") {
  dbConnect(duckdb::duckdb(), dbase)
}

disconnect_duckdb <- function(con) {
  dbDisconnect(con, shutdown = TRUE)
}

# Set uo directory
dir_path <- "data/banking_data/banks_financials"
if (!dir.exists(dir_path)) {
  dir.create(dir_path, recursive = TRUE)
}



# Start calculation
con <- connect_duckdb()

# File paths
ORBIS_2024 <- "data/orbis_data/2024-12/banks_global_financials_and_ratios_usd.parquet"
ORBIS_2019 <- "data/orbis_data/2019-08/banks_global_financials_and_ratios_usd.parquet"

# Combine datasets
dbExecute(con, glue("
  CREATE OR REPLACE VIEW combined_financials AS
  SELECT *, '2024-12' AS orbis_version FROM read_parquet('{ORBIS_2024}')
  UNION ALL
  SELECT *, '2019-08' AS orbis_version FROM read_parquet('{ORBIS_2019}')
"))

# Process and clean
banks_financials <- tbl(con, "combined_financials") %>%
  transmute(
    bvd_id = `BvD ID number`,
    fye = as.character(`Closing date`),
    conscode = `Consolidation code`,
    nr_month = `Number of months`, 
    total_assets = `Total assets`,
    orbis_version = orbis_version
  ) %>%
  select(bvd_id, fye, conscode, nr_month, total_assets, orbis_version)

copy_to(con, banks_financials, "banks_financials", temporary = FALSE, overwrite = TRUE)

# Rank and filter 
dbExecute(con, glue("
  CREATE OR REPLACE TABLE banks_financials AS
  WITH base AS (
    SELECT *,
      substr(fye, 1, 4) AS year,
      CASE WHEN conscode = 'C1' THEN 4
           WHEN conscode = 'C2' THEN 3
           WHEN conscode = 'U1' THEN 2
           WHEN conscode = 'U2' THEN 1
           ELSE 0 END AS cc_rank,
      CASE WHEN nr_month = 12 THEN 1 ELSE 0 END AS month_rank
    FROM banks_financials
    WHERE nr_month BETWEEN 1 AND 24
      AND conscode IN ('C1', 'C2', 'U1', 'U2')
  ),
  ranked AS (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY bvd_id, year
        ORDER BY cc_rank DESC,           
                 month_rank DESC, 
                 fye DESC,
                 nr_month DESC,
                 orbis_version DESC,
                 total_assets DESC 
      ) AS rn
    FROM base
  )
  SELECT * EXCLUDE (cc_rank, month_rank, rn)
  FROM ranked
  WHERE rn = 1
"))

# Export to parquet
dbExecute(con, "
  COPY banks_financials
  TO 'data/banking_data/banks_financials/banks_financials.parquet' 
  (FORMAT 'parquet');
")

# Disconnect
disconnect_duckdb(con)






