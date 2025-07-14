
###################################
# This script loads raw transparency score data and
# processes it in DuckDB to retain only one financial statement per firm-year 
# based on a series of prioritization rules (e.g., consolidation level, reporting period length)
# The cleaned file is exportd to a new Parquet file
###################################

suppressPackageStartupMessages(suppressWarnings({
  library(dplyr)
  library(duckdb)
  library(glue)
}))

source("code/config.R")

# Define connection functions
connect_duckdb <- function(dbase = NULL) {
  con <- dbConnect(duckdb::duckdb(), ":memory:")
  return(con)
}

disconnect_duckdb <- function(con) {
  dbDisconnect(con, shutdown = TRUE)
}

# Connect to DuckDB
con <- connect_duckdb()

# File path
path <- "data/transparency_scores/transparency_scores_raw.parquet"

# Create a view with the single dataset
dbExecute(con, glue("
  CREATE OR REPLACE VIEW transparency_scores AS
  SELECT * FROM read_parquet('{path}')
"))

# Performs the following filtering steps and keeps only statement per firm-year:

# Take most aggregated statement (highest cc_rank)
# Among those take the one with a 12 month horizon if possible
# Among those, take the most recent closing date
# If there are no reports for a 12 month horizon take the one with the longest reporting period
# Among those, prefer the newer orbis_version
# Among those prefer the one with more bs items
# Among those prefer the one with more is items
# Among those prefer the one with more notes items
# If still more than one observation, take first

dbExecute(con, glue("
  CREATE OR REPLACE TABLE transparency_scores_clean AS
  WITH base AS (
    SELECT *,
      substr(fye, 1, 4) AS year,
      CASE WHEN conscode = 'C1' THEN 4
           WHEN conscode = 'C2' THEN 3
           WHEN conscode = 'U1' THEN 2
           WHEN conscode = 'U2' THEN 1
           ELSE 0 END AS cc_rank,
      CASE WHEN nr_month = 12 THEN 1 ELSE 0 END AS month_rank
    FROM transparency_scores
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
                 bs_total DESC,
                 is_total DESC,
                 notes_total DESC
      ) AS rn
    FROM base
  )
  SELECT * EXCLUDE (cc_rank, month_rank, rn)
  FROM ranked
  WHERE rn = 1
"))

# Export to parquet
dbExecute(con, "
  COPY transparency_scores_clean
  TO 'data/transparency_scores/transparency_scores.parquet' 
  (FORMAT 'parquet');
")

# Disconnect
disconnect_duckdb(con)
