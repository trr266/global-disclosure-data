
##############################
# Load packages
##############################

suppressMessages({
  library(duckdb)
  library(DBI)
  library(dplyr)
})

##############################
# Set up
##############################


pdata <- function(pfile) {
  file.path(PARQUET_FOLDER, pfile) 
} 

connect_duckdb <- function(dbase) {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  return(con)
}

disconnect_duckdb <- function(con){
  DBI::dbDisconnect(con, shutdown = TRUE)
}

PARQUET_FOLDER <- "data/orbis_data/2024-12"


# Path to parquet file
legal_info_path <- pdata("legal_info.parquet")


query_str_us_legal_pairs <- sprintf(
  paste0(
    "SELECT DISTINCT ",
    "  COALESCE(\"National legal form\", 'NA') AS national_legal_form, ",
    "  COALESCE(\"Standardised legal form\", 'NA') AS standardised_legal_form ",
    "FROM '%s' ",
    "WHERE SUBSTR(\"BvD ID number\", 1, 2) = 'US' ",
    "ORDER BY national_legal_form, standardised_legal_form"
  ),
  legal_info_path
)


con <- connect_duckdb(":memory:")
dt <- dbGetQuery(con, query_str_us_legal_pairs)
disconnect_duckdb(con)

