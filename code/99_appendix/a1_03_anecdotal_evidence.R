
###############################################
# Load needed packages 
###############################################

suppressMessages({
  library(DBI)
  library(glue)
})


###############################################
# Define functions and parameters to access parquet files 
###############################################


# Define helper to build parquet path
pdata <- function(pfile) {
  file.path(PARQUET_FOLDER, pfile) 
} 

# Functions to manage DuckDB connection
connect_duckdb <- function(dbase) {
  con <- dbConnect(duckdb::duckdb(), ":memory:")
  return(con)
}

disconnect_duckdb <- function(con){
  dbDisconnect(con, shutdown = TRUE)
}




###############################################
# Find firms with data from 2019 version
###############################################

country <- "DE"

# File paths
PATH <- paste0("data/entity_level_data/",country,"_entity_level_data.parquet")


# Connect to DuckDB
con <- connect_duckdb()
on.exit(disconnect_duckdb(con), add = TRUE)


# Create a view
dbExecute(con, glue("
  CREATE OR REPLACE VIEW entity_level_data AS
  SELECT * FROM read_parquet('{PATH}')
                      "))


version_summary <- tbl(con, "entity_level_data") %>%
  group_by(bvd_id) %>%
  summarize("n_2019" = sum(as.integer(orbis_version %in% "2019-08"))) %>%
  collect()



###############################################
# Set BvD ID
###############################################

bvd_id <- "DE5350139159"

###############################################
# 2024 Data
###############################################

# Set parquet folder
PARQUET_FOLDER <- "data/orbis_data/2024-12"

financial_info <- pdata("industry_global_financials_and_ratios_usd.parquet")


financial_info_cols <- c("BvD ID number",
                         "Consolidation code",
                         "Closing date",
                         "Number of months",
                         "Total Assets")

# Query string adjusted to filter by BvD ID number instead of country
query_str <- sprintf(
  paste0("SELECT \"%s\" FROM '%s' WHERE \"BvD ID number\" = '%s'"),
  paste(financial_info_cols, collapse = '", "'),
  financial_info,
  bvd_id
)

con <- connect_duckdb(":memory:")
financials_2024  <- dbGetQuery(con, query_str)
disconnect_duckdb(con)


# Get contact data
contact_info <- pdata("contact_info.parquet")

contact_info_cols <- c("BvD ID number",
                       "NAME_INTERNAT",
                       "Website address",
                       "Telephone number",
                       "Country ISO code")

# Query string adjusted to filter by BvD ID number instead of country
query_str <- sprintf(
  paste0("SELECT \"%s\" FROM '%s' WHERE \"BvD ID number\" = '%s'"),
  paste(contact_info_cols, collapse = '", "'),
  contact_info,
  bvd_id
)

con <- connect_duckdb(":memory:")
contact_info <- dbGetQuery(con, query_str)
disconnect_duckdb(con)


###############################################
# 2019 Data
###############################################

# Set parquet folder
PARQUET_FOLDER <- "data/orbis_data/2019-08"

financial_info <- pdata("industry_global_financials_and_ratios_usd.parquet")


financial_info_cols <- c("BvD ID number",
                         "Consolidation code",
                         "Closing date",
                         "Number of months",
                         "Total Assets")

# Query string adjusted to filter by BvD ID number instead of country
query_str <- sprintf(
  paste0("SELECT \"%s\" FROM '%s' WHERE \"BvD ID number\" = '%s'"),
  paste(financial_info_cols, collapse = '", "'),
  financial_info,
  bvd_id
)

con <- connect_duckdb(":memory:")
financials_2019  <- dbGetQuery(con, query_str)
disconnect_duckdb(con)

