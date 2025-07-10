##############################
# Function: get_lseg_data
#
# This function retrieves information from LSEG via WRDS:
#
# - Loads available ISINs for a given country from Orbis (identifiers.parquet)
# - Uses ISINs to query LSEG via WRDS:
#     - Loads trading volume data (TRDSTRM / wrds_ds2dsf)
#     - Loads total assets data (TRWS / wrds_ws_funda)
# - Merges both sources at the BvD ID-year level
# - Ensures only one observation per BvD ID-year, selecting the one 
#   with highest total assets if multiple ISINs map to the same firm
#
##############################



##############################
# Load necessary packages
##############################
suppressMessages({
  library(tidyverse)
  library(RPostgres)
  library(duckdb)
  library(dbplyr)
  library(dotenv)
})

##############################
# Set up specifications
##############################

source("code/config.R")
load_dot_env(file = "global-disclosure-data.env")

WRDS_USER_NAME =  Sys.getenv("WRDS_USER_NAME")
WRDS_PASSWORD = Sys.getenv("WRDS_PASSWORD")

PARQUET_FOLDER <- "data/orbis_data/2024-12"


##############################
# Define functions 
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


connect_wrds <- function() {
  dbConnect(
    Postgres(),
    host = 'wrds-pgdata.wharton.upenn.edu',
    port = 9737,
    dbname = 'wrds',
    sslmode = 'require',
    user = WRDS_USER_NAME,
    password = WRDS_PASSWORD
  )
}



get_lseg_data <- function(ctry, year_min = 2015, output_dir = "data/lseg_data") {
  
  log_info("{ctry}: Start pulling LSEG data")
  
  ########################################
  # Get all ISINs in Orbis for country
  ########################################
  
  identifiers_path <- pdata("identifiers.parquet")
  identifiers_cols <- c("BvD ID number", "ISIN number")
  
  con <- connect_duckdb(":memory:")
  on.exit(disconnect_duckdb(con), add = TRUE)
  
  query_str <- sprintf(
    paste0("SELECT \"%s\" FROM '%s' WHERE \"BvD ID number\" LIKE '%s%%'"),
    paste(identifiers_cols, collapse = '", "'),
    identifiers_path,
    ctry         
  )
  
  identifiers <- DBI::dbGetQuery(con, query_str) %>%
    filter(!is.na(`ISIN number`))
  
  
  isins <- identifiers$`ISIN number`
  if (length(isins) == 0) {
    log_info("No ISINs found for {ctry}")
    return(invisible(NULL))
  }
  
  # Output file
  file_name <- file.path(output_dir, paste0(ctry, "_lseg_data.rds"))
  if (file.exists(file_name)) {
    log_info("Skipping {ctry} — file already exists.")
    return(invisible(NULL))
  }
  
  
  ########################################
  # Connect to WRDS and get LSEG Volume data for all ISINs
  ########################################
  
  wrds <- connect_wrds()
  on.exit(dbDisconnect(wrds), add = TRUE)
  
  lseg_identifiers <- tbl(wrds, in_schema("trdstrm", "wrds_ds_names"))
  lseg_equities_data <- tbl(wrds, in_schema("trdstrm", "wrds_ds2dsf"))
  
  dt_volume <- lseg_identifiers %>%
    filter(isin %in% isins) %>%
    inner_join(lseg_equities_data, by = "infocode") %>%
    filter(marketdate >= as.Date(paste0(as.integer(year_min), "-01-01"))) %>%
    select(isin, marketdate, volume) %>%
    mutate(year = sql("EXTRACT(YEAR FROM marketdate)")) %>%
    group_by(isin, year) %>%
    summarise(lseg_volume = sum(volume, na.rm = TRUE), .groups = "drop") %>%
    collect() 
  
  
  ########################################
  # Connect to WRDS and get LSEG Total Assets data for all ISINs
  ########################################  
  
  toas_data <- tbl(wrds, in_schema("trws", "wrds_ws_funda"))
  
  dt_toas <- toas_data %>%
    filter(item6008 %in% isins) %>%
    select(item6008, item7230, item5350) %>%
    rename(isin = item6008,
           lseg_toas = item7230,
           lseg_closedate = item5350) %>%
    mutate(year = year(lseg_closedate)) %>%
    filter(year > year_min) %>%
    collect() %>%
    
    # Arrange to prioritize highest lseg_closedate and then lseg_toas
    arrange(isin, year, desc(lseg_closedate), desc(lseg_toas), is.na(lseg_toas)) %>%
    
    # Keep only first row per isin-year combination
    group_by(isin, year) %>%
    slice(1) %>%
    select(-lseg_closedate) %>%
    ungroup()
  
  # Merge
  dt_lseg <- full_join(dt_volume, dt_toas, by = c("isin", "year"))
  dt <- merge(identifiers, dt_lseg,  by.x =  "ISIN number", by.y = "isin" )
  
  dt <- dt %>% rename(bvd_id = `BvD ID number`,
                      isin = `ISIN number`)
  
  ########################################
  # Clean & Save result
  ########################################
  
  dt_clean <- dt %>%
    group_by(bvd_id, year) %>%
    arrange(desc(lseg_toas), .by_group = TRUE) %>%  # prioritize highest lseg_toas (likely most aggregated)
    slice(1) %>%  # take first after ordering
    ungroup()
  
  
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  saveRDS(dt_clean, file = file_name)
  log_info("Done {ctry}")
}



########################################
# Run function for each country separately
########################################


# Get list of countries
ctry_profiles <-  read.csv("data/country_characteristics/ctry_profiles.csv")
ctries <- ctry_profiles$iso2
done <- list.files(path = "data/lseg_data")
done <- substr(done, 1, 2)
ctries <- ctries[!ctries %in% done]

# Run loop
for(ctry in ctries){
  get_lseg_data(ctry)
}  