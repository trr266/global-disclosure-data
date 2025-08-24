

##############################
# This file loads country level data and compares the total assets provided by Orbis 
# to the Total Assets in in LSEG
##############################
suppressMessages({
  library(dplyr)
  library(parallel)
  library(duckdb)
  library(DBI)
  library(glue)
  library(tidyr)
  library(data.table)
  library(stringr)
})

source("code/config.R")

run_parallel <- toupper(Sys.getenv("RUN_PARALLEL")) == "TRUE"


# Define connection functions
connect_duckdb <- function(dbase = NULL) {
  con <- dbConnect(duckdb::duckdb(), ":memory:")
  return(con)
}

disconnect_duckdb <- function(con) {
  dbDisconnect(con, shutdown = TRUE)
}


orbis_lseg_comparison <- function(country){
  
  
  
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
  
  dt <- tbl(con, "entity_level_data") %>%
    filter(listed) %>%
    collect()
  
  
  dt <- dt %>% 
    mutate(ctry = country) %>%
    select(ctry, bvd_id, year,total_assets, lseg_toas)
  
  # Log message
  message("Finished processing country: ", country)
  
  dt
}

# Get countries with listed firms
dt_ctry_year <- read.csv("data/aggregated_data/dt_ctry_year.csv")
listed <- dt_ctry_year %>% filter(legal_form %in% c("Listed", "Other Listed"))
ctries <- unique(listed$ctry)

# Exclude countries that are not part of the sample
excluded_ctries <- c("CD", "ME", "RS", "SS", "ER", "VE")
ctries  <- ctries [!ctries  %in% excluded_ctries]



# Set up and run function for countries with wfe member stock exchanges
results <- lapply(ctries, orbis_lseg_comparison)
output <- bind_rows(results)



# Save result
dir.create("output/orbis_lseg_comparison", showWarnings = FALSE, recursive = TRUE)
saveRDS(output, file = "output/orbis_lseg_comparison/orbis_lseg_comparison.rds")


