


###############################
# Load packages
###############################
suppressMessages({
  library(dplyr)
  library(parallel)
  library(duckdb)
  library(DBI)
  library(glue)
  library(data.table)
  library(tidyr)
  library(stringr)
})


###############################
# Set up 
###############################
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


###############################
# Define a function to aggregate baking data
###############################



get_ctry_level_banks <- function(country){
  
  
  # File paths
  PATH <- paste0("data/banking_data/banks_entity_level_data/",country,"_banks.parquet")
  
  
  # Connect to DuckDB
  con <- connect_duckdb()
  on.exit(disconnect_duckdb(con), add = TRUE)
  
  
  # Create a view
  dbExecute(con, glue("
  CREATE OR REPLACE VIEW entity_level_data AS
  SELECT * FROM read_parquet('{PATH}')
                      "))

  
  ###############################
  # Group by ctry-year
  ###############################

  # Aggregate data
  dt_banking <- tbl(con, "entity_level_data") %>%
    mutate(specialisation = coalesce(specialisation, "Not Available")) %>%
    group_by(year, legal_form, specialisation) %>%
    summarise(
      ctry = country,
      n_banks = n(),
      n_fs = sum(as.integer(!is.na(total_assets))),  
      sum_total_assets = sum(total_assets, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()
  
  

  ###############################
  # Return output
  ###############################
  
  # Log message
  message("Finished processing country: ", country)
  
  # Do some cleaning
  dt_banking    <- dt_banking[, c("ctry", setdiff(names(dt_banking), "ctry"))]
  
  # Return the data frames
  return(dt_banking)
  
}



####################################
# Run function
####################################

# Get all files 
files <- list.files(path = "data/banking_data/banks_entity_level_data")
countries <- substr(files, 1, 2)

# Run for all countries
output_list <- lapply(countries, get_ctry_level_banks)
dt_banks      <- bind_rows(output_list)



####################################
# Save
####################################
dir.create("data/banking_data/banks_aggregated_data", showWarnings = FALSE, recursive = TRUE)
write.csv(dt_banks, "data/banking_data/banks_aggregated_data/dt_banks.csv", row.names = FALSE)



