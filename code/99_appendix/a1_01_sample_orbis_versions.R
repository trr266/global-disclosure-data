suppressMessages({
  library(dplyr)
  library(parallel)
  library(duckdb)
  library(DBI)
  library(glue)
  library(data.table)
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

get_version <- function(country){
  
  
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
  
  
  # Load exchange data
  exchange_data <- read.csv("data/country_characteristics/exchange_data.csv")
  
  
  df_ctry <- tbl(con, "entity_level_data") %>%
    filter(fs_total > 0) %>%
    summarise(
      ctry = country,
      n = sum(as.integer(!is.na(orbis_version))),
      version_2019 = sum(as.integer(orbis_version == "2019-08")),
      version_2024 = sum(as.integer(orbis_version == "2024-12"))
    ) %>%
    collect()
  
  return(df_ctry)
}




# Get all files 
files <- list.files(path = "data/entity_level_data")
countries <- substr(files, 1, 2)

# Exclude countries that are not part of the sample
excluded_ctries <- c("CD", "ME", "RS", "SS", "ER", "VE", "GD", "FJ")
countries <- countries[!countries %in% excluded_ctries]

# Run for all countries
dt <- lapply(countries, get_version)
dt <- bind_rows(dt)

# Save
dir.create("output/orbis_version_summary", showWarnings = FALSE, recursive = TRUE)
write.csv(dt, "output/orbis_version_summary/orbis_version_summary.csv", row.names = FALSE)


