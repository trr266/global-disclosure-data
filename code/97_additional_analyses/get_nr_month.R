
# Get

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

get_nr_month <- function(country){
  
  
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
  
  output <- dbGetQuery(con, "
  SELECT nr_month, COUNT(*) AS n_obs
  FROM entity_level_data
  GROUP BY nr_month
  ORDER BY nr_month
")
  
  # Clean
  output <- output %>%
    filter(!is.na(nr_month)) %>%
    mutate(ctry = country) %>%
    select(ctry, nr_month, n_obs)
  
  # Log message
  message("Finished processing country: ", country)
  
  # Return the data frame
  return(list(df_nr_month = output))
  
}



# Get all files 
files <- list.files(path = "data/entity_level_data")
countries <- substr(files, 1, 2)



# Run for all countries
output_list <- lapply(countries, get_nr_month)
df_nr_month <- bind_rows(lapply(output_list, `[[`, "df_nr_month"))


dt <- df_nr_month %>%
  group_by(ctry) %>%
  summarise(
    n_12 = sum(n_obs[nr_month == 12]),
    n_not_12 = sum(n_obs[nr_month != 12]),
    share = n_not_12 / (n_12 + n_not_12)
  )
