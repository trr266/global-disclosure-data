
###############################################################
# Aggregate entity-level panels to country-year summaries.
#
# - Groups data by country, year, legal form
# - Distinguishes between domestic/WFE-listed and other listed firms.
# - Merges in exchange characteristics (WFE, OTC).
# - Produces summary statistics and firm counts.
#
# Input: data/entity_level_data/
# Output: data/aggregated_data/
###############################################################



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
# Define a function to aggregate entity-level data
###############################

get_ctry_level_data <- function(country){
  
  
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
  
  ###############################
  # Group by county
  ###############################
  dt_ctry <- tbl(con, "entity_level_data")  %>%
    summarise(
      ctry = country,
      firms = n_distinct(bvd_id),
      
      # Extensive Margin
      n_fs = sum(as.integer(fs_total > 0), na.rm = TRUE),
      n_bs = sum(as.integer(bs_total > 0), na.rm = TRUE),
      n_is = sum(as.integer(is_total > 0), na.rm = TRUE),
      n_notes = sum(as.integer(notes_total > 0), na.rm = TRUE),
      
      # Intensive Margin
      fs_items = mean(ifelse(fs_total > 0, fs_total, NA), na.rm = TRUE),
      bs_items = mean(ifelse(bs_total > 0, bs_total, NA), na.rm = TRUE),
      is_items = mean(ifelse(is_total > 0, is_total, NA), na.rm = TRUE),
      notes_items = mean(ifelse(notes_total > 0, notes_total, NA), na.rm = TRUE),
      
      .groups = "drop"
    ) %>%
    collect()
  
  
  ###############################
  # Load exchange data and further aggregate data 
  ###############################
  exchange_data <- read.csv("data/country_characteristics/exchange_data.csv")
  exchange_data$exchange_country[is.na(exchange_data$exchange_country)] <- "NA"
  
  # Split and unnest
  exchange_data <- exchange_data %>%
    mutate(exchange_country = str_split(exchange_country, ",")) %>%
    unnest(exchange_country)
  
  # Filter domestic exchanges and those that are WFE members (and London Stock Exchange)
  domestic_wfe_members <- exchange_data %>%
    filter(exchange_country %in% country) %>%
    filter(wfe_member == 1 | exchange_name %in% "London Stock Exchange")
  
  # Aggregate stock exchanges and combine listed and legal_form variable
  dt_ctry_year <- tbl(con, "entity_level_data") %>%
    mutate(
      listed = case_when(
        listed == TRUE & !is.na(main_exchange) & main_exchange %in% domestic_wfe_members$exchange_name ~ "Listed",
        listed == TRUE & (!is.na(main_exchange) & !(main_exchange %in% domestic_wfe_members$exchange_name)) ~ "Other Listed",
        listed == TRUE & is.na(main_exchange) ~ "Other Listed",
        TRUE ~ "FALSE"
      ),
      legal_form = case_when(
        legal_form %in% c("Public limited companies", "Private limited companies") ~ "Limited liability companies",
        TRUE ~ legal_form
      ),
      # Replace legal_form with listed if listed is not FALSE
      legal_form = case_when(
        listed == "FALSE" ~ legal_form,
        TRUE ~ listed
      )
    ) %>%
    group_by(year, legal_form) %>%
    summarise(
      ctry = country,
      firms = n(),
      
      # Extensive Margin
      n_fs = sum(as.integer(fs_total > 0), na.rm = TRUE),
      n_bs = sum(as.integer(bs_total > 0), na.rm = TRUE),
      n_is = sum(as.integer(is_total > 0), na.rm = TRUE),
      n_notes = sum(as.integer(notes_total > 0), na.rm = TRUE),
      
      # Intensive Margin
      fs_items = mean(ifelse(fs_total > 0, fs_total, NA), na.rm = TRUE),
      bs_items = mean(ifelse(bs_total > 0, bs_total, NA), na.rm = TRUE),
      is_items = mean(ifelse(is_total > 0, is_total, NA), na.rm = TRUE),
      notes_items = mean(ifelse(notes_total > 0, notes_total, NA), na.rm = TRUE),
      
      # Aggregate total assets
      aggregated_total_assets = sum(total_assets, na.rm = TRUE),
      
      .groups = "drop"
    ) %>%
    collect()
  
  
  ###############################
  # Return output
  ###############################
  
  # Log message
  message("Finished processing country: ", country)
  log_info("{country}: Done (Country level data)")
  
  # Do some cleaning
  dt_ctry         <- dt_ctry[, c("ctry", setdiff(names(dt_ctry), "ctry"))]
  dt_ctry_year    <- dt_ctry_year[, c("ctry", setdiff(names(dt_ctry_year), "ctry"))]
  
  dt_ctry[c("n_fs", "n_bs", "n_is", "n_notes")] <- lapply(
    dt_ctry[c("n_fs", "n_bs", "n_is", "n_notes")],
    function(x) ifelse(is.na(x), 0, x)
  )
  
  dt_ctry_year[c("n_fs", "n_bs", "n_is", "n_notes")] <- lapply(
    dt_ctry_year[c("n_fs", "n_bs", "n_is", "n_notes")],
    function(x) ifelse(is.na(x), 0, x)
  )
  
  # Return the data frames
  return(list(dt_ctry_year = dt_ctry_year,
              dt_ctry      = dt_ctry))
  
}


####################################
# Run function
####################################

# Get all files 
files <- list.files(path = "data/entity_level_data")
countries <- substr(files, 1, 2)

# Run for all countries
output_list <- lapply(countries, get_ctry_level_data)
dt_ctry      <- bind_rows(lapply(output_list, `[[`, "dt_ctry"))
dt_ctry_year <- bind_rows(lapply(output_list, `[[`, "dt_ctry_year"))



####################################
# Save
####################################
dir.create("data/aggregated_data", showWarnings = FALSE, recursive = TRUE)
write.csv(dt_ctry, "data/aggregated_data/dt_ctry.csv", row.names = FALSE)
write.csv(dt_ctry_year, "data/aggregated_data/dt_ctry_year.csv", row.names = FALSE)


