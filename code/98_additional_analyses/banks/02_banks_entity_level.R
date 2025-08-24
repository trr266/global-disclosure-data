
##############################
# Load packages
##############################

suppressMessages({
  library(duckdb)
  library(DBI)
  library(dplyr)
  library(tidyr)
  library(rlog)
  library(parallel)
  library(glue)
  library(arrow)
})

##############################
# Set up
##############################

source("code/config.R")

run_parallel <- toupper(Sys.getenv("RUN_PARALLEL")) == "TRUE"

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



###############################################
# Get UN constituents
###############################################

ctry_profiles <- read.csv("data/country_characteristics/ctry_profiles.csv")
ctries <- ctry_profiles$iso2
ctries_sql <- paste0("'", ctries, "'", collapse = ", ")


###############################################
# Check winch UN Constituents remain in the sample after filtering
###############################################

# Path to parquet file
legal_info_path <- pdata("legal_info.parquet")

query_str <- sprintf(
  paste0(
    "SELECT ",
    "  SUBSTR(\"BvD ID number\", 1, 2) AS country_code, ",
    "  COUNT(*) AS n_obs ",
    "FROM '%s' ",
    "WHERE \"Type of entity\" IN ('Bank') ",
    "  AND SUBSTR(\"BvD ID number\", 1, 2) IN (%s) ",
    "  AND (",
    "    \"Date of incorporation\" IS NULL OR ",
    "    CAST(LEFT(CAST(\"Date of incorporation\" AS VARCHAR), 4) AS INTEGER) <= 2021",
    "  ) ",
    "  AND NOT (",
    "    Status IN (",
    "      'Administratively suspended', ",
    "      'Bankruptcy', ",
    "      'Dissolved', ",
    "      'Dissolved (bankruptcy)', ",
    "      'Dissolved (demerger)', ",
    "      'Dissolved (liquidation)', ",
    "      'Dissolved (merger or take-over)', ",
    "      'In liquidation', ",
    "      'Inactive (no precision)'",
    "    ) ",
    "    AND (\"Status date\" IS NULL OR ",
    "         CAST(LEFT(CAST(\"Status date\" AS VARCHAR), 4) AS INTEGER) < 2015)",
    "  ) ",
    "GROUP BY country_code ",
    "ORDER BY n_obs DESC"
  ),
  legal_info_path,
  ctries_sql
)

# Connect, query, and disconnect
con <- connect_duckdb(":memory:")
sample_size <- dbGetQuery(con, query_str)
disconnect_duckdb(con)


sum(sample_size$n_obs)
ctries <- sample_size$country_code



##############################
# Define function: Filters the data and build bank-level panel data
##############################

get_bank_data <- function(ctry){
  
  log_info("Start processing {ctry}")
  
  # Set horizon for analysis 
  analysis_years <- 2015:2021
  
  # Define inactive statuses 
  inactive_statuses <- c(
    "Administratively suspended",
    "Bankruptcy",
    "Dissolved",
    "Dissolved (bankruptcy)",
    "Dissolved (demerger)",
    "Dissolved (liquidation)",
    "Dissolved (merger or take-over)",
    "In liquidation",
    "Inactive (no precision)"
  )
  
  # Get relevant variables
  legal_info_cols <- c(
    "BvD ID number",
    "Standardised legal form",
    "Status",
    "Status date", 
    "Date of incorporation",
    "Type of entity"
  )
  
  con <- connect_duckdb(":memory:")
  
  min_year <- min(analysis_years)
  max_year <- max(analysis_years)
  
  legal_info <- tbl(con, pdata("legal_info.parquet")) %>%
    select(all_of(legal_info_cols)) %>%
    filter(`BvD ID number` %like% paste0(ctry, "%")) %>%
    transmute(
      bvd_id = `BvD ID number`,
      legal_form = `Standardised legal form`,
      type_of_entity = `Type of entity`,
      status = Status,
      incorp_date = `Date of incorporation`,
      status_date = `Status date`
    ) %>%
    filter(
      type_of_entity %in% c("Bank"),
      !(status %in% inactive_statuses &
          (is.na(status_date) | as.integer(substr(as.character(status_date), 1, 4)) < !!min_year)),
      (
        is.na(incorp_date) |
          as.integer(substr(as.character(incorp_date), 1, 4)) <= !!max_year
      )
    ) %>%
    collect()
  
  disconnect_duckdb(con)
  
  
  # Summarize status in three groups and keep year
  dt <- legal_info %>%
    mutate(
      status = ifelse(
        grepl("Active", substr(status,1,6), ignore.case = F), "active",
        ifelse(status %in% "Status unknown", "unknown", "inactive")),
      incorp_year = as.integer(substr(incorp_date, 1, 4)),
      status_year = as.integer(substr(status_date, 1, 4))
    ) 
  
  # Get start and end year for panel
  dt <- dt %>%
    mutate(
      start_year = ifelse(is.na(incorp_year), min(analysis_years), incorp_year),  # Set to start of analysis horizon if incorporation date is missing
      end_year = ifelse(status %in% "inactive", status_year, max(analysis_years))  # Set to end of horizon if not inactive. For inactive firms set it to status date
    )
  
  
  # Set start year to start of analysis for those founded before that
  dt$start_year[dt$start_year < min(analysis_years)] <- min(analysis_years)
  
  # Set end year to end of analysis for those going inactive after that
  dt$end_year[dt$end_year > max(analysis_years)] <- max(analysis_years)
  
  
  # Make a panel from start to end year
  dt <- dt %>%
    rowwise() %>%
    mutate(years_active = list(start_year:end_year)) %>%
    unnest(years_active) %>%
    ungroup() %>%
    rename(year = years_active)
  
  # Keep needed variables
  dt <- dt %>% select(bvd_id, year, legal_form)
  
  
  ##############################
  # Get financials
  ##############################
  
  con <- connect_duckdb()
  
  # Load cleaned financial data
  FINANCIALS <- "data/banking_data/banks_financials/banks_financials.parquet"
  dbExecute(con, glue("
  CREATE VIEW banks_financials AS 
  SELECT * FROM read_parquet('{FINANCIALS}')
"))
  
  
  banks_financials <- tbl(con, "banks_financials") %>%
    filter(sql(glue("bvd_id LIKE '{ctry}%'"))) %>% 
    mutate(year = as.numeric(year)) %>%
    collect()
  
  
  dbDisconnect(con, shutdown = TRUE)
  
  # Merge 
  dt <- left_join(dt, banks_financials , by = c("bvd_id", "year"))
  
  # Clean up
  rm(banks_financials)
  gc()
  
  
  
  
  ##############################
  # Get specialization
  ##############################

  con <- connect_duckdb()
  
  # Load trade description parquet file into DuckDB
  SCORES <- "data/orbis_data/2024-12/trade_description.parquet"
  dbExecute(con, glue("
  CREATE VIEW trade_description AS 
  SELECT * FROM read_parquet('{SCORES}')
"))
  

  trade_description <- tbl(con, "trade_description") %>%
    select(
      `BvD ID number`,
      `Specialisation (banks only)`
    ) %>%
    rename(
      bvd_id = `BvD ID number`,
      specialisation = `Specialisation (banks only)`
    ) %>%
    filter(sql(glue("bvd_id LIKE '{ctry}%'"))) %>% 
    collect()
  

  dbDisconnect(con, shutdown = TRUE)
  
  # Merge 
  dt <- left_join(dt, trade_description, by = "bvd_id")
  
  # Clean up
  rm(trade_description)
  gc()
  
  

  
  ########################################
  # Save result
  ########################################
  dir.create("data/banking_data/banks_entity_level_data/", showWarnings = FALSE, recursive = TRUE)
  write_parquet(dt, paste0("data/banking_data/banks_entity_level_data/", ctry,"_banks.parquet"))
  
  log_info("End processing {ctry}")
  
}

########################################
# Run function
########################################

#Check if some countries are already done
done <- list.files(path = "data/banking_data/banks_entity_level_data")
done <- substr(done, 1, 2)
ctries <- ctries[!ctries %in% done]


# Take care of Namibia
ctries[is.na(ctries)] <- "NA"

if (run_parallel) {
  
  # Detect the number of cores available for parallel processing
  num_cores <- detectCores() - 1  
  
  # Create a cluster
  cl <- makeCluster(num_cores)
  rv <- clusterEvalQ(cl, {
    library(duckdb)
    library(DBI)
    library(dplyr)
    library(tidyr)
    library(rlog)
    library(parallel)
    library(glue)
    library(arrow)
    source("code/config.R")
  })
  
  rv <- clusterExport(
    cl,
    varlist = c("PARQUET_FOLDER", "connect_duckdb", "disconnect_duckdb", "pdata")
  )
  
  # Use parLapply to run the function in parallel across the country data
  parLapply(cl, ctries, get_bank_data)
  
  
}else{
  
  # Run each country separately 
  for(ctry in ctries){
    get_bank_data(ctry)
  }
  
}

