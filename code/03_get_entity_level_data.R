###############################################################
# Build entity-level panel data (2015–2021) from Orbis files.
# 
# - Filters firms by legal form, status, and activity.
# - Merges transparency scores, listing info, and market data.
# - Outputs one parquet file per country.
#
# Set RUN_PARALLEL=TRUE to enable parallel processing.
#
# Data inputs: Orbis legal info, transparency scores, LSEG data.
# Output path: data/entity_level_data/
###############################################################

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
    "WHERE \"Type of entity\" IN ('Corporate') ",
    "  AND SUBSTR(\"BvD ID number\", 1, 2) IN (%s) ",
    "  AND \"Standardised legal form\" IN ('Private limited companies', 'Public limited companies', 'Partnerships', 'Sole traders/proprietorships') ",
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
# Define function: Filters the data and builds entity-level panel data
##############################

get_entity_level_data <- function(ctry){
  log_info("Start: Entity level data {ctry}")
  
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
    "Type of entity",
    "Listed/Delisted/Unlisted",
    "Main exchange",
    "IPO date",
    "Delisted date"
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
      status_date = `Status date`,
      listing_status = `Listed/Delisted/Unlisted`,
      main_exchange = `Main exchange`,
      ipo_date = `IPO date`,
      delist_date = `Delisted date`
    ) %>%
    filter(
      type_of_entity %in% c("Corporate"),
      legal_form %in% c("Private limited companies", "Public limited companies", "Partnerships", "Sole traders/proprietorships"),
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
  # Get transparency scores
  ##############################
  
  con <- connect_duckdb()
  
  SCORES <- "data/transparency_scores/transparency_scores.parquet"
  dbExecute(con, glue("
  CREATE VIEW transparency_scores AS 
  SELECT * FROM read_parquet('{SCORES}')
"))
  
  transparency_scores <- tbl(con, "transparency_scores") %>%
    select(bvd_id, year, nr_month, bs_total, is_total, notes_total, total_assets, orbis_version) %>%
    filter(bvd_id %like% paste0(ctry, "%")) %>%
    mutate(
      bs_total = coalesce(bs_total, 0),
      is_total = coalesce(is_total, 0),
      notes_total = coalesce(notes_total, 0),
      fs_total = bs_total + is_total + notes_total,
      year = as.numeric(year)
    ) %>%
    collect()
  
  dbDisconnect(con, shutdown = TRUE)
  
  
  dt <- left_join(dt, transparency_scores, by = c("bvd_id", "year"))
  
  rm(transparency_scores)
  gc()
  
  
  
  
  
  ##############################
  # Get Listing Status
  ##############################
  
  # Prep listing data
  listing_info <- legal_info %>%
    filter(listing_status %in% c("Listed")) %>% 
    mutate(
      ipo_year   = as.integer(substr(ipo_date, 1, 4)),
      start_year = ifelse(is.na(ipo_year), 
                          min(analysis_years), # Assumption: If the IPO date is missing, assume they are listed over the full horizon
                          ipo_year+1 ) # Make sure that they have been listed for at least one business year
    ) %>%
    select(bvd_id, listing_status, main_exchange, ipo_year, start_year) %>%
    filter(!start_year > max(analysis_years)) # Delete those that have a IPO after the analysis horizon
  
  
  
  
  
  if (nrow(listing_info) > 0) {
    
    # Make a panel from the beginning of the analysis horizon (or the IPO date) to the end of the analysis horizon
    listing_info <- listing_info %>%
      rowwise() %>%
      mutate(years_listed = list(max(start_year, min(analysis_years)):max(analysis_years))) %>%  
      unnest(years_listed) %>%
      ungroup() %>%
      rename(year = years_listed) %>%
      select(-start_year)
    
    dt <- left_join(dt, listing_info, by = c("bvd_id", "year"))
    
    dt$listed <- dt$listing_status %in% "Listed"
    
    rm(listing_info)
    gc()
    
  } else {
    dt$listed <- FALSE
    dt$main_exchange <- NA
    dt$ipo_year <- NA
  }
  
  
  ##############################
  # Get trading volume and total assets data
  ##############################
  
  if (file.exists(paste0("data/lseg_data/",ctry,"_lseg_data.rds"))) {
    
    lseg_data <- readRDS(paste0("data/lseg_data/",ctry,"_lseg_data.rds"))
    
    lseg_data <- lseg_data %>%
      select(bvd_id, year, lseg_volume, lseg_toas, isin)
    
    dt <- left_join(dt, lseg_data, by = c("bvd_id", "year"))
    
    rm(lseg_data)
    gc()
    
  } else {
    dt$lseg_volume <- NA
    dt$lseg_toas <- NA
    dt$isin <- NA
  }
  
  ##############################
  # Clean
  ##############################
  
  dt <- dt %>%
    select(bvd_id, year, legal_form, listed, main_exchange, ipo_year, nr_month, fs_total, bs_total, is_total, notes_total, total_assets, orbis_version, lseg_volume, lseg_toas, isin)
  
  
  ########################################
  # Save result
  ########################################
  dir.create("data/entity_level_data", showWarnings = FALSE, recursive = TRUE)
  write_parquet(dt, paste0("data/entity_level_data/", ctry,"_entity_level_data.parquet"))
  log_info("End: Entity level data {ctry}")
  
  
  
}


########################################
# Run function
########################################

#Check if some countries are alredy done
done <- list.files(path = "data/entity_level_data")
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
  parLapply(cl, ctries, get_entity_level_data)
  
  
}else{
  
  # Run each country separately 
  for(ctry in ctries){
    get_entity_level_data(ctry)
  }
  
}
