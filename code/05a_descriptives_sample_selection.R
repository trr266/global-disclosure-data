
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


# Get UN constituents
ctry_profiles <- read.csv("data/country_characteristics/ctry_profiles.csv")
ctries <- ctry_profiles$iso2
ctries_sql <- paste0("'", ctries, "'", collapse = ", ")




##############################
# Population: Legal entities in UN constituent countries
##############################

# Path to parquet file
legal_info_path <- pdata("legal_info.parquet")

query_str <- sprintf(
  paste0(
    "SELECT ",
    "  SUBSTR(\"BvD ID number\", 1, 2) AS country_code, ",
    "  COUNT(*) AS n_obs ",
    "FROM '%s' ",
    "WHERE SUBSTR(\"BvD ID number\", 1, 2) IN (%s) ",
    "GROUP BY country_code ",
    "ORDER BY n_obs DESC"
  ),
  legal_info_path,
  ctries_sql
)

# Connect, query, and disconnect
con <- connect_duckdb(":memory:")
population <- dbGetQuery(con, query_str)
disconnect_duckdb(con)



##############################
# Filter 1: Entities active before analysis period
##############################


query_str <- sprintf(
  paste0(
    "SELECT ",
    "  SUBSTR(\"BvD ID number\", 1, 2) AS country_code, ",
    "  COUNT(*) AS n_obs ",
    "FROM '%s' ",
    "WHERE SUBSTR(\"BvD ID number\", 1, 2) IN (%s) ",
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
filter_1 <- dbGetQuery(con, query_str)
disconnect_duckdb(con)


##############################
# Filter 2: Entities founded after analysis period
##############################


query_str <- sprintf(
  paste0(
    "SELECT ",
    "  SUBSTR(\"BvD ID number\", 1, 2) AS country_code, ",
    "  COUNT(*) AS n_obs ",
    "FROM '%s' ",
    "WHERE SUBSTR(\"BvD ID number\", 1, 2) IN (%s) ",
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
filter_2 <- dbGetQuery(con, query_str)
disconnect_duckdb(con)




##############################
# Filter 3: Keep only TYpe == Corporate
##############################

query_str <- sprintf(
  paste0(
    "SELECT ",
    "  SUBSTR(\"BvD ID number\", 1, 2) AS country_code, ",
    "  COUNT(*) AS n_obs ",
    "FROM '%s' ",
    "WHERE SUBSTR(\"BvD ID number\", 1, 2) IN (%s) ",
    "  AND \"Type of entity\" IN ('Corporate') ",
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
filter_3 <- dbGetQuery(con, query_str)
disconnect_duckdb(con)


##############################
# Filter 4: Keep only limited liabiliy companies, partnerships & sole propritorships 
##############################

query_str <- sprintf(
  paste0(
    "SELECT ",
    "  SUBSTR(\"BvD ID number\", 1, 2) AS country_code, ",
    "  COUNT(*) AS n_obs ",
    "FROM '%s' ",
    "WHERE SUBSTR(\"BvD ID number\", 1, 2) IN (%s) ",
    "  AND \"Type of entity\" IN ('Corporate') ",
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
filter_4 <- dbGetQuery(con, query_str)
disconnect_duckdb(con)



# Investigate excluded legal forms
query_str_excluded <- sprintf(
  paste0(
    "SELECT ",
    "  COALESCE(\"Standardised legal form\", 'NA') AS legal_form, ",
    "  COUNT(*) AS n_obs ",
    "FROM '%s' ",
    "WHERE SUBSTR(\"BvD ID number\", 1, 2) IN (%s) ",
    "  AND \"Type of entity\" IN ('Corporate') ",
    "  AND (",
    "    \"Standardised legal form\" NOT IN ('Private limited companies', 'Public limited companies', 'Partnerships', 'Sole traders/proprietorships') ",
    "    OR \"Standardised legal form\" IS NULL ",
    "  ) ",
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
    "GROUP BY legal_form ",
    "ORDER BY n_obs DESC"
  ),
  legal_info_path,
  ctries_sql
)

# Connect, query, and disconnect
con <- connect_duckdb(":memory:")
excluded_legal_forms <- dbGetQuery(con, query_str_excluded)
disconnect_duckdb(con)

# Sanity check
sum(filter_4$n_obs) - sum(filter_3$n_obs) + sum(excluded_legal_forms$n_obs) == 0


##############################
# Make a table
##############################

sample_selection <- data.frame(step = c("Population", 
                                        "Inactive before period",
                                        "Founded after period",
                                        "Firm type 'Corporate'",
                                        "Legal forms LLC, Partnership, Sole Propritorship",
                                        "Sample"
),
obs = c(sum(population$n_obs), 
        sum(filter_1$n_obs)-sum(population$n_obs),
        sum(filter_2$n_obs)-sum(filter_1$n_obs),
        sum(filter_3$n_obs)-sum(filter_2$n_obs),
        sum(filter_4$n_obs)-sum(filter_3$n_obs),
        sum(filter_4$n_obs)
),
ctries = c(nrow(population),
           nrow(filter_1) - nrow(population),  
           nrow(filter_2) - nrow(filter_1),  
           nrow(filter_3) - nrow(filter_2), 
           nrow(filter_4) - nrow(filter_3),
           nrow(filter_4)
)
)


####################################
# Save
####################################
dir.create("output/summary_statistics", showWarnings = FALSE, recursive = TRUE)
write.csv(sample_selection, "output/summary_statistics/sample_selection.csv", row.names = FALSE)
