# -----------------------------------------------------------------------------
# Title:    Orbis Dataset Downloader
# Purpose:  This script connects to the TRR 266 Orbis API to:
#             - Retrieve available dataset versions
#             - Download specific parquet files (financials and descriptives)
#               for a selected version of the Orbis dataset (by Moody's).
#
#           The data is downloaded each
#           year via the SFTP server provided by Moody's. 
#           The used files are called 
#           "Financials - Global format incl histo for industries June text" 
#           and "Descriptive data incl GIIN and LEI June text"
#           and is the Orbis Historical version, as described by 
#           Kalemli-Özcan et al. 2024.
#
# -----------------------------------------------------------------------------

# Load necessary packages
suppressMessages({
  library(jsonlite)
  library(httr)
  library(readr)
  library(utils)
  library(dotenv)
})

# Load API key from environment file
load_dot_env(file = "global-disclosure-data.env")
BVD_API_KEY <- Sys.getenv("BVD_API_KEY")
BVD_API_URL <- "https://trr266.wiwi.hu-berlin.de/bvd"

# Function to get available versions (optional, if needed)
versions <- fromJSON(paste0(BVD_API_URL, "/versions/", BVD_API_KEY))

# Function to get CSV file info (optional, if needed)
get_pfiles_info <- function(version) {
  url <- paste0(BVD_API_URL, "/info/", BVD_API_KEY, "/", version)
  return(read_csv(url))
}

# Function to download parquet files safely
download_pfiles <- function(version, folder, file, destfile) {
  # Ensure directory exists
  dir.create(dirname(destfile), recursive = TRUE, showWarnings = FALSE)
  
  # Construct URL
  url <- paste0(BVD_API_URL, "/data/", BVD_API_KEY, "/", version, "/", folder, "/", file)
  cat("\nDownloading:", version, folder, file, "\n")
  
  # Download file
  download.file(url, destfile = destfile, mode = "wb")
}

# Increase timeout window for larger files
options(timeout = 10000)

# -----------------------------------------------------------------------------
# Download 2024-12 version files into data/orbis_data/
# -----------------------------------------------------------------------------
download_pfiles("2024-12", "financials_global", "industry_global_financials_and_ratios_usd.parquet", "data/orbis_data/2024-12/industry_global_financials_and_ratios_usd.parquet")
download_pfiles("2024-12", "financials_global", "banks_global_financials_and_ratios_usd.parquet", "data/orbis_data/2024-12/banks_global_financials_and_ratios_usd.parquet")
download_pfiles("2024-12", "descriptives", "legal_info.parquet", "data/orbis_data/2024-12/legal_info.parquet")
download_pfiles("2024-12", "descriptives", "identifiers.parquet", "data/orbis_data/2024-12/identifiers.parquet")
download_pfiles("2024-12", "descriptives", "trade_description.parquet", "data/orbis_data/2024-12/trade_description.parquet")

# -----------------------------------------------------------------------------
# Download 2019-08 version files into data/orbis_data/
# -----------------------------------------------------------------------------
download_pfiles("2019-08", "financials_global", "industry_global_financials_and_ratios_usd.parquet", "data/orbis_data/2019-08/industry_global_financials_and_ratios_usd.parquet")
download_pfiles("2019-08", "financials_global", "banks_global_financials_and_ratios_usd.parquet", "data/orbis_data/2019-08/banks_global_financials_and_ratios_usd.parquet")
