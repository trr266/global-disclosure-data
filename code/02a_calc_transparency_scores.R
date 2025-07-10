# ------------------------------------------------------------------------------
# Calculate transparency scores similar to Kim & Olbert (2022) - https://marcelolbert.com/data-and-code/
# ------------------------------------------------------------------------------

suppressPackageStartupMessages(suppressWarnings({
  library(dplyr)
  library(duckdb)
  library(glue)
}))

source("code/config.R")


# Define some functions to access parquet files
pdata <- function(pfile) {
  file.path(PARQUET_FOLDER, pfile) 
} 

connect_duckdb <- function(dbase) {
  con <- dbConnect(duckdb::duckdb(), ":memory:")
  return(con)
}

disconnect_duckdb <- function(con){
  dbDisconnect(con, shutdown = TRUE)
}

financial_identifiers <- c(
  "BvD ID number",
  "Consolidation code",
  "Closing date",
  "Number of months"
)

# Categorize Orbis items following Kim & Olbert (2022)
bs_items <- c(
  "Fixed assets", 			#assets_fixed
  "Intangible fixed assets", 	#assets_fixed_intan
  "Tangible fixed assets",		#assets_fixed_tan
  "Other fixed assets",		#assets_fixed_oth
  "Current assets",			#assets_current		
  "Stock",				#assets_current_stock
  "Debtors",			#assets_current_debtors
  "Other current assets",		#assets_current_oth
  "Cash & cash equivalent",	#assets_cash
  "Total assets",			#assets_total
  "Non-current liabilities",		#liab_noncurrent
  "Long term debt",  		#liab_noncurrent_ltdebt
  "Other non-current liabilities",	#liab_noncurrent_oth
  "Current liabilities",		#liab_current
  "Loans",				#liab_current_loans
  "Creditors", 			#liab_current_creditors
  "Other current liabilities",  	#liab_current_oth
  "Provisions",			#liab_noncurrent_prov
  "Shareholders funds", 		#equity_funds
  "Capital",			#equity_capital
  "Other shareholders funds",         #equity_funds_oth            
  "Total shareh. funds & liab." 	# liab_equity_total
)

is_items <- c(
  "Operating revenue (Turnover)",  	#revenue
  "Costs of goods sold", 			#exp_goods
  "Gross profit",				#profit_gross
  "Other operating expenses",		#exp_operating_oth
  "Operating P/L [=EBIT]", 			#profit_ebit
  "Financial revenue", 			#revenue_fin
  "Financial expenses",			#exp_fin
  "P/L before tax", 			#profit_ebt
  "Taxation", 				#exp_tax
  "P/L after tax" 				#profit_ebiat
)

notes_items <- c(
  "Number of employees", 		#number_employees         
  "Export revenue", 			#revenue_export
  "Costs of employees",   			#exp_employees
  "Material costs", 			#exp_material
  "Depreciation & Amortization", 		#exp_depreciation
  "Interest paid",				#exp_interst
  "Research & Development expenses" 	#exp_rd
)

# Define function to calculate number of non missing items
count_not_na_sql <- function(varlist) {
  paste(
    sprintf('CAST ("%s" IS NOT NULL AS INTEGER)', varlist), 
    collapse = " + "
  )
}	

# Set uo directory
dir_path <- "data/transparency_scores"
if (!dir.exists(dir_path)) {
  dir.create(dir_path, recursive = TRUE)
}




# Start calculating the transparency scores
log_info("Calculating transparency scores at the firm report level")
con <- connect_duckdb()


# File paths
ORBIS_2024 <- "data/orbis_data/2024-12/industry_global_financials_and_ratios_usd.parquet"
ORBIS_2019 <- "data/orbis_data/2019-08/industry_global_financials_and_ratios_usd.parquet"


# Create a combined view with all three datasets
dbExecute(con, glue("
  CREATE OR REPLACE VIEW combined_financials AS
  SELECT *, '2024-12' AS orbis_version FROM read_parquet('{ORBIS_2024}')
  UNION ALL
  SELECT *, '2019-08' AS orbis_version FROM read_parquet('{ORBIS_2019}')
"))


transparency_data <- tbl(con, "combined_financials") %>%
    select(all_of(c(financial_identifiers, bs_items, is_items, notes_items, "orbis_version"))) %>%
    transmute(
      bvd_id = `BvD ID number`,
      fye = as.character(`Closing date`),
      conscode = `Consolidation code`,
      nr_month = `Number of months`, 
      bs_total = sql(count_not_na_sql(bs_items)),
      is_total = sql(count_not_na_sql(is_items)),
      notes_total = sql(count_not_na_sql(notes_items)),
      total_assets = `Total assets`,
      orbis_version = orbis_version
    )


rv <- DBI::dbExecute(
  con, paste0(
    "COPY (", dbplyr::sql_render(transparency_data), 
    ") TO 'data/transparency_scores/transparency_scores_raw.parquet' (FORMAT 'parquet');"
  ))

log_info("Stored as data/transparency_scores/transparency_scores_raw.parquet")
disconnect_duckdb(con)