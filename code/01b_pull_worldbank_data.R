

# ------------------------------------------------------------------------------
# Pull Worldbank data for country-level indicators
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(dplyr)
  library(wbstats)
})

# --- Download data ------------------------------------------------------------

dload_wbank_data <- function(
    vars = c(# Development indicators
             "SP.POP.TOTL", 
             "NY.GDP.MKTP.CD", 
             "NY.GDP.PCAP.CD",
             "CM.MKT.LDOM.NO",
             "CM.MKT.LCAP.GD.ZS",
             "BX.KLT.DINV.WD.GD.ZS",
             "FS.AST.PRVT.GD.ZS",
             
             # Governance Indicators
             "VA.EST",
             "VA.PER.RNK",
             "PV.EST",
             "PV.PER.RNK",
             "GE.EST",
             "GE.PER.RNK", 
             "RQ.EST",
             "RQ.PER.RNK",
             "RL.EST",
             "RL.PER.RNK",
             "CC.EST",
             "CC.PER.RNK"
             ),
    
    labels = c(#Development indicators
               "population",
               "gdp_current_usd",
               "gdp_per_cap",
               "listed_domestic_comps",
               "mkt_cap_gdp",
               "foreign_invest_gdp",
               "dom_cred",
               
               # Governance Indicators
               "voice_account_est",
               "voice_account_rank",
               "pol_stab_est",
               "pol_stab_rank",
               "gov_eff_est",
               "gov_eff_rank",
               "reg_qual_est",
               "reg_qual_rank",
               "rule_law_est",
               "rule_law_rank",
               "control_cor_est",
               "control_cor_rank"
               )) 
{
  wb_data(
    indicator = vars, start_date = 2015, end_date = 2021, return_wide = TRUE
  ) %>%
    mutate(year = as.numeric(date)) %>%
    rename_at(all_of(vars), ~labels) %>%
    select(iso2c, year, all_of(labels))
}

worldbank_data <- dload_wbank_data() 


# --- Clean and save -----------------------------------------------------------

colnames(worldbank_data) <- c("ctry",
                              "year",
                              #Development indicators
                              "population",
                              "gdp_current_usd",
                              "gdp_per_cap",
                              "listed_domestic_comps",
                              "mkt_cap_gdp",
                              "foreign_invest_gdp",
                              "dom_cred",
                              
                              # Governance Indicators
                              "voice_account_est",
                              "voice_account_rank",
                              "pol_stab_est",
                              "pol_stab_rank",
                              "gov_eff_est",
                              "gov_eff_rank",
                              "reg_qual_est",
                              "reg_qual_rank",
                              "rule_law_est",
                              "rule_law_rank",
                              "control_cor_est",
                              "control_cor_rank")



dir.create("data/country_characteristics/worldbank_data", showWarnings = FALSE, recursive = TRUE)
write.csv(worldbank_data, "data/country_characteristics/worldbank_data/worldbank_data.csv", row.names = FALSE)
