library(dplyr)
library(purrr)
library(tibble)

######################
#Load data
######################
dt_ctry_year <- read.csv("data/aggregated_data/dt_ctry_year.csv")
sum(dt_ctry_year$firms)

legal_forms <- dt_ctry_year  %>%
  group_by(legal_form) %>%
  summarise(n = sum(firms))

legal_forms$share <- legal_forms$n / sum(dt_ctry_year$firms)

######################
# Number of observations per year
######################

dt <- dt_ctry_year %>%
  group_by(year) %>%
  summarise(
    countries = length(unique(dt_ctry_year$ctry)),
    firms = sum(firms),
    statements = sum(n_fs),
    share = statements / firms, 
    .groups = "drop"
  ) %>%
  mutate(year = as.character(year)) %>%  
  bind_rows(
    tibble(
      year = "Total",
      countries = length(unique(dt_ctry_year$ctry)),
      firms = sum(.$firms),
      statements = sum(.$statements),
      share = statements / firms
    )
  )

# Save
write.csv(dt , "output/summary_statistics/obs_per_year.csv", row.names = FALSE)




######################
# Summary table
######################

# Prep data to summarize
smp <- dt_ctry_year %>% filter(year %in% 2021)

sum(smp$firms)

dt <- smp %>%
  group_by(ctry, year) %>%
  summarise(
    
    # Count total number of firms
    number_firms = sum(firms),
    share_fs = sum(n_fs) / sum(firms) ,
    items    = mean(fs_items, na.rm =T),
    
    # Count listed firms
    number_listed_regulated = sum(ifelse(legal_form %in% "Listed", firms, 0)),
    number_listed_other = sum(ifelse(legal_form %in% "Other Listed", firms, 0)),
    
    # Count firms by legal form
    number_limited      = sum(ifelse(legal_form == "Limited liability companies", firms, 0)),
    number_partnerships = sum(ifelse(legal_form == "Partnerships", firms, 0)),
    number_sole         = sum(ifelse(legal_form == "Sole traders/proprietorships", firms, 0))
    
    
  )




# Variables to summarize
vars_to_summarise <- c("number_firms",
                       "share_fs",  
                       "items",
                       "number_limited",      
                       "number_partnerships", 
                       "number_sole",   
                       "number_listed_regulated", 
                       "number_listed_other")

# Create summary table
summary_table <- map_dfr(vars_to_summarise, function(var) {
  vals <- dt[[var]]
  tibble(
    variable = var,
    n = sum(!is.na(vals)),
    mean = mean(vals, na.rm = TRUE),
    min = min(vals, na.rm = TRUE),
    p25 = quantile(vals, 0.25, na.rm = TRUE),
    median = median(vals, na.rm = TRUE),
    p75 = quantile(vals, 0.75, na.rm = TRUE),
    max = max(vals, na.rm = TRUE)
  )
})


# Save
write.csv(summary_table, "output/summary_statistics/summary_table.csv", row.names = FALSE)


sum(dt$number_listed_regulated > 0)
sum(dt$number_listed_other > 0)
sum(dt$number_limited > 0)
sum(dt$number_partnerships> 0)
sum(dt$number_sole > 0)

