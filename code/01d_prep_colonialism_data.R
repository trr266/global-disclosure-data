
############################
# Load packages
############################
library(dplyr)
library(readr)

############################
# Load colonialism data
############################
colonialism_data <- read_csv("data/country_characteristics/colonialism_data/colonialism_data_raw.csv")

colnames(colonialism_data) <- c("ctry_name", "iso3","year","colonial_power")


############################
# Clean the data
############################
colonial_iso2 <- c(
  "Belgium" = "BE",
  "France" = "FR",
  "Germany" = "DE",
  "Italy" = "IT",
  "Netherlands" = "NL",
  "Portugal" = "PT",
  "Spain" = "ES",
  "United Kingdom" = "GB",
  "z. Multiple colonizers" = "Multiple",
  "zz. Colonizer" = "Colonizer",
  "zzz. Not colonized" = "Not colonized",
  "zzzz. No longer colonized" = "No longer colonized"
)

# Replace colonial power names with ISO2 codes
colonialism_data <- colonialism_data %>%
  mutate(colonial_power = colonial_iso2[colonial_power])

# Read in country profile data
ctry_profiles <- read_csv("data/country_characteristics/ctry_profiles.csv")

# Merge ISO2 code from country profiles
colonialism_data <- colonialism_data %>%
  left_join(ctry_profiles %>% select(iso3, iso2), by = "iso3") %>%
  mutate(
    iso2 = case_when(
      iso3 == "NAM" ~ "NA",
      iso3 == "TWN" ~ "TW",
      TRUE ~ iso2
    ))

# Select and rename final columns
colonialism_data <- colonialism_data %>%
  transmute(ctry = iso2, year, colonial_power)

############################
# Summarize data
############################

# Filter out non-colonized or multiple cases
dt <- colonialism_data %>%
  filter(!(colonial_power %in% c("Not colonized", "Colonizer", "No longer colonized", "Multiple")))

# Count years each colonial power ruled each country
colonialism_summary <- dt %>%
  group_by(ctry, colonial_power) %>%
  summarise(
    years_colonized = n(),
    start_year = min(year),
    end_year = max(year),
    .groups = "drop"
  )


############################
# Save
############################
write.csv(colonialism_data, "data/country_characteristics/colonialism_data/colonialism_data.csv", row.names = FALSE)
write.csv(colonialism_summary, "data/country_characteristics/colonialism_data/colonialism_summary.csv", row.names = FALSE)