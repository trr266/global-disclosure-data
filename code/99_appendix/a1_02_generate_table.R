library(dplyr)


# Load data
ctry_profiles <- read.csv("data/country_characteristics/ctry_profiles.csv")
sample_orbis_versions <- read.csv("output/orbis_version_summary/orbis_version_summary.csv")

# Merge
ctry_profiles <- ctry_profiles %>% rename(ctry = iso2)
sample_orbis_versions <- left_join(sample_orbis_versions, ctry_profiles, by = "ctry")


# Get countries with high share of 2019 observations
sample_orbis_versions$share_2019 <- sample_orbis_versions$version_2019 / sample_orbis_versions$n




# Create summary by country
dt_sample <- sample_orbis_versions %>%
  group_by(continent) %>%
  summarise(
    `Statements` = sum(n, na.rm = TRUE),
    `source_2024` = sum(version_2024, na.rm = TRUE),
    `source_2019` = sum(version_2019, na.rm = TRUE),
    .groups = "drop"
  )

# Create total row
total_row <- data.frame(
  continent = "Total",
  `Statements` = sum(dt_sample$Statements, na.rm = TRUE),
  `source_2024` = sum(dt_sample$`source_2024`, na.rm = TRUE),
  `source_2019` = sum(dt_sample$`source_2019`, na.rm = TRUE)
)

# Append totals row to the summary
dt_sample <- bind_rows(dt_sample, total_row)

# Save
write.csv(dt_sample, "output/orbis_version_summary/sample_versions_by_continent.csv", row.names = FALSE)

