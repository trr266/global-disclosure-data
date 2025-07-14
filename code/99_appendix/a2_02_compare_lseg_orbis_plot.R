

library(ggplot2)
library(extrafont)
loadfonts(device = "win")

# Load data
dt <- readRDS("output/orbis_lseg_comparison/orbis_lseg_comparison.rds")

# check availability
1-sum(is.na(dt$total_assets))/nrow(dt)
1-sum(is.na(dt$lseg_toas))/nrow(dt)
1-sum(is.na(dt$total_assets) | is.na(dt$lseg_toas))/nrow(dt)

dt <- dt %>%
  filter(!is.na(total_assets) & !is.na(lseg_toas))

###################################
# Plot comparison of total assets
###################################
text_size  <- 18
point_size <- 2
line_size  <- 1

assets_comparison_scatter <- ggplot(dt, aes(x = log(total_assets), y = log(lseg_toas))) +
  geom_point(color = "#14676B", alpha = 0.7, size = point_size) +
  geom_abline(intercept = 0, slope = 1, linetype = "dashed", size = line_size, color = "#EC9007") +
  labs(
    x = "Log(Total Assets) - Orbis",
    y = "Log(Total Assets) - LSEG",
    title = element_blank()
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    axis.text  = element_text(size = text_size, family = "Times New Roman"),
    axis.title = element_text(size = text_size, family = "Times New Roman")
  )
assets_comparison_scatter
ggsave("output/orbis_lseg_comparison/assets_comparison_scatter.png")

# Calculate deviation
dt$deviation <- abs(dt$total_assets - dt$lseg_toas) / dt$total_assets
dt$deviation[dt$total_assets == 0 & dt$lseg_toas == 0] <- 0
dt$large_deviation <- dt$deviation > 0.05

# Share of observations with a large deviation
sum(dt$large_deviation) /nrow(dt)
1-sum(dt$large_deviation) /nrow(dt)

###################################
# Median deviation by country
###################################

deviation_by_country <- dt %>%
  group_by(ctry) %>%
  summarise(median_deviation = median(deviation))



# Get GDP
worldbank_data <- read.csv("data/country_characteristics/worldbank_data/worldbank_data.csv")
gdp <- worldbank_data  %>%
  filter(year %in% 2019) %>%
  select(ctry, gdp_per_cap)
deviation_by_country <- left_join(deviation_by_country, gdp, by = "ctry")

# Take most recent available GDP data for Venezuela
deviation_by_country$gdp_per_cap[deviation_by_country$ctry %in% "VE"] <- 15943.613

text_size  <- 18
point_size <- 2
line_size  <- 1


deviation_scatter <- ggplot(deviation_by_country, aes(x = gdp_per_cap, y = median_deviation)) +
  geom_point(color = "#14676B", alpha = 0.7, size = point_size) +
  geom_smooth(method = "lm", color = "#EC9007", se = FALSE, size = line_size) +
  labs(
    x = "GDP per Capita",
    y = "Median Deviation by Country",
    title = element_blank()
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    axis.text  = element_text(size = text_size, family = "Times New Roman"),
    axis.title = element_text(size = text_size, family = "Times New Roman")
  )
deviation_scatter 
ggsave("output/orbis_lseg_comparison/deviation_scatter.png")
