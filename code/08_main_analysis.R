######################
# Load libraries and data
######################

library(dplyr)
library(fixest)
library(modelsummary)
library(countrycode)
library(ggplot2)
library(extrafont)
loadfonts(device = "win")


dt_ctry_year <- read.csv("data/aggregated_data/dt_ctry_year.csv")
legal_origin_data <- read.csv("data/country_characteristics/la_porta_data/legal_origin_data.csv")
worldbank_data <- read.csv("data/country_characteristics/worldbank_data/worldbank_data.csv")

# Take care of Namibia
dt_ctry_year$ctry[is.na(dt_ctry_year$ctry)] <- "NA"
legal_origin_data$ctry[is.na(legal_origin_data$ctry)] <- "NA"
worldbank_data$ctry[is.na(worldbank_data$ctry)] <- "NA"



######################
# Select sample
######################
# Get all firms
dt_all <- dt_ctry_year %>% 
  filter(!ctry %in% c("CD", "ME", "RS", "SS", "ER", "VE", "GD", "FJ")) %>%
  mutate(sum_items = n_fs * fs_items) %>%
  group_by(ctry, year) %>%
  summarise(total_firms = sum(firms),
            total_statements = sum(n_fs),
            total_items = sum(sum_items, na.rm = TRUE),
            disc_rate = total_statements/total_firms,
            disc_int = total_items / total_statements) %>%
  ungroup()


# Get all private firms 
dt_private <- dt_ctry_year %>% 
  filter(legal_form %in% c("Partnerships","Limited liability companies","Sole traders/proprietorships" )) %>%
  filter(!ctry %in% c("CD", "ME", "RS", "SS", "ER", "VE", "GD", "FJ")) %>%
  mutate(sum_items = n_fs * fs_items) %>%
  group_by(ctry, year) %>%
  summarise(total_firms = sum(firms),
            total_statements = sum(n_fs),
            total_items = sum(sum_items, na.rm = TRUE),
            disc_rate = total_statements/total_firms,
            disc_int = total_items / total_statements) %>%
  ungroup()


# Get LLCs 
dt_llc <- dt_ctry_year %>% 
  filter(legal_form %in% c("Limited liability companies")) %>%
  filter(!ctry %in% c("CD", "ME", "RS", "SS", "ER", "VE", "GD", "FJ")) %>%
  mutate(sum_items = n_fs * fs_items) %>%
  group_by(ctry, year) %>%
  summarise(total_firms = sum(firms),
            total_statements = sum(n_fs),
            total_items = sum(sum_items, na.rm = TRUE),
            disc_rate = total_statements/total_firms,
            disc_int = total_items / total_statements) %>%
  ungroup()



######################
# Prep Legal Origin
######################

# Add a variable indicating legal origin
legal_origin_data <- legal_origin_data %>%
  mutate(
    legal_origin = case_when(
      legor_fr == 1 ~ "civil_fra",
      legor_ge == 1 ~ "civil_ger",
      legor_sc == 1 ~ "civil_scan",
      legor_so == 1 ~ "socialist",
      legor_uk == 1 ~ "common",
      TRUE ~ NA_character_
    ),
    civil = as.numeric(legor_fr == 1 | legor_ge == 1 | legor_sc == 1) 
  ) %>%
  rename(
    civil_fra  = legor_fr,
    civil_ger  = legor_ge,
    civil_scan = legor_sc,
    socialist  = legor_so,
    common  = legor_uk
  )


######################
# Prep World Bank Data
######################

# Summarize world bank data
dt_worldbank <- worldbank_data %>%
  filter(year %in% c(2015:2021)) %>%
  group_by(ctry) %>%
  summarise(rule_of_law   = mean(rule_law_est, na.rm =T),
            gdp_per_cap   = (mean(gdp_per_cap, na.rm =T))/1000,
            mcap_gdp      = mean(mkt_cap_gdp, na.rm =T)/100,
            domcredit_gdp = mean(dom_cred, na.rm =T)/100)


# Count sample countries with missing data
missing_counts <- dt_llc %>%
  filter(year == 2021) %>%
  distinct(ctry) %>%                     
  left_join(dt_worldbank, by = "ctry") %>%
  summarise(
    n_countries           = n(),
    missing_rule_of_law   = sum(is.na(rule_of_law)),
    missing_gdp_per_cap   = sum(is.na(gdp_per_cap)),
    missing_mcap_gdp      = sum(is.na(mcap_gdp)),
    missing_domcredit_gdp = sum(is.na(domcredit_gdp))
  )

# Set missing to zero
dt_worldbank$mcap_gdp[is.na(dt_worldbank$mcap_gdp)] <- 0
dt_worldbank$domcredit_gdp[is.na(dt_worldbank$domcredit_gdp)] <- 0


######################
# Merge
######################
dt_all <- dt_all %>%
  left_join(legal_origin_data, by = "ctry") %>%
  left_join(dt_worldbank, by = "ctry")

dt_private <- dt_private %>%
  left_join(legal_origin_data, by = "ctry") %>%
  left_join(dt_worldbank, by = "ctry") 

dt_llc <- dt_llc %>%
  left_join(legal_origin_data, by = "ctry") %>%
  left_join(dt_worldbank, by = "ctry") 

# Set year 2021 as year of analysis 
smp_all <- dt_all %>% filter(year %in% 2021)
smp_llc <- dt_llc %>% filter(year %in% 2021)
smp_private <- dt_private %>% filter(year %in% 2021)



######################
# World Map - Legal origin
######################


world_map <- map_data("world") %>%
  mutate(ctry= countrycode(region, "country.name", "iso2c")) %>%
  left_join(smp_all, by = "ctry") 

world_map$legal_origin[is.na(world_map$legal_origin)] <- "Not in Sample"
  


world_map <-  world_map %>% 
  mutate(
  legal_origin = recode(legal_origin,
                          "civil_fra"  = "Civil Law: French",
                          "civil_ger"  = "Civil Law: German",
                          "civil_scan" = "Civil Law: Scandinavian",
                          "socialist"  = "Socialist Law",
                          "common"  = "Common Law"
    ),
    legal_origin = factor(
      legal_origin,
      levels = c("Common Law", "Civil Law: French", "Civil Law: German", "Civil Law: Scandinavian", "Socialist Law", "Not in Sample")
    )
  )






text_size <- 10

legal_origin_map <- ggplot(world_map, aes(x = long, y = lat, group = group, fill = legal_origin)) +
  geom_polygon(color = NA) +
  labs(
    title = "Legal Origin of Sample Countries"
  ) +
  scale_fill_manual(
    values = c(
      "Common Law"              = "#EC9007",  
      "Civil Law: French"       = "#14676B", 
      "Civil Law: German"       = "#6ecbe2",  
      "Civil Law: Scandinavian" = "#3d9ca4", 
      "Socialist Law"           = "#934664"   
    )
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    axis.text = element_blank(),
    axis.title = element_blank(),
    axis.ticks = element_blank(),
    panel.grid = element_blank(),
    plot.title = element_text(size = text_size + 2, family = "Times New Roman", hjust = 0.5),
    legend.text = element_text(size = text_size, family = "Times New Roman"),
    legend.title = element_blank(),
    panel.background = element_rect(fill = "white", color = NA)
  ) +
  coord_fixed(1.3) +
  coord_cartesian(ylim = c(-50, 90))+ theme(
    legend.position = "bottom",
    legend.box = "horizontal"
  ) +
  guides(fill = guide_legend(nrow = 1, byrow = TRUE))


legal_origin_map <- legal_origin_map +
  theme(
    legend.position = "bottom",
    legend.box = "horizontal",
    legend.justification = "center",
    # add extra bottom space for the legend
    plot.margin = margin(10, 10, 60, 10, "pt")
  ) +
  # wrap legend so it fits the width
  guides(fill = guide_legend(nrow = 2, byrow = TRUE))


ggsave("output/main_analysis/legal_origin_map.png",
       plot = legal_origin_map,
       width = 6, height = 5.1, units = "in", dpi = 300)




######################
# Get legal origin of all countries in a table
######################

summary_legal_origin <- smp_private %>% select(ctry, legal_origin)

#Save 
dir.create("output/main_analysis", showWarnings = FALSE, recursive = TRUE)
write.csv(summary_legal_origin, "output/main_analysis/summary_legal_origin.csv", row.names = FALSE)


######################
# Statistics at country level by legal origin (year 2021)
######################


# Helper to make summary
summary_stats <- function(df, var) {
  df %>%
    summarise(
      country = n(),  # number of countries with data
      mean    = mean(.data[[var]], na.rm = TRUE),
      sd      = sd(.data[[var]], na.rm = TRUE),
      min     = min(.data[[var]], na.rm = TRUE),
      p25     = quantile(.data[[var]], 0.25, na.rm = TRUE),
      median  = median(.data[[var]], na.rm = TRUE),
      p75     = quantile(.data[[var]], 0.75, na.rm = TRUE),
      max     = max(.data[[var]], na.rm = TRUE)
    )
}

# Helper to build panel tables
make_panel <- function(df, var_name) {
  by_lo <- df %>%
    group_by(legal_origin) %>%
    summary_stats(var_name) %>%
    ungroup()
  
  civil <- df %>%
    filter(legal_origin %in% c("civil_fra", "civil_ger", "civil_scan")) %>%
    summary_stats(var_name) %>%
    mutate(legal_origin = "civil_total")
  
  total <- df %>%
    summary_stats(var_name) %>%
    mutate(legal_origin = "Total")
  
  bind_rows(by_lo, civil, total) %>%
    relocate(legal_origin)
}

# Generate summary stats
panel_disc_rate_private <- make_panel(filter(smp_private, !is.na(disc_rate)), "disc_rate")
panel_disc_rate_llc     <- make_panel(filter(smp_llc, !is.na(disc_rate)), "disc_rate")
panel_disc_int_private  <- make_panel(filter(smp_private, !is.na(disc_int)), "disc_int")
panel_disc_int_llc      <- make_panel(filter(smp_llc, !is.na(disc_int)), "disc_int")
panel_rule_of_law       <- make_panel(filter(smp_private, !is.na(rule_of_law)), "rule_of_law")
panel_mcap_gdp          <- make_panel(filter(smp_private, !is.na(mcap_gdp)), "mcap_gdp")
panel_domcredit_gdp     <- make_panel(filter(smp_private, !is.na(domcredit_gdp)), "domcredit_gdp")
panel_gdp_per_cap       <- make_panel(filter(smp_private, !is.na(gdp_per_cap)), "gdp_per_cap")

# Save
write.csv(panel_disc_rate_private, "output/main_analysis/panel_disc_rate_private.csv", row.names = FALSE)
write.csv(panel_disc_rate_llc, "output/main_analysis/panel_disc_rate_llc.csv", row.names = FALSE)
write.csv(panel_disc_int_private, "output/main_analysis/panel_disc_int_private.csv", row.names = FALSE)
write.csv(panel_disc_int_llc, "output/main_analysis/panel_disc_int_llc.csv", row.names = FALSE)
write.csv(panel_rule_of_law, "output/main_analysis/panel_rule_of_law.csv", row.names = FALSE)
write.csv(panel_mcap_gdp, "output/main_analysis/panel_mcap_gdp.csv", row.names = FALSE)
write.csv(panel_domcredit_gdp, "output/main_analysis/panel_domcredit_gdp.csv", row.names = FALSE)
write.csv(panel_gdp_per_cap, "output/main_analysis/panel_gdp_per_cap.csv", row.names = FALSE)


######################
# Correlation table
######################

df <- smp_llc %>%
  select(ctry, disc_rate, disc_int) %>%
  rename(disc_rate_llc = disc_rate,
         disc_int_llc = disc_int) %>%
  right_join(smp_private, by = "ctry") %>%
  rename(disc_rate_private = disc_rate,
         disc_int_private = disc_int,) %>%
  select(disc_rate_private, disc_rate_llc,
         common, civil, socialist,
        mcap_gdp,domcredit_gdp,rule_of_law, gdp_per_cap)



pearson  <- cor(df, use = "pairwise.complete.obs", method = "pearson")
spearman <- cor(df, use = "pairwise.complete.obs", method = "spearman")

cor <- pearson
cor[upper.tri(cor)] <- spearman[upper.tri(spearman)]
diag(cor) <- 1

write.csv(cor, "output/main_analysis/correlation.csv", row.names = FALSE)



######################
# Regressions (Disclosure Rate)
######################

# Define EU members for sub analysis
eu <- c("AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR",
             "DE","GR","HU","IE","IT","LV","LT","LU","MT","NL",
             "PL","PT","RO","SK","SI","ES","SE")


coef_order <- c(
  "common",
  "civil",
  "socialist",
  "mcap_gdp",
  "domcredit_gdp",
  "rule_of_law",
  "gdp_per_cap",
  "(Intercept)"  
)



# All private
disc_rate_private <- list(
  # (1) 
  feols(disc_rate ~ common + rule_of_law  + gdp_per_cap, 
        data = smp_private),
  # (2) 
  feols(disc_rate ~ civil + socialist +  rule_of_law  + gdp_per_cap,
        data = smp_private),
  # (3) 
  feols(disc_rate ~ mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_private),
  # (4) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_private),
  # (5) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_private[smp_private$total_firms > 25000,]),
  # (6) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_private[!smp_private$ctry %in% eu,])
)

modelsummary(
  disc_rate_private,
  stars = TRUE,
  coef_map = coef_order
)



# LLCs 
disc_rate_llc <- list(
  # (1) 
  feols(disc_rate ~ common +  rule_of_law  + gdp_per_cap, 
        data = smp_llc),
  # (2) 
  feols(disc_rate ~ civil + socialist +  rule_of_law  + gdp_per_cap,
        data = smp_llc),
  # (3) 
  feols(disc_rate ~ mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_llc),
  # (4) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_llc),
  # (5) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_llc[smp_llc$total_firms > 25000,]),
  # (6) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_llc[!smp_llc$ctry %in% eu,])
)

modelsummary(
  disc_rate_llc,
  stars = TRUE,
  coef_map = coef_order
)


######################
# Regressions (Other years)
######################

# All private
disc_rate_private_years <- list(
  # (1) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_private[dt_private$year %in% 2015,]),
  # (2) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_private[dt_private$year %in% 2016,]),
  # (3) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_private[dt_private$year %in% 2017,]),
  # (4) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_private[dt_private$year %in% 2018,]),
  # (5) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_private[dt_private$year %in% 2019,]),
  # (6) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_private[dt_private$year %in% 2020,]),
  # (6) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_private[dt_private$year %in% 2021,])
)


modelsummary(
  disc_rate_private_years,
  stars = TRUE,
  coef_map = coef_order
)



# All LLCs
disc_rate_llc_years <- list(
  # (1) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_llc[dt_llc$year %in% 2015,]),
  # (2) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_llc[dt_llc$year %in% 2016,]),
  # (3) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_llc[dt_llc$year %in% 2017,]),
  # (4) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_llc[dt_llc$year %in% 2018,]),
  # (5) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_llc[dt_llc$year %in% 2019,]),
  # (6) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_llc[dt_llc$year %in% 2020,]),
  # (6) 
  feols(disc_rate ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = dt_llc[dt_llc$year %in% 2021,])
)

modelsummary(
  disc_rate_llc_years,
  stars = TRUE,
  coef_map = coef_order
)




######################
# Regressions (Disclosure Intensity)
######################

# All private
smp_disc_int_private <- smp_private %>% filter(!is.na(disc_int))

disc_int_private <- list(
  # (1) 
  feols(disc_int ~ common + rule_of_law  + gdp_per_cap, 
        data = smp_disc_int_private),
  # (2) 
  feols(disc_int ~ civil + socialist +  rule_of_law  + gdp_per_cap,
        data = smp_disc_int_private),
  # (3) 
  feols(disc_int ~ mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_disc_int_private),
  # (4) 
  feols(disc_int ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_disc_int_private),
  # (5) 
  feols(disc_int ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_disc_int_private[smp_disc_int_private$total_firms > 25000,]),
  # (6) 
  feols(disc_int ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_disc_int_private[!smp_disc_int_private$ctry %in% eu,])
)

modelsummary(
  disc_int_private,
  stars = TRUE,
  coef_map = coef_order
)


# LLCs 
smp_disc_int_llc <- smp_llc %>% filter(!is.na(disc_int))

disc_int_llc <- list(
  # (1) 
  feols(disc_int ~ common + rule_of_law  + gdp_per_cap, 
        data = smp_disc_int_llc),
  # (2) 
  feols(disc_int ~ civil + socialist +  rule_of_law  + gdp_per_cap,
        data = smp_disc_int_llc),
  # (3) 
  feols(disc_int ~ mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_disc_int_llc),
  # (4) 
  feols(disc_int ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_disc_int_llc),
  # (5) 
  feols(disc_int ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_disc_int_llc[smp_disc_int_llc$total_firms > 25000,]),
  # (6) 
  feols(disc_int ~ civil + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_disc_int_llc[!smp_disc_int_llc$ctry %in% eu,])
)

modelsummary(
  disc_int_llc,
  stars = TRUE,
  coef_map = coef_order
)






######################
# Appendix
######################

coef_order <- c(
  "common",
  "civil_fra",
  "civil_ger",
  "civil_scan",
  "socialist",
  "mcap_gdp",
  "domcredit_gdp",
  "rule_of_law",
  "gdp_per_cap",
  "(Intercept)"  
)



# All private
disc_rate_private_granular <- list(
  # (1) 
  feols(disc_rate ~ common + rule_of_law  + gdp_per_cap, 
        data = smp_private),
  # (2) 
  feols(disc_rate ~ civil_fra + civil_ger + civil_scan + socialist +  rule_of_law  + gdp_per_cap,
        data = smp_private),
  # (3) 
  feols(disc_rate ~ mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_private),
  # (4) 
  feols(disc_rate ~ civil_fra + civil_ger + civil_scan + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_private),
  # (5) 
  feols(disc_rate ~ civil_fra + civil_ger + civil_scan + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_private[smp_private$total_firms > 25000,]),
  # (6) 
  feols(disc_rate ~ civil_fra + civil_ger + civil_scan + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_private[!smp_private$ctry %in% eu,])
)

modelsummary(
  disc_rate_private_granular,
  stars = TRUE,
  coef_map = coef_order
)



# LLCs 
disc_rate_llc_granular <- list(
  # (1) 
  feols(disc_rate ~ common +  rule_of_law  + gdp_per_cap, 
        data = smp_llc),
  # (2) 
  feols(disc_rate ~ civil_fra + civil_ger + civil_scan + socialist +  rule_of_law  + gdp_per_cap,
        data = smp_llc),
  # (3) 
  feols(disc_rate ~ mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_llc),
  # (4) 
  feols(disc_rate ~ civil_fra + civil_ger + civil_scan + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_llc),
  # (5) 
  feols(disc_rate ~ civil_fra + civil_ger + civil_scan + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_llc[smp_llc$total_firms > 25000,]),
  # (6) 
  feols(disc_rate ~ civil_fra + civil_ger + civil_scan + socialist + mcap_gdp + domcredit_gdp + rule_of_law  + gdp_per_cap,
        data = smp_llc[!smp_llc$ctry %in% eu,])
)

modelsummary(
  disc_rate_llc_granular,
  stars = TRUE,
  coef_map = coef_order
)






