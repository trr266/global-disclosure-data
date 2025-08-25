
library(dplyr)
library(extrafont)
library(readxl)
library(ggplot2)
library(modelsummary)
loadfonts(device = "win")


# Load data
construct_validity <- read_excel("data/country_characteristics/disclosure_regulation/construct_validity.xlsx")
dt_ctry_year <- read.csv("data/aggregated_data/dt_ctry_year.csv")
dt_ctry_year$ctry[is.na(dt_ctry_year$ctry)] <- "NA"


# Prep Orbis data
dt <- dt_ctry_year %>%
  filter(year %in% 2021) %>%
  filter(legal_form %in% "Limited liability companies") %>%
  group_by(ctry) %>%
  summarize("firms" = sum(firms, na.rm =T),
            "n_fs" = sum(n_fs, na.rm =T)) %>%
  mutate("fs_share" = n_fs/firms)

dt <- right_join(dt, construct_validity, by = c("ctry"))  


dt$Group <- NA_character_
dt$Group[dt$private_firm_disclosure & !dt$fs_disclosure_threshold] <- "Private Mandated"
dt$Group[dt$private_firm_disclosure &  dt$fs_disclosure_threshold] <- "Private Threshold"
dt$Group[!dt$private_firm_disclosure]                               <- "Private No Mandate"

dt$Group <- factor(
  dt$Group,
  levels = c(
    "Private Mandated",
    "Private Threshold",
    "Private No Mandate"
  )
)


text_size  <- 12
point_size <- 2.5  
line_size  <- 1.2  

disclosure_rate_oecd_boxplot <- ggplot(dt, aes(x = factor(Group), y = fs_share, fill = factor(Group))) +
  geom_jitter(
    position = position_jitter(width = 0.2, height = 0),
    aes(color = factor(Group)),
    alpha = 0.7,
    size = point_size
  ) +
  geom_boxplot(
    alpha = 0.5,
    show.legend = FALSE,
    outlier.shape = NA
  ) +
  labs(
    x = element_blank(),
    y = "Disclosure Rate"
  ) +
  scale_fill_manual(values = c("#934664", "#14676B", "#EC9007")) +  # Custom colors for fill
  scale_color_manual(values = c("#934664", "#14676B", "#EC9007")) + # Custom colors for jitter
  theme_minimal(base_family = "Times New Roman") +
  theme(
    axis.title.x = element_blank(),  # Remove x-axis title
    axis.title.y = element_text(size = text_size, family = "Times New Roman"),
    axis.text.x = element_text(size = text_size, angle = 45, hjust = 1, family = "Times New Roman"),  # Adjust x-axis labels
    axis.text.y = element_text(size = text_size, family = "Times New Roman"),  # Adjust y-axis labels
    plot.title = element_text(size = 1.2 * text_size, face = "bold", family = "Times New Roman", hjust = 0.5),
    legend.position = "none",  # Remove legend position
    legend.title = element_blank(),
    legend.text = element_blank(),
    panel.background = element_rect(fill = "white", color = NA)  # Ensure a clean white background
  )
disclosure_rate_oecd_boxplot 
ggsave("output/construct_validity/disclosure_rate_oecd_boxplot.png", width = 6, height = 4, dpi = 300)


###########################################
# Get worldbank data
###########################################
worldbank_data <- read.csv("data/country_characteristics/worldbank_data/worldbank_data.csv")

# Summarize world bank data
dt_worldbank <- worldbank_data %>%
  filter(year %in% c(2015:2021)) %>%
  group_by(ctry) %>%
  summarise(rule_of_law = mean(rule_law_est, na.rm =T),
            gdp_per_cap_log = log(mean(gdp_per_cap, na.rm =T)),
            mkt_cap_gdp = mean(mkt_cap_gdp, na.rm =T))


# Merge
worldbank_data$ctry[is.na(worldbank_data$ctry)] <- "NA"
dt <- left_join(dt, dt_worldbank, by = "ctry")


###########################################
# Regression
###########################################

m1 <- list(
  fixest::feols(
    fs_share ~ i(Group, ref = "Private No Mandate"),
    data = dt
  ),
  fixest::feols(
    fs_share ~ rule_of_law,
    data = dt[dt$private_firm_disclosure & !dt$fs_disclosure_threshold, ]
  )
)

modelsummary(
  models = m1,
  stars = TRUE
)


modelsummary(
  models = m1,
  stars = TRUE,
  coef_map = c(
    "(Intercept)"                  = "(Intercept)",
    "Group::Private Mandated"     = "Private Mandated",
    "Group::Private Threshold"  = "Private Threshold",
    "rule_of_law"                  = "Rule of Law"
  ),
  gof_omit = "IC|RMSE",
  title = "Dependent Variable: Disclosure Rate LLCs"
)




###########################################
# Make a scatter
###########################################

text_size  <- 12
point_size <- 2.5  
line_size  <- 1.2  

rule_of_law_disclosure_scatter <- ggplot(
  data = dt[dt$private_firm_disclosure & !dt$fs_disclosure_threshold, ],
  aes(x =  rule_of_law, y = fs_share)
) +
  geom_point(color = "#14676B", alpha = 0.7, size = point_size) +
  geom_smooth(
    method = "lm",
    se = FALSE,
    size = line_size,
    color = "#EC9007"
  ) +

  labs(
    x = "Rule of Law",
    y = "Disclosure Rate (Private Mandated)",
    title = NULL
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    axis.text  = element_text(size = text_size, family = "Times New Roman"),
    axis.title = element_text(size = text_size + 2, family = "Times New Roman"),
    plot.title = element_text(size = text_size + 4, family = "Times New Roman", face = "bold")
  )
ggsave("output/construct_validity/rule_of_law_disclosure_scatter.png", width = 6, height = 4, dpi = 300)

cor(dt$fs_share[dt$private_firm_disclosure & !dt$fs_disclosure_threshold], dt$rule_of_law[dt$private_firm_disclosure & !dt$fs_disclosure_threshold] )


###########################################
# Correlations with Rule of law
###########################################

dt <- dt_ctry_year %>%
  filter(year %in% 2021) %>%
  filter(legal_form %in% "Limited liability companies") %>%
  group_by(ctry) %>%
  summarize("firms" = sum(firms, na.rm =T),
            "n_fs" = sum(n_fs, na.rm =T)) %>%
  mutate("fs_share" = n_fs/firms)

# Load and summarize world bank data
worldbank_data <- read.csv("data/country_characteristics/worldbank_data/worldbank_data.csv")
worldbank_data$ctry[is.na(worldbank_data$ctry)] <- "NA"

dt_worldbank <- worldbank_data %>%
  filter(year %in% c(2015:2021)) %>%
  group_by(ctry) %>%
  summarise(rule_of_law = mean(rule_law_est, na.rm =T),
            gdp_per_cap_log = log(mean(gdp_per_cap, na.rm =T)),
            mkt_cap_gdp = mean(mkt_cap_gdp, na.rm =T))

dt <- left_join(dt, dt_worldbank, by = "ctry")
cor(dt$fs_share, dt$rule_of_law)




mean(dt$rule_of_law)
