
library(dplyr)
library(extrafont)
library(readxl)
library(ggplot2)
library(modelsummary)
loadfonts(device = "win")


# Load data
construct_validity <- read_excel("data/country_characteristics/disclosure_regulation/construct_validity.xlsx")
dt_ctry_year <- read.csv("data/aggregated_data/dt_ctry_year.csv")


# Prep Orbis data
dt <- dt_ctry_year %>%
  filter(year %in% 2021) %>%
  filter(legal_form %in% "Limited liability companies") %>%
  group_by(ctry) %>%
  summarize("firms" = sum(firms, na.rm =T),
            "n_fs" = sum(n_fs, na.rm =T)) %>%
  mutate("fs_share" = n_fs/firms)

dt <- right_join(dt, construct_validity, by = c("ctry"))  





dt$Group <- NA
dt$Group[dt$private_firm_disclosure == TRUE & dt$fs_disclosure_threshold == FALSE]  <- "Limited liability companies \n mandated to disclose"
dt$Group[dt$private_firm_disclosure == TRUE & dt$fs_disclosure_threshold == TRUE ]  <- "Limited liability companies  \n mandated to disclose if \n they exceed a threshold"
dt$Group[dt$private_firm_disclosure == FALSE]  <- "Only listed firms mandated \n to disclose"


dt$Group <- factor(
  dt$Group,
  levels = c(
    "Limited liability companies \n mandated to disclose",
    "Limited liability companies  \n mandated to disclose if \n they exceed a threshold",
    "Only listed firms mandated \n to disclose"
  )
)


text_size  <- 18
point_size <- 2.5  
line_size  <- 1.2  

construct_validity_extensive <- ggplot(dt, aes(x = factor(Group), y = fs_share, fill = factor(Group))) +
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
    y = "Share of Disclosing Firms \n (Extensive Margin)"
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
construct_validity_extensive
ggsave("output/construct_validity/construct_validity_extensive.png")




###########################################
# Regression
###########################################

m1 <- list(
  fixest::feols(fml = fs_share ~ private_firm_disclosure, data = dt),  
  fixest::feols(fml = fs_share ~ fs_disclosure_threshold, data = dt),  
  fixest::feols(fml = fs_share ~ private_firm_disclosure + fs_disclosure_threshold, data = dt)
)


modelsummary(
  models = m1,
  stars = TRUE,  # Add significance stars
  coef_map = c(
    "(Intercept)"                 = "(Intercept)",
    "private_firm_disclosureTRUE" = "Private Firm Disclosure",
    "fs_disclosure_thresholdTRUE" = "Disclosure Threshold"
  ),  # Use exact term names from your models
  gof_omit = "IC|RMSE",  # Remove unneeded goodness-of-fit stats
  title = "Dependent Variable: Share of Disclosing Limited Liability Companies"
)




