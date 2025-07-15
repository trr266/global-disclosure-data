library(dplyr)
library(patchwork)
library(extrafont)
loadfonts(device = "win")

######################
# Load data
######################
dt_ctry_year <- read.csv("data/aggregated_data/dt_ctry_year.csv")


# Config plot
text_size  <- 14
point_size <- 2
line_size  <- 1

######################
# Extensive margin by legal form
######################

# Aggregate data
dt <- dt_ctry_year %>%
  filter(year == 2021) %>%
  group_by(ctry, legal_form) %>%
  summarise(
    firms = sum(firms),
    statements = sum(n_fs),
    fs_share = statements / firms,
    .groups = "drop"
  )

# Rename "Listed" to "Regulated Domestic Listed"
dt$legal_form <- recode(
  dt$legal_form,
  "Listed" = "Regulated Domestic Listed"
)

# Set factor levels for correct ordering
dt$legal_form <- factor(dt$legal_form, levels = c(
  "Regulated Domestic Listed",
  "Other Listed",
  "Limited liability companies",
  "Partnerships",
  "Sole traders/proprietorships"
))

# Define colors for all 5 groups
custom_colors <- c(
  "Regulated Domestic Listed"     = "#934664", # Renamed here
  "Other Listed"                  = "#14676B",
  "Limited liability companies"   = "#EC9007",
  "Partnerships"                  = "#6ecbe2",
  "Sole traders/proprietorships"  = "#c29fea"
)

# Plot
legal_form_extensive_margin <- ggplot(dt, aes(x = legal_form, y = fs_share, fill = legal_form)) +
  geom_jitter(
    position = position_jitter(width = 0.2, height = 0),
    aes(color = legal_form),
    alpha = 0.7,
    size = point_size
  ) +
  geom_boxplot(
    alpha = 0.5,
    show.legend = FALSE,
    outlier.shape = NA
  ) +
  labs(
    x = NULL,
    y = "Share of Disclosing Firms \n (Extensive Margin)"
  ) +
  scale_fill_manual(values = custom_colors) +
  scale_color_manual(values = custom_colors) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    axis.title.x = element_blank(),
    axis.title.y = element_text(size = text_size, family = "Times New Roman"),
    axis.text.x = element_blank(),
    axis.text.y = element_text(size = text_size, family = "Times New Roman"),
    plot.title = element_text(size = 1.2 * text_size, face = "bold", family = "Times New Roman", hjust = 0.5),
    legend.position = "none",
    panel.background = element_rect(fill = "white", color = NA)
  )

legal_form_extensive_margin

######################
# Intensive margin by legal form
######################

# Aggregate data
dt <- dt_ctry_year %>%
  filter(year == 2021) %>%
  group_by(ctry, legal_form) %>%
  summarise(
    statements = sum(n_fs),
    mean_items = sum(fs_items*n_fs) / statements,
    .groups = "drop"
  )



# Rename "Listed" to "Regulated Domestic Listed"
dt$legal_form <- recode(
  dt$legal_form,
  "Listed" = "Regulated Domestic Listed"
)

# Set factor levels for correct ordering
dt$legal_form <- factor(dt$legal_form, levels = c(
  "Regulated Domestic Listed",
  "Other Listed",
  "Limited liability companies",
  "Partnerships",
  "Sole traders/proprietorships"
))

# Define colors for all 5 groups
custom_colors <- c(
  "Regulated Domestic Listed"     = "#934664", # Renamed here
  "Other Listed"                  = "#14676B",
  "Limited liability companies"   = "#EC9007",
  "Partnerships"                  = "#6ecbe2",
  "Sole traders/proprietorships"  = "#c29fea"
)

# Plot
legal_form_intensive_margin <- ggplot(dt, aes(x = legal_form, y = mean_items, fill = legal_form)) +
  geom_jitter(
    position = position_jitter(width = 0.2, height = 0),
    aes(color = legal_form),
    alpha = 0.7,
    size = point_size
  ) +
  geom_boxplot(
    alpha = 0.5,
    show.legend = FALSE,
    outlier.shape = NA
  ) +
  labs(
    x = NULL,
    y = "Average Number of Disclosed Items \n (Intensive Margin)"
  ) +
  scale_fill_manual(values = custom_colors) +
  scale_color_manual(values = custom_colors) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    axis.title.x = element_blank(),
    axis.title.y = element_text(size = text_size, family = "Times New Roman"),
    axis.text.x = element_text(size = text_size, angle = 45, hjust = 1, family = "Times New Roman"),
    axis.text.y = element_text(size = text_size, family = "Times New Roman"),
    plot.title = element_text(size = 1.2 * text_size, face = "bold", family = "Times New Roman", hjust = 0.5),
    legend.position = "none",
    panel.background = element_rect(fill = "white", color = NA)
  )

legal_form_intensive_margin





##############################
# Combine output and save
##############################
combined_plot <- legal_form_extensive_margin  / legal_form_intensive_margin  + 
  plot_annotation(tag_levels = 'A')
combined_plot
dir.create("output/cross_section", showWarnings = FALSE, recursive = TRUE)
ggsave("output/cross_section/cross_section_extensive_itensive.png", plot = combined_plot, width = 5.59, height = 7.5, dpi = 300)

