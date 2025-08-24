library(dplyr)
library(purrr)
library(tibble)
library(patchwork)
library(ggplot2)
library(countrycode)
library(extrafont)
loadfonts(device = "win")


######################
#Load data and get sample 
######################
dt_ctry_year <- read.csv("data/aggregated_data/dt_ctry_year.csv")

# Prep data to summarize
smp <- dt_ctry_year %>% 
  filter(!ctry %in% c("CD", "ME", "RS", "SS", "ER", "VE", "GD", "FJ")) 

smp$ctry[is.na(smp$ctry)] <- "NA"

sum(smp$firms)

# Define categories of firms
smp$category <- ifelse(smp$legal_form %in% c("Partnerships","Limited liability companies","Sole traders/proprietorships" ),
                       "Private",
                       "Public")



######################
# Panel A: Aggregate statistics by year
######################
obs_per_year <- smp %>%
  group_by(year) %>%
  summarise(
    countries = n_distinct(ctry),
    firms = sum(firms),
    statements = sum(n_fs),
    share = statements / firms,
    .groups = "drop"
  ) %>%
  mutate(year = as.character(year)) %>%
  bind_rows(
    tibble(
      year = "Total",
      countries = n_distinct(smp$ctry),
      firms = sum(.$firms),
      statements = sum(.$statements),
      share = statements / firms
    )
  )

# Save
write.csv(obs_per_year , "output/summary_statistics/obs_per_year.csv", row.names = FALSE)


######################
# Aggregate statistics by legal form (year 2021)
######################
obs_per_legal_form <- smp %>%
  filter(year %in% 2021) %>%
  group_by(legal_form) %>%
  summarise(
    countries = n_distinct(ctry),
    firms = sum(firms),
    statements = sum(n_fs),
    share = statements / firms,
    .groups = "drop"
  ) %>%
  bind_rows(
    tibble(
      legal_form = "Total",
      countries = n_distinct(smp$ctry),
      firms = sum(.$firms),
      statements = sum(.$statements),
      share = statements / firms
    )
  )


obs_per_category <- smp %>%
  filter(year %in% 2021) %>%
  group_by(category) %>%
  summarise(
    countries = n_distinct(ctry),
    firms = sum(firms),
    statements = sum(n_fs),
    share = statements / firms,
    .groups = "drop"
  ) %>%
  rename(legal_form = category)

obs_per_legal_form <- rbind(obs_per_legal_form, obs_per_category)

# Save
write.csv(obs_per_legal_form, "output/summary_statistics/obs_per_legal_form.csv", row.names = FALSE)


######################
# Share of firms by legal form
######################
legal_forms <- smp  %>%
  filter(year %in% 2021) %>%
  group_by(legal_form) %>%
  summarise(n = sum(firms))

legal_forms$share <- legal_forms$n / sum(legal_forms$n)



######################
#  Statistics at country level by legal form (year 2021)
######################



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

# Panel A: number of firms
n_firms_by_legal_form <- smp %>%
  filter(year == 2021) %>%
  group_by(legal_form, ctry) %>%
  summarise(n_firms = sum(firms, na.rm = TRUE), .groups = "drop") %>%
  group_by(legal_form) %>%
  summary_stats("n_firms")

n_firms_by_categroy <- smp %>%
  filter(year == 2021) %>%
  group_by(category, ctry) %>%
  summarise(n_firms = sum(firms, na.rm = TRUE), .groups = "drop") %>%
  group_by(category) %>%
  summary_stats("n_firms") %>%
  rename(legal_form = category)

n_firms_by_legal_form <- rbind(n_firms_by_legal_form, n_firms_by_categroy)

# Panel B: share of firms
share_firms_by_legal_form  <- smp %>%
  filter(year == 2021) %>%
  group_by(ctry, legal_form) %>%
  summarise(total_firms_legal_form = sum(firms, na.rm = TRUE)) %>%
  group_by(ctry) %>%
  mutate(total_firms_ctry = sum(total_firms_legal_form, na.rm = TRUE),
         share_firms = total_firms_legal_form / total_firms_ctry) %>%
  group_by(legal_form) %>%
  summary_stats("share_firms") 

share_firms_by_categroy  <- smp %>%
  filter(year == 2021) %>%
  group_by(ctry, category) %>%
  summarise(total_firms_category = sum(firms, na.rm = TRUE)) %>%
  group_by(ctry) %>%
  mutate(total_firms_ctry = sum(total_firms_category, na.rm = TRUE),
         share_firms = total_firms_category / total_firms_ctry) %>%
  group_by(category) %>%
  summary_stats("share_firms") %>%
  rename(legal_form = category)

share_firms_by_legal_form <- rbind(share_firms_by_legal_form, share_firms_by_categroy)


# Panel C: Disclosure rate
disclosure_rate_by_legal_form  <- smp %>%
  filter(year == 2021) %>%
  group_by(ctry, legal_form) %>%
  summarise(n_statements_legal_form = sum(n_fs),
            n_firms_legal_form = sum(firms)) %>%
  mutate(mean_disc_ext = n_statements_legal_form / n_firms_legal_form)%>%
  group_by(legal_form) %>%
  summary_stats("mean_disc_ext")

disclosure_rate_by_category  <- smp %>%
  filter(year == 2021) %>%
  group_by(ctry, category) %>%
  summarise(n_statements_category = sum(n_fs),
            n_firms_category = sum(firms)) %>%
  mutate(mean_disc_ext = n_statements_category / n_firms_category)%>%
  group_by(category) %>%
  summary_stats("mean_disc_ext") %>%
  rename(legal_form = category)

disclosure_rate_by_legal_form <- rbind(disclosure_rate_by_legal_form, disclosure_rate_by_category)


# Save
write.csv(n_firms_by_legal_form, "output/summary_statistics/n_firms_by_legal_form.csv", row.names = FALSE)
write.csv(share_firms_by_legal_form, "output/summary_statistics/share_firms_by_legal_form.csv", row.names = FALSE)
write.csv(disclosure_rate_by_legal_form, "output/summary_statistics/disclosure_rate_by_legal_form.csv", row.names = FALSE)


######################
#  Plot disclosure rate by legal form (year 2021)
######################

# Config plot
text_size  <- 12
point_size <- 2
line_size  <- 1

# Aggregate data
dt <- smp %>%
  filter(year == 2021) %>%
  group_by(ctry, legal_form) %>%
  summarise(
    firms = sum(firms),
    statements = sum(n_fs),
    fs_share = statements / firms,
    .groups = "drop"
  )

# Rename "Listed" to "Listed: Regulated"
dt$legal_form <- recode(
  dt$legal_form,
  "Limited liability companies" = "Private: LLC",
  "Partnerships"                = "Private: Partnership",
  "Sole traders/proprietorships" = "Private: Sole proprietor",
  "Other Listed"                 = "Public: Other",
  "Listed"                       = "Public: Regulated"
)

# Set factor levels for correct ordering
dt$legal_form <- factor(dt$legal_form, levels = c(
  "Private: LLC",
  "Private: Partnership",
  "Private: Sole proprietor",
  "Public: Other",
  "Public: Regulated"
))

# Define colors for all 5 groups
custom_colors <- c(
  "Public: Regulated"        = "#934664",
  "Public: Other"            = "#EC9007",
  "Private: LLC" = "#14676B",
  "Private: Partnership"     = "#6ecbe2",
  "Private: Sole proprietor" = "#c29fea"
)

# Plot
legal_form_disclosure_rate <- ggplot(dt, aes(x = legal_form, y = fs_share, fill = legal_form)) +
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
    y = "Share of Disclosing Firms \n (Disclosure Rate)"
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

ggsave("output/summary_statistics/legal_form_disclosure_rate.png", plot = legal_form_disclosure_rate, width = 6, height = 4, dpi = 300)

######################
#  Investigate disclosure rates
######################

# Above/Below 0.01 
dt <- smp %>%
  filter(year == 2021) %>%
  group_by(ctry,legal_form) %>%
  summarise(
    firms = sum(firms),
    statements = sum(n_fs),
    fs_share = statements / firms,
    .groups = "drop"
  ) %>%
  group_by(legal_form) %>%
  summarise(n = n(),
            above = sum(fs_share > 0.01),
            below = sum(fs_share < 0.01) )

# Get extremes
dt <- smp %>%
  filter(year == 2021) %>%
  filter(legal_form %in% "Limited liability companies") %>%
  group_by(ctry) %>%
  summarise(
    firms = sum(firms),
    statements = sum(n_fs),
    fs_share = statements / firms,
    .groups = "drop"
  ) %>%
  filter(fs_share == max(fs_share))



######################
#  World Maps - Disclosure Rate
######################

# Panel A: Unlisted 
dt <- dt_ctry_year %>% 
  filter(!ctry %in% c("CD", "ME", "RS", "SS", "ER", "VE", "GD", "FJ"),
         year %in% 2021,
         legal_form %in% c("Partnerships", "Limited liability companies", "Sole traders/proprietorships")) %>%
  group_by(ctry) %>%
  summarize(firms_total = sum(firms),
            statements_total = sum(n_fs),
            statements_share = statements_total / firms_total,
            mean_items = sum(fs_items*n_fs, na.rm = T) / statements_total)



world_map <- map_data("world") %>%
  mutate(ctry= countrycode(region, "country.name", "iso2c"))

world_map <- left_join(world_map, dt, by = "ctry")


text_size <- 10

disclosure_rate_all_private <- ggplot(world_map, aes(x = long, y = lat, group = group, fill = statements_share)) +
  geom_polygon(color = NA) +  # Remove country borders for a clean look
  scale_fill_gradient(low = "#bbdde1", high = "#0f4547", name = element_blank()) +  # Adjust the fill gradient
  labs(
    title = "Disclosure Rate \n (All Private Firms)"
  ) +
  theme_minimal(base_family = "Times New Roman") +  # Use consistent font style
  theme(
    axis.text = element_blank(),  # Remove axis text
    axis.title = element_blank(),  # Remove axis titles
    axis.ticks = element_blank(),  # Remove axis ticks
    panel.grid = element_blank(),  # Remove grid lines
    plot.title = element_text(size = text_size + 2, family = "Times New Roman", hjust = 0.5),
    legend.text = element_text(size = text_size, family = "Times New Roman"),
    legend.title = element_text(size = text_size, family = "Times New Roman"),
    panel.background = element_rect(fill = "white", color = NA)  # Ensure a clean background
  ) +
  coord_fixed(1.3) +  # Keep the aspect ratio fixed
  coord_cartesian(ylim = c(-50, 90))  # Adjust latitude range
disclosure_rate_all_private


# Panel B: LLC
dt <- dt_ctry_year %>% 
  filter(!ctry %in% c("CD", "ME", "RS", "SS", "ER", "VE", "GD", "FJ"),
         year %in% 2021,
         legal_form %in% c("Limited liability companies")) %>%
  group_by(ctry) %>%
  summarize(firms_total = sum(firms),
            statements_total = sum(n_fs),
            statements_share = statements_total / firms_total,
            mean_items = sum(fs_items*n_fs, na.rm = T) / statements_total)



world_map <- map_data("world") %>%
  mutate(ctry= countrycode(region, "country.name", "iso2c"))

world_map <- left_join(world_map, dt, by = "ctry")


text_size <- 10

disclosure_rate_private_llc <- ggplot(world_map, aes(x = long, y = lat, group = group, fill = statements_share)) +
  geom_polygon(color = NA) +  # Remove country borders for a clean look
  scale_fill_gradient(low = "#bbdde1", high = "#0f4547", name = element_blank()) +  # Adjust the fill gradient
  labs(
    title = "Disclosure Rate \n (Private: LLC)"
  ) +
  theme_minimal(base_family = "Times New Roman") +  # Use consistent font style
  theme(
    axis.text = element_blank(),  # Remove axis text
    axis.title = element_blank(),  # Remove axis titles
    axis.ticks = element_blank(),  # Remove axis ticks
    panel.grid = element_blank(),  # Remove grid lines
    plot.title = element_text(size = text_size + 2, family = "Times New Roman", hjust = 0.5),
    legend.text = element_text(size = text_size, family = "Times New Roman"),
    legend.title = element_text(size = text_size, family = "Times New Roman"),
    panel.background = element_rect(fill = "white", color = NA)  # Ensure a clean background
  ) +
  coord_fixed(1.3) +  # Keep the aspect ratio fixed
  coord_cartesian(ylim = c(-50, 90))  # Adjust latitude range
disclosure_rate_private_llc






disclosure_rate_all_private <- disclosure_rate_all_private +
  theme(legend.position = "bottom", legend.justification = "center")

disclosure_rate_private_llc <- disclosure_rate_private_llc +
  theme(legend.position = "bottom", legend.justification = "center")

combined_plot <- disclosure_rate_all_private / disclosure_rate_private_llc +
  plot_annotation(tag_levels = 'A') +
  plot_layout(guides = "collect") & theme(legend.position = "bottom")

ggsave("output/summary_statistics/map.png",
       plot = combined_plot,
       width = 6, height = 8.5, units = "in", dpi = 300)




