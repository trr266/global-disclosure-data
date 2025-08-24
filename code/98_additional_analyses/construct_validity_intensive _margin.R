library(dplyr)
library(ggplot2)
library(arrow)
library(extrafont)
loadfonts(device = "win")


###############################################
# Load and filter data
###############################################

# Set countries
ctry_1 <- "BE"
ctry_2 <- "DE"
ctry_1_name <- "Belgium"
ctry_2_name <- "Germany"

# Load files
df_ctry_1 <- read_parquet(paste0("data/entity_level_data/",ctry_1,"_entity_level_data.parquet"))
df_ctry_2 <- read_parquet(paste0("data/entity_level_data/",ctry_2,"_entity_level_data.parquet"))

# Get year 2021
df_ctry_1 <- df_ctry_1 %>% filter(year %in% 2021 & legal_form %in% c("Public limited companies", "Private limited companies") & !listed) %>% mutate(ctry = ctry_1)
df_ctry_2 <- df_ctry_2 %>% filter(year %in% 2021 & legal_form %in% c("Public limited companies", "Private limited companies") & !listed) %>% mutate(ctry = ctry_2)

# Relabel countries
df_ctry_1$ctry <- ctry_1_name
df_ctry_2$ctry <- ctry_2_name



###############################################
# Consider exchange rate (Orbis data is in dollar)
###############################################
df_ctry_1$total_assets <- df_ctry_1$total_assets /1.14
df_ctry_2$total_assets <- df_ctry_2$total_assets /1.14

###############################################
# Make a plot
###############################################
dt <- rbind(df_ctry_1, df_ctry_2)

# Define bin breaks 
breaks <- seq(1e6, 11e6, length.out = 21)

# Create size groups based on raw Total Assets
dt <- dt %>%
  filter(total_assets >= 1e6 & total_assets  <= 11e6) %>%  # optional: filter to focus on this range
  mutate(size_group = cut(total_assets,
                          breaks = breaks,
                          include.lowest = TRUE))


# Calculate average fs_items per size group and country
avg_items <- dt %>%
  group_by(size_group, ctry) %>%
  summarise(avg_fs_items = mean(fs_total, na.rm = TRUE)) %>%
  ungroup()

# Convert size_group to a numeric midpoint for plotting on a continuous x-axis
avg_items <- avg_items %>%
  mutate(midpoint = as.numeric(gsub(".*,|\\]", "", levels(size_group)))[size_group])


text_size  <- 14
point_size <- 2
line_size  <- 1

construct_validity_intensive <- ggplot(avg_items, aes(x = midpoint, y = avg_fs_items, color = ctry)) +
  geom_line(linewidth = line_size) +
  geom_point(size = point_size) +
  geom_vline(xintercept = 6e6, color = "red", linetype = "dashed", linewidth = line_size) +
  scale_color_manual(values = c("Belgium" = "#EC9007", "Germany" = "#14676B")) +
  scale_x_continuous(labels = scales::label_comma(), name = "Total Assets") +
  labs(title = element_blank(),
       y = "Financial Statement Items (Intensive Margin)",
       color = "Country") +
  theme_minimal(base_size = text_size, base_family = "Times New Roman")
construct_validity_intensive
ggsave("output/construct_validity/construct_validity_intensive.png")
