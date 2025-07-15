
# Load required libraries
library(ggplot2)
library(dplyr)
library(countrycode)
library(extrafont)
loadfonts(device = "win")




###########################################
# Load data
###########################################

dt_ctry_year <- read.csv("data/aggregated_data/dt_ctry_year.csv")



###########################################
# World Map of sample firms
###########################################


# Prep orbis data data
dt <- dt_ctry_year %>%
  filter(year %in% 2021 & legal_form %in% "Limited liability companies") %>%
  group_by(ctry) %>%
  summarize(firms_total = sum(firms),
            statements_total = sum(n_fs),
            statements_share = statements_total / firms_total,
            mean_items = sum(fs_items*n_fs, na.rm = T) / statements_total)


dt$ctry[is.na(dt$ctry)] <- "NA"

###########################################
# World Map of sample firms
###########################################

world_map <- map_data("world") %>%
  mutate(ctry= countrycode(region, "country.name", "iso2c"))

world_map <- left_join(world_map, dt, by = "ctry")


text_size <- 10

extensive_map <- ggplot(world_map, aes(x = long, y = lat, group = group, fill = statements_share)) +
  geom_polygon(color = NA) +  # Remove country borders for a clean look
  scale_fill_gradient(low = "#bbdde1", high = "#0f4547", name = element_blank()) +  # Adjust the fill gradient
  labs(
    title = "Share of Disclosing Firms \n (Extensive Margin)"
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
extensive_map



intensive_map <-  ggplot(world_map, aes(x = long, y = lat, group = group, fill = mean_items)) +
  geom_polygon(color = NA) +  # Remove country borders for a clean look
  scale_fill_gradient(low = "#bbdde1", high = "#0f4547", name = element_blank()) +  # Adjust the fill gradient
  labs(
    title = "Average Number of Disclosed Items \n (Intensive Margin)"
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
intensive_map


##############################
# Combine output and save
##############################

dir.create("output/main_analysis", showWarnings = FALSE, recursive = TRUE)
combined_plot <- extensive_map  / intensive_map + 
  plot_annotation(tag_levels = 'A')
combined_plot
ggsave("output/main_analysis/llc__map.png", plot = combined_plot, width = 5.59, height = 6.5, dpi = 300)
