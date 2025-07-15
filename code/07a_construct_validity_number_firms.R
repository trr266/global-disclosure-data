
library(dplyr)
library(ggplot2)
library(readxl)
library(extrafont)
loadfonts(device = "win")



##############################
# Prep World Bank Entrepreneurship Data
# Manually downloaded: https://www.worldbank.org/en/programs/entrepreneurship
##############################


# Load data
wb_data <- read_excel("data/country_characteristics/worldbank_data/wb_entrepreneurship_data_raw.xlsx")
ctry_profiles <- read.csv("data/country_characteristics/ctry_profiles.csv")

# Remove the last 4 rows 
wb_data <- wb_data[c(1:1707),]




# Remove asterisk an rename column
wb_data$Economy <- gsub("\\*", "", wb_data$Economy)
colnames(wb_data)[1] <- "ctry_name"

# Get list of Countries
countries <- data.frame("ctry_name" = unique(wb_data$ctry_name)) 
iso2_codes <- ctry_profiles[,c("ctry_name", "iso2")]



#Try to merge 
countries <- merge(countries, iso2_codes, by = "ctry_name", all.x = T)


# Get countries that could not be assigned
countries$ctry_name[is.na(countries$iso2)]

# Manually (aka ChatGPT) assign the missing codes
countries$iso2[countries$ctry_name %in% "Brunei Darussalam"] <- "BN"
countries$iso2[countries$ctry_name %in% "Cayman Islands"] <- "KY"
countries$iso2[countries$ctry_name %in% "Congo, Dem. Rep."] <- "CD"
countries$iso2[countries$ctry_name %in% "Cook Islands"] <- "CK"
countries$iso2[countries$ctry_name %in% "Egypt, Arab Rep."] <- "EG"
countries$iso2[countries$ctry_name %in% "Guernsey"] <- "GG"
countries$iso2[countries$ctry_name %in% "Hong Kong SAR, China"] <- "HK"
countries$iso2[countries$ctry_name %in% "Iran, Islamic Rep."] <- "IR"
countries$iso2[countries$ctry_name %in% "Isle of Man"] <- "IM"
countries$iso2[countries$ctry_name %in% "Jersey"] <- "JE"
countries$iso2[countries$ctry_name %in% "Korea, Rep."] <- "KR"
countries$iso2[countries$ctry_name %in% "Kosovo"] <- "XK"
countries$iso2[countries$ctry_name %in% "Kyrgyz Republic"] <- "KG"
countries$iso2[countries$ctry_name %in% "Myanmar"] <- "MM"
countries$iso2[countries$ctry_name %in% "Namibia"] <- "NA"
countries$iso2[countries$ctry_name %in% "North Macedonia, Rep"] <- "MK"
countries$iso2[countries$ctry_name %in% "Puerto Rico, US"] <- "PR"
countries$iso2[countries$ctry_name %in% "Russian Federation"] <- "RU"
countries$iso2[countries$ctry_name %in% "Slovak Republic"] <- "SK"
countries$iso2[countries$ctry_name %in% "Taiwan, China"] <- "TW"

# Manually assign value to turkey
countries$iso2[125] <- "TR"



# Check that all are assigned than merge
if(sum(is.na(countries$iso2)) == 0){
  wb_data <- merge(wb_data, countries, by = c("ctry_name"))
}else{
  print("Some iso 2 codes are missing!")
}



# Do some final cleaning
wb_data <- wb_data[,c("iso2", "Year", "Total Number of\r\nLimited Liability Companies")]
colnames(wb_data) <- c("ctry", "year", "n_llc_wb")
wb_data <- wb_data[complete.cases(wb_data),]
wb_data <- wb_data %>% filter(year %in% 2021)


##############################
# Load Orbis data, merge, and make plot
##############################

# Prep Orbis data
dt_ctry_year <- read.csv("data/aggregated_data/dt_ctry_year.csv")

analysis_year <- 2021

# Prep orbis data data
dt <- dt_ctry_year %>%
  filter(year %in% 2021) %>%
  filter(legal_form %in% "Limited liability companies") %>%
  group_by(ctry) %>%
  summarize("firms" = sum(firms, na.rm =T))


# Merge
dt <- left_join(dt, wb_data, by = c("ctry"))

text_size  <- 18  
point_size <- 2.5  
line_size  <- 1.2  

wb_orbis_scatter <- ggplot(data = dt, aes(x = log(firms), y = log(n_llc_wb))) +
  geom_point(color = "#14676B", alpha = 0.7, size = point_size) +
  geom_abline(intercept = 0, slope = 1, linetype = "dashed", size = line_size, color = "#EC9007") +
  expand_limits(x = 0, y = 0) +
  labs(
    x = "log(Limited liability companies - Orbis)",
    y = "log(Limited liability companies - World Bank)",
    title = element_blank()
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    axis.text  = element_text(size = text_size, family = "Times New Roman"),
    axis.title = element_text(size = text_size + 2, family = "Times New Roman"),
    plot.title = element_text(size = text_size + 4, family = "Times New Roman", face = "bold")
  )

wb_orbis_scatter

dir.create("output/construct_validity", showWarnings = FALSE, recursive = TRUE)

ggsave("output/construct_validity/wb_orbis_scatter.png")
sum(!is.na(dt$n_llc_wb))


