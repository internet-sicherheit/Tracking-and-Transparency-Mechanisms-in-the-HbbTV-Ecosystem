library(RColorBrewer)
require(ggplot2)
library("ggmap")
library(maptools)
library(maps)
library(dplyr)
library(PBSmapping)
library(ggthemes)
library(sf)
library(ggpubr)
library(ggalluvial)

cookie_data =  read.csv("{path}\\cookie_usage.csv",
                        header = TRUE, sep = ",")
MY_FONT_FACE= "sans"

third_party_counts <- cookie_data %>% group_by(origin) %>%  count() %>%ungroup() %>%  arrange(desc(n)) %>% top_n(10)
top_parties <- third_party_counts['origin']
shouldBecomeOther<-!(cookie_data$origin %in% unlist(top_parties))
cookie_data$origin[shouldBecomeOther]<- "Other"
cookie_data <- cookie_data %>% group_by(origin) %>%  mutate(count=n())

test <- head(cookie_data)

alluvial_map <- ggplot(cookie_data, aes(y=count, axis1=channelname, axis2=origin)) +
  geom_alluvium(width = 1/12) 
  # geom_stratum(width = 1/12, fill = "black", color = "grey") +
  # scale_x_discrete(limits = c("Channel", "Third Party"), expand = c(.05, .05)) +
  # scale_y_continuous() +
  # # scale_fill_continuous(low="#D3D3D3", high="#000000", guide="colorbar", name="Requests") +
  # geom_text(stat = "stratum", color="white",label.strata = T, angle=90) +
  # # theme_bw()  +
  # labs_pubr() +
  # theme(plot.title = element_text(hjust = 0.5),
  #       text=element_text(MY_FONT_FACE),
  #       axis.text.y = element_blank(),
  #       axis.title.y = element_blank(),
  #       axis.ticks = element_blank(),
  #       legend.position="bottom")
alluvial_map
ggsave(paste(IMG_PATH, "origin_taget_alluvial_map.png", sep=""),device ="pdf", dpi=600, plot=alluvial_map)