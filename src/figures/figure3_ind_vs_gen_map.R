#####
# Generate Figure 3 Wastewater Facilities Map
# Author: Nicole Lin
# Input:
# permit and location data from RegLab AWS database
# Output: Calibration Plots
# output/figures/figure3a_wastewater_sewerage_map.png
# output/figures/figure3b_wastewater_sewerage_map_VA_NC.png
#####


# set up ------------------------------------------------------------------

# set repo dir regvcalss as the working directory
repo_dir <- rstudioapi::getActiveProject()
setwd(repo_dir) 

output_dir <- file.path('output', 'figures')
if (!dir.exists(output_dir)){
  dir.create(output_dir)
}

# import packages
if (!require("pacman")){
  install.packages("pacman", repos=" https://CRAN.R-project.org")
}

# pacman is a package that helps with loading libraries: https://cran.r-project.org/web/packages/pacman/index.html
library(pacman)
p_load('lubridate',  # for date variables
       'stringr',  # for strings manipulation
       'tidyverse', # for dataframe operations
       'RPostgres', # for AWS database connection
       'DBI',  # for AWS database connection
       'ggmap', # for ploting maps
       'grid', # for anotation with table
       'gridExtra'
) 


# database connection credentials 
# for instructions on how to save credentials as environment variables: https://asconfluence.stanford.edu/confluence/display/REGLAB/PostgreSQL+Database#PostgreSQLDatabase-ManagingUsersandRoles
db_name <- Sys.getenv('EPA_DB_NAME')
db_host <- Sys.getenv('EPA_DB_HOST')
db_port <- Sys.getenv('EPA_DB_PORT')
db_username <- Sys.getenv('EPA_DB_USER')
db_password <- Sys.getenv('EPA_DB_PASSWORD')

# connect to the AWS database
con <- dbConnect(RPostgres::Postgres(),
                 dbname=db_name,
                 host=db_host,
                 port=db_port ,
                 user=db_username,
                 password=db_password)

## set gloabl variables
color1 <- '#E6945C'
color2 <- '#5CA7E6'
seed <- 333


# read data ---------------------------------------------------------------

query <- "
with permits as(
  select npdes_permit_id, 
    permit_state, 
    individual_permit_flag, 
    wastewater_permit_flag,
    major_permit_flag,
    sewage_permit_flag
  from icis.permits
  where latest_version_flag
  and is_currently_active_flag
),

locations as(
 select npdes_permit_id, 
    geocode_latitude,
    geocode_longitude
 from icis.facilities
)

select *
from permits 
left join locations
using (npdes_permit_id)
"

data <-  dbSendQuery(con, query) %>%
  dbFetch()

# remove territories and states with less than 10 permits
territories <- c('AS', 'GU', 'MP', 'PR', 'VI', 'UM')
keep_states <- data %>%
  filter(wastewater_permit_flag == 1 & sewage_permit_flag == 1) %>%
  filter(!(permit_state %in% territories)) %>%
  group_by(permit_state) %>%
  summarise(permit_count = n()) %>%
  filter(permit_count > 10)


# map across US -----------------------------------------------------------

register_google(key = Sys.getenv('SNC_MAP_API'))

us <- c(left = -128, bottom = 25, right = -67, top = 49)
map <- get_stamenmap(us, zoom = 5, maptype = "toner-lite")

plot_df <- data %>%
  filter(wastewater_permit_flag == 1 & sewage_permit_flag == 1) %>%
  filter(permit_state %in% keep_states$permit_state) %>%
  mutate(individual_permit_flag = ifelse(individual_permit_flag == 1, 'Individual Permit', 'General Permit'))

n_individual <- sum(plot_df$individual_permit_flag == 'Individual Permit')
n_general <- sum(plot_df$individual_permit_flag == 'General Permit')

# randomize order of points
set.seed(seed)
random_idx <- sample(1:nrow(plot_df), nrow(plot_df), replace = FALSE)
plot_df <- plot_df[random_idx, ]

ggmap(map) +
  geom_point(data = plot_df, 
             aes(x = geocode_longitude, y = geocode_latitude, color = individual_permit_flag),
             size = 0.3,
             alpha = 0.3) +
  annotate('text', x= -122.2, y= 27, 
           label = paste0('Individual Permit\nn = ', format(n_individual, big.mark = ',')),
           color = color2,
           size = 2.5,
           fontface =2) +
  annotate('text', x= -122.6, y= 30.2, 
           label = paste0('General Permit\nn = ', format(n_general, big.mark = ',')), 
           color = color1,
           size = 2.5,
           fontface =2) +
  scale_color_manual(name = '',
                     values = c(color1, color2)) +
  labs(x ='',
       y = '') +
  theme(legend.position = 'none',
        legend.text = element_text (color = color1),
        axis.text.x = element_blank(),
        axis.text.y = element_blank(),
        axis.ticks = element_blank(),
        rect = element_blank()) 
ggsave(file.path(output_dir, 'figure3a_wastewater_sewerage_map.png'),
       width = 5,
       height = 4)


# highlight a pair of states ----------------------------------------------

states <- c('VA', 'NC')
sub_us <- c(left = -85, bottom = 33, right = -75, top = 40)
map <- get_stamenmap(sub_us, zoom = 6, maptype = "toner-lite")

state_plot_df <- plot_df %>%
  filter(permit_state %in% states)

# make accompanying summary statistics table
permit_count <- state_plot_df %>%
  group_by(permit_state, individual_permit_flag) %>%
  summarise(count = n()) %>%
  spread(individual_permit_flag, count) %>%
  arrange(`General Permit`)
names(permit_count) <- c('State', 'General', 'Individual')
cols <- matrix("black", nrow(permit_count) , ncol(permit_count))
cols[, 2] <- color1
cols[, 3] <- color2
tt <- ttheme_minimal(core=list(fg_params = list(col = cols),
                               bg_params = list(col=NA)),
                     rowhead=list(bg_params = list(col=NA)),
                     colhead=list(bg_params = list(col=NA)),
                     base_size = 10,
                     padding = unit(c(2,2), 'mm'))
table <- tableGrob(permit_count, rows = NULL, theme = tt)
table$grobs[[2]] <- editGrob(table$grobs[[2]], gp=gpar(col= color1))
table$grobs[[3]] <- editGrob(table$grobs[[3]], gp=gpar(col= color2))

ggmap(map) +
  geom_point(data = state_plot_df, 
             aes(x = geocode_longitude, y = geocode_latitude, color = individual_permit_flag),
             size = 1,
             alpha = 0.3) +
  scale_color_manual(name = '',
                     values = c(color1, color2)) +
  labs(x ='',
       y = '') +
  inset(table,xmin = -78.6, xmax = -75, ymin = 32, ymax = 35) +
  theme(legend.position = 'none',
        legend.text = element_text (color = color1),
        axis.text.x = element_blank(),
        axis.text.y = element_blank(),
        axis.ticks = element_blank(),
        rect = element_blank()) 

ggsave(file.path(output_dir, 'figure3b_wastewater_sewerage_map_VA_NC.png'),
       width = 5,
       height = 5)

