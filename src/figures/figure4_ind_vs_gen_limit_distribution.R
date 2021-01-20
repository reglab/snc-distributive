#####
# Generate Figure 4 Discharge Limit Distributions
# Author: Nicole Lin
# Input:
# permit and limit data from RegLab AWS database
# Output: 
# output/figures/figure4_wastewater_sewage_common_pollutant_density.pdf
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

# subset to 2018
# limits that are quantities

query <- "
with permits as(
  select npdes_permit_id, 
    permit_state, 
    individual_permit_flag, 
    wastewater_permit_flag,
    sewage_permit_flag
  from icis.permits
  where latest_version_flag
  and is_currently_active_flag
),

parameters as (
select parameter_code, snc_flag
from ontologies.icis__ref_parameter
), 

dmr as (
select 
  npdes_permit_id, 
  monitoring_period_end_date,
  parameter_code,
  parameter_desc,
  snc_flag,
  limit_value_standard_units,
  standard_unit_desc,
  statistical_base_monthly_avg,
  extract (year from monitoring_period_end_date) as monitoring_year
from icis.dmrs_dedupped
left join parameters
using (parameter_code)
where limit_value_qualifier_code in ('<', '<=')
and limit_value_type_code in ('Q1', 'Q2')
)

select *
from dmr 
left join permits 
using (npdes_permit_id)
where monitoring_year = 2018
"

data <-  dbSendQuery(con, query) %>%
  dbFetch()

territories <- c('AS', 'GU', 'MP', 'PR', 'VI', 'UM')
keep_states <- data %>%
  filter(wastewater_permit_flag == 1 & sewage_permit_flag == 1) %>%
  filter(!(permit_state %in% territories)) %>%
  group_by(permit_state) %>%
  summarise(permit_count = n()) %>%
  filter(permit_count > 10)

wwtp_sewage <- data %>%
  filter(wastewater_permit_flag == 1 & sewage_permit_flag == 1) %>%
  filter(permit_state %in% keep_states$permit_state) 


# plot distributions ------------------------------------------------------

# note not a lof of general permits submit DMRs
# to make the two groups comparable - randomly select the same number of individual permits from the sample
wwtp_sewage %>%
  group_by(npdes_permit_id, individual_permit_flag)%>%
  summarise(n= n()) %>%
  group_by(individual_permit_flag) %>%
  summarise(count = n())

individual_ids <- unique(wwtp_sewage[wwtp_sewage$individual_permit_flag == 1, ]$npdes_permit_id)
general_ids <- unique(wwtp_sewage[wwtp_sewage$individual_permit_flag == 0, ]$npdes_permit_id)
n_individual <- length(individual_ids)
n_general <- length(general_ids)

set.seed(seed)
sub_individual_ids <- sample(individual_ids, n_general)

# a) pollutants coverage
plot_df <- wwtp_sewage %>%
  filter(npdes_permit_id %in% c(sub_individual_ids, general_ids)) %>%
  mutate(individual_permit_flag = ifelse(individual_permit_flag == 1, 'individual', 'general')) %>%
  mutate(snc_flag = replace_na(snc_flag, 3)) %>%
  mutate(snc_flag = paste0('Category ', snc_flag)) %>%
  group_by( parameter_code, parameter_desc, snc_flag, individual_permit_flag) %>%
  summarise(n = n()) %>%
  spread(individual_permit_flag, n) %>%
  mutate_if(is.integer, replace_na, 0) %>%
  mutate(coverage = ifelse(individual > 0 & general > 0, 'Both',
                           ifelse(individual > 0 & general == 0, 'Only\nIndividual\nPermits', 'Only\nGeneral\nPermits')))

# b) limit values for 6 common pollutants
tmp <- plot_df %>%
  filter(coverage == 'Both') %>%
  arrange(-general) %>%
  head(16) 
common_pollutants <- c('00310', '80082', '00665', '00056', '00530', '01051')

plot_df <- wwtp_sewage %>%
  #filter(npdes_permit_id %in% c(sub_individual_ids, general_ids)) %>%
  filter(parameter_code %in% common_pollutants) %>%
  mutate(individual_permit_flag = ifelse(individual_permit_flag == 1, 'individual', 'general')) %>%
  distinct(npdes_permit_id, permit_state, individual_permit_flag, parameter_code, parameter_desc, limit_value_standard_units, standard_unit_desc) 

make_limit_comparison <- function(plot_df, code){
  df <- plot_df %>%
    filter(parameter_code == code) 
  
  # remove outliers
  q <- quantile(df$limit_value_standard_units, probs = c(0.25, 0.75), na.rm = TRUE)
  iqr <- IQR(df$limit_value_standard_units)
  up <- q[2] + 1.5 * iqr
  low <- q[1] - 1.5 * iqr
  
  df <- df %>%
    filter(limit_value_standard_units < up & limit_value_standard_units > low)
  
  # return dataframe
  df
}

sub_plot_df <- lapply(common_pollutants, make_limit_comparison, plot_df = plot_df) %>%
  do.call(rbind, .) %>%
  mutate(standard_unit_desc = ifelse(standard_unit_desc == 'MGD', 'Million Gallons per Day', 
                                     ifelse(standard_unit_desc == 'kg/d', 'kg/day', standard_unit_desc))) %>%
  mutate(parameter_desc = paste0(parameter_desc, ' (', standard_unit_desc, ')'))

table(sub_plot_df$individual_permit_flag)

ggplot(sub_plot_df, aes(x = limit_value_standard_units, 
                        group = individual_permit_flag, 
                        fill = individual_permit_flag)) +
  geom_density(alpha = 0.6) +
  facet_wrap(~parameter_desc, scales = 'free') +
  labs(x = 'Limit Values In Quantities',
       y = 'Density') +
  scale_fill_manual(name = 'Sewage WWTP Permit Type',
                    labels = c('General', 'Individual'),
                    values = c(color1, color2)) +
  theme_bw() +
  theme(legend.position = 'bottom')

ggsave(file.path(output_dir, 'figure4_wastewater_sewage_common_pollutant_density.pdf'),
       width = 9,
       height = 5)
