#####
# Generate Table 4 Summary Statistics for Prediction Data in the Appendix of SNC Distributive Impacts Paper: https://www.overleaf.com/project/5eed55fb7ff4580001737f38
# Input: random forest prediction data
## data/prediction/prediction_data.rds
# Output: summary statistics table for predictive features in latex format
## output/tables/tableS4_prediction_data_summary_<run_time>.latex
#####

# for numeric features: datatype,  mean (SD), range (min - max), # of missing (pencentage)
# for categorical features: datatype,  # of levels, (distribution chart), # of missing

# set up ------------------------------------------------------------------

# set repo dir regvcalss as the working directory
repo_dir <- rstudioapi::getActiveProject()
setwd(repo_dir) 

input_dir <- file.path(repo_dir, 'data', 'prediction')
output_dir <- file.path(repo_dir, 'output', 'tables')

run_time <- Sys.time() %>%
  substr(., 1, 20) %>%
  str_replace_all(., ' ', '_') %>%
  str_replace_all(., ':', '-')

# import packages
if (!require("pacman")){
  install.packages("pacman", repos=" https://CRAN.R-project.org")
}

# pacman is a package that helps with loading libraries: https://cran.r-project.org/web/packages/pacman/index.html
library(pacman)
p_load('lubridate',  # for date variables
       'stringr',  # for strings manipulation
       'tidyverse' # for dataframe operations
       ) 


# read data ---------------------------------------------------------------

prediction_data <- readRDS(file.path(input_dir, 'prediction_data.rds')) 

# numeric features --------------------------------------------------------

summarise_numeric <- function(data, feature){
  values <- pull(data, feature)
  m <- round(mean(values, na.rm = TRUE), 2) %>%
    format(big.mark = ',', scientific = FALSE)
  s <- round(sd(values, na.rm = TRUE), 2)%>%
    format(big.mark = ',', scientific = FALSE)
  mi <- round(min(values, na.rm = TRUE), 2)%>%
    format(big.mark = ',', scientific = FALSE)
  ma <- round(max(values, na.rm = TRUE), 2)%>%
    format(big.mark = ',', scientific = FALSE)
  tibble(feature = feature,
         mean = paste0(m, ' (', s, ')'),
         range = paste0(mi, ' - ', ma)) 
}


numeric_features <- c("previous_measurement_count" ,
                      "previous_true_cat_1and2_exceedence_pct_sum",
                      "previous_true_cat_1and2_weighted_exceedence_pct_sum",
                      "previous_true_chronic_violation_count",
                      "previous_true_TRC_violation_count",
                      "target_measurement_count",
                      "target_prediction_cat_1and2_exceedence_pct_sum",
                      "target_prediction_cat_1and2_weighted_exceedence_pct_sum",
                      "target_prediction_chronic_violation_count" ,
                      "target_prediction_TRC_violation_count",
                      "total_design_flow_nmbr",
                      "actual_average_flow_nmbr")

numeric_table <- lapply(numeric_features, summarise_numeric, data = prediction_data) %>%
  do.call(rbind,.)
previous_miss <- sum(prediction_data$previous_measurement_count == 0)
previous_miss_pct <- round(previous_miss/nrow(prediction_data), 2)*100
previous_miss_col <- paste0(previous_miss %>%
                              format(big.mark = ',', scientific = FALSE),
                            ' (', previous_miss_pct, '\\%)')
target_miss <- sum(prediction_data$target_measurement_count == 0)
target_miss_pct <- round(target_miss/nrow(prediction_data), 2)*100
target_miss_col <- paste0(target_miss %>%
                              format(big.mark = ',', scientific = FALSE),
                            ' (', target_miss_pct, '\\%)')
design_flow_miss <- sum(prediction_data$total_design_flow_nmbr == 0)
design_flow_miss_pct <- round(design_flow_miss/nrow(prediction_data), 2)*100
design_flow_miss_col <- paste0(design_flow_miss %>%
                            format(big.mark = ',', scientific = FALSE),
                          ' (', design_flow_miss_pct, '\\%)')
actual_flow_miss <- sum(prediction_data$actual_average_flow_nmbr == 0)
actual_flow_miss_pct <- round(actual_flow_miss/nrow(prediction_data), 2)*100
actual_flow_miss_col <- paste0(actual_flow_miss %>%
                                 format(big.mark = ',', scientific = FALSE),
                               ' (', actual_flow_miss_pct, '\\%)')

numeric_table <- numeric_table %>%
  mutate(missing = ifelse(str_detect(feature, 'previous'), previous_miss_col, 
                          ifelse(str_detect(feature, 'target'), target_miss_col,
                                 ifelse(str_detect(feature, 'design_flow'), design_flow_miss_col, actual_flow_miss_col
                                        )
                                 )
                          )
         )

# categorical features ----------------------------------------------------


summarise_cat <- function(data, feature){
  values <- pull(data, feature)
  
  n <- length(unique(values))
  m <- names(which.max(table(values)))
  m_pct <- round(as.numeric(table(values)[m])/length(values), 2)*100
  miss <- round(sum(values == 'missing'), 2) %>%
    format(big.mark = ',', scientific = FALSE)
  miss_pct <- round(sum(values == 'missing')/length(values), 2)*100
  
  if (feature == 'facility_type_indicator'){
    if (m == 'NON-POTW'){m <- 'Non-POTW'}
  } else if (feature == 'individual_permit_flag'){
    if (m == '1'){m <- 'Individual'}
  } else if (feature == 'major_minor_status_flag'){
    if (m == 'N'){m <- 'Minor'}
  } else if (feature == 'wastewater_permit_flag'){
    if (m == '1'){m <- 'Wastewater'}
  } else if (feature == 'sewage_permit_flag'){
    if (m == '0'){m <- 'Non-Sewage'}
  } else if (feature == 'facility_type_code'){
    if (m == 'POF'){m <- 'Privately Owned \\\\ Facility'}
  } else if (feature == 'impaired_waters'){
    if (m == 'missing'){m <- 'Missing \\\\ (Non-303(D)-Listed)'}
  } else if (feature == 'status_1_quarter_before'){
    if (m == 'missing'){m <- 'Missing \\\\ (Automatic Compliant)'}
  }
  
  tibble(feature = feature,
         count = n,
         mode = paste0(m, ' (', m_pct, '\\%)'),
         missing = paste0(miss, ' (', miss_pct, '\\%)'))
}

cat_features <- c("facility_type_indicator",
                  "individual_permit_flag",
                  "major_minor_status_flag",
                  "wastewater_permit_flag",
                  "sewage_permit_flag",
                  "facility_type_code",
                  "impaired_waters",
                  "status_1_quarter_before",
                  "status_2_quarter_before",
                  "status_3_quarter_before",
                  "status_4_quarter_before",
                  "status_5_quarter_before",
                  "status_6_quarter_before",
                  "status_7_quarter_before",
                  "status_8_quarter_before")

cat_table <- lapply(cat_features, summarise_cat, data = prediction_data) %>%
  do.call(rbind,.)


f# assemble latex table ----------------------------------------------------
# customized format 

latex_table <- paste0('

\\begin{table*}[!htbp] \\centering 
\\begin{tabular}{llccc} 
\\\\
\\multicolumn{5}{c}{\\textbf{\\large{Numerical Features}}}
\\\\
\\\\','[-1.8ex]\\hline 
\\hline \\\\','[-1.8ex] 
 & \\textbf{Feature} & \\textbf{Mean (SD)} & \\textbf{Range} & \\textbf{Missing Count (\\%)} \\\\ 
\\hline \\\\','[-1.8ex] 
& \\multicolumn{1}{l}{\\textbf{Aggregated Time Series Features}} &&&
\\\\
& \\multicolumn{1}{l}{\\textbf{From the Previous Quarter*}} &&&
\\\\
1 & \\multicolumn{1}{r}{Measurement Count Across All Parameters}  &', numeric_table$mean[numeric_table$feature == 'previous_measurement_count'], '  & ', numeric_table$range[numeric_table$feature == 'previous_measurement_count'], '  & ', numeric_table$missing[numeric_table$feature == 'previous_measurement_count'], ' 
\\\\','[0.1cm] 
2 & \\makecell[r]{Unweighted Sum of Exceedance Percentages Across \\\\ All Category 1 and 2 Parameters} &', numeric_table$mean[numeric_table$feature == 'previous_true_cat_1and2_exceedence_pct_sum'], '  & ', numeric_table$range[numeric_table$feature == 'previous_true_cat_1and2_exceedence_pct_sum'], '  & ', numeric_table$missing[numeric_table$feature == 'previous_true_cat_1and2_exceedence_pct_sum'], '
\\\\','[0.1cm] 
3 & \\makecell[r]{Weighted Sum of Exceedance Percentages Across \\\\ All Category 1 and 2 Parameters} & ', numeric_table$mean[numeric_table$feature == 'previous_true_cat_1and2_weighted_exceedence_pct_sum'], '  & ', numeric_table$range[numeric_table$feature == 'previous_true_cat_1and2_weighted_exceedence_pct_sum'], '  & ', numeric_table$missing[numeric_table$feature == 'previous_true_cat_1and2_weighted_exceedence_pct_sum'], '
\\\\','[0.1cm] 
4 & \\makecell[r]{Count of All Effluent Violations} & ', numeric_table$mean[numeric_table$feature == 'previous_true_chronic_violation_count'], '  & ', numeric_table$range[numeric_table$feature == 'previous_true_chronic_violation_count'], '  & ', numeric_table$missing[numeric_table$feature == 'previous_true_chronic_violation_count'], '
\\\\','[0.1cm] 
5 & \\makecell[r]{Count of Values that Exceeded 40\\% or 20\\% of  Limit \\\\ Value for Category 1 or 2 Parameters Respectively} & ', numeric_table$mean[numeric_table$feature == 'previous_true_TRC_violation_count'], '  & ', numeric_table$range[numeric_table$feature == 'previous_true_TRC_violation_count'], '  & ', numeric_table$missing[numeric_table$feature == 'previous_true_TRC_violation_count'], '
\\\\
&&&&
\\\\
& \\multicolumn{1}{l}{\\textbf{Predicted Aggregated Time Series Features}} &&&
\\\\
& \\multicolumn{1}{l}{\\textbf{From the Target Quarter*}} &&&
\\\\ 
6 & \\makecell[r]{Measurement Count Across All Parameters} & ', numeric_table$mean[numeric_table$feature == 'target_measurement_count'], '  & ', numeric_table$range[numeric_table$feature == 'target_measurement_count'], '  & ', numeric_table$missing[numeric_table$feature == 'target_measurement_count'], ' 
\\\\','[0.1cm] 
7 & \\makecell[r]{Unweighted Sum of Exceedance Percentages Across \\\\ All Category 1 and 2 Parameters } & ', numeric_table$mean[numeric_table$feature == 'target_prediction_cat_1and2_exceedence_pct_sum'], '  & ', numeric_table$range[numeric_table$feature == 'target_prediction_cat_1and2_exceedence_pct_sum'], '  & ', numeric_table$missing[numeric_table$feature == 'target_prediction_cat_1and2_exceedence_pct_sum'], ' 
\\\\','[0.1cm] 
8 & \\makecell[r]{Weighted Sum of Exceedance Percentages Across \\\\ All Category 1 and 2 Parameters} & ', numeric_table$mean[numeric_table$feature == 'target_prediction_cat_1and2_weighted_exceedence_pct_sum'], '  & ', numeric_table$range[numeric_table$feature == 'target_prediction_cat_1and2_weighted_exceedence_pct_sum'], '  & ', numeric_table$missing[numeric_table$feature == 'target_prediction_cat_1and2_weighted_exceedence_pct_sum'], '  
\\\\','[0.1cm] 
9 & \\makecell[r]{Count of All Effluent Violations} & ', numeric_table$mean[numeric_table$feature == 'target_prediction_chronic_violation_count'], '  & ', numeric_table$range[numeric_table$feature == 'target_prediction_chronic_violation_count'], '  & ', numeric_table$missing[numeric_table$feature == 'target_prediction_chronic_violation_count'], '  
\\\\','[0.1cm] 
10 & \\makecell[r]{Count of Values that Exceeded 40\\% or 20\\% of Limit \\\\ Value for Category 1 or 2 Parameters Respectively} & ', numeric_table$mean[numeric_table$feature == 'target_prediction_TRC_violation_count'], '  & ', numeric_table$range[numeric_table$feature == 'target_prediction_TRC_violation_count'], '  & ', numeric_table$missing[numeric_table$feature == 'target_prediction_TRC_violation_count'], '   
\\\\
&&&&
\\\\
& \\multicolumn{1}{l}{\\textbf{Facility-Level Features}} &&&
\\\\
11 & \\makecell[r]{The Amount of Flow (Million Gallons per Day) \\\\ a permitted facility was designed to accommodate } & ', numeric_table$mean[numeric_table$feature == 'total_design_flow_nmbr'], '  & ', numeric_table$range[numeric_table$feature == 'total_design_flow_nmbr'], '  & ', numeric_table$missing[numeric_table$feature == 'total_design_flow_nmbr'], '   
\\\\','[0.1cm] 
12 & \\makecell[r]{The Amount of Flow (Million Gallons per Day) that \\\\ the facility actually had at the time of application} & ', numeric_table$mean[numeric_table$feature == 'actual_average_flow_nmbr'], '  & ', numeric_table$range[numeric_table$feature == 'actual_average_flow_nmbr'], '  & ', numeric_table$missing[numeric_table$feature == 'actual_average_flow_nmbr'], '   
\\\\
&&&&
\\\\
\\multicolumn{5}{c}{\\textbf{\\large{Categorical Features}}}
\\\\
\\\\','[-1.8ex]\\hline 
\\hline \\\\','[-1.8ex] 
 & \\textbf{Feature} & \\textbf{Category Count} & \\textbf{Mode (\\%)} & \\textbf{Missing Count (\\%)} \\\\ 
\\hline \\\\','[-1.8ex] 
& \\multicolumn{1}{l}{\\textbf{Permit-Level Features}}
\\\\
13 & \\makecell[r]{Facility Type (POTW, Non-POTW, or Federal Entity)} & ', cat_table$count[cat_table$feature == 'facility_type_indicator'], '  & \\makecell[c]{', cat_table$mode[cat_table$feature == 'facility_type_indicator'], '}  & ', cat_table$missing[cat_table$feature == 'facility_type_indicator'], '  
\\\\','[0.1cm] 
14 & \\makecell[r]{Individual or General Permit} & ', cat_table$count[cat_table$feature == 'individual_permit_flag'], '  & \\makecell[c]{', cat_table$mode[cat_table$feature == 'individual_permit_flag'], '}  & ', cat_table$missing[cat_table$feature == 'individual_permit_flag'], '  
\\\\','[0.1cm] 
15 & \\makecell[r]{Major or Minor Permit} & ', cat_table$count[cat_table$feature == 'major_minor_status_flag'], '  & \\makecell[c]{', cat_table$mode[cat_table$feature == 'major_minor_status_flag'], '}  & ', cat_table$missing[cat_table$feature == 'major_minor_status_flag'], '  
\\\\','[0.1cm] 
16 & \\makecell[r]{Wastewater or Non-Wastewater Permit} & ', cat_table$count[cat_table$feature == 'wastewater_permit_flag'], '  & \\makecell[c]{', cat_table$mode[cat_table$feature == 'wastewater_permit_flag'], '}  & ', cat_table$missing[cat_table$feature == 'wastewater_permit_flag'], '
\\\\','[0.1cm] 
17 & \\makecell[r]{Sewage  Treatment or Non-Sewage Permit} & ', cat_table$count[cat_table$feature == 'sewage_permit_flag'], '  & \\makecell[c]{', cat_table$mode[cat_table$feature == 'sewage_permit_flag'], '}  & ', cat_table$missing[cat_table$feature == 'sewage_permit_flag'], '
\\\\','[0.1cm] 
18 & \\makecell[r]{Ownership Type of the Facility (e.g. Municipality)} & ', cat_table$count[cat_table$feature == 'facility_type_code'], '  & \\makecell[c]{', cat_table$mode[cat_table$feature == 'facility_type_code'], '}  & ', cat_table$missing[cat_table$feature == 'facility_type_code'], '
\\\\','[0.1cm] 
19 & \\makecell[r]{Category of Water Body that the Permit Discharges to} & ', cat_table$count[cat_table$feature == 'impaired_waters'], '  & \\makecell[c]{', cat_table$mode[cat_table$feature == 'impaired_waters'], '}  & ', cat_table$missing[cat_table$feature == 'impaired_waters'], '
\\\\ 
&&&&
\\\\
& \\multicolumn{1}{l}{\\textbf{Historical Statuses}}
\\\\
20 & \\makecell[r]{The Official Historical Compliance Status \\\\ One Quarter Before the Target Quarter} & ', cat_table$count[cat_table$feature == 'status_1_quarter_before'], '  & \\makecell[c]{', cat_table$mode[cat_table$feature == 'status_1_quarter_before'], '}  & ', cat_table$missing[cat_table$feature == 'status_1_quarter_before'], '
\\\\','[0.1cm] 
\\makecell[c]{21-\\\\27} & 
\\makecell[r]{The Official Historical Statuses \\\\ Two to Eight Quarters Before the Target Quarter**} & ', cat_table$count[cat_table$feature == 'status_1_quarter_before'], ' & - & - 
\\\\
\\hline \\\\','[-1.8ex] 
\\end{tabular} 
\\caption{Summary Statistics of All 27 Prediction Features in the Random Forest Model. *As the target quarter in the test set is FY2020-Q1, the previous quarter refers to FY2019-Q4. In the training dataset (target quarter FY2019-Q4), the previous quarter refers to FY2019-Q3. **The remaining historical status features share similar summary statistics and are thus omitted here.}
  \\label{tab:pred_feature_stats} 
\\end{table*} 
                       
')

write(latex_table, file = file.path(output_dir, paste0('tableS4_prediction_data_summary_', run_time, '.tex')))
