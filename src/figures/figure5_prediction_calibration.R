#####
# Generate Figure 5 Calibration Plots for Classification and Regression Models
# Input:
# output/models/classification
# output/models/regression
# Output: Calibration Plots
# output/figures/figure5a_classification_calibration_<run_time>.pdf
# output/figures/figure5b_regression_calibration_<run_time>.pdf
#####

# Note that the figures were generated in the prediction process in src/prediction/2_run_random_forest.R script
# this script redirects the figures to the figures folder

# set up ------------------------------------------------------------------

# set repo dir regvcalss as the working directory
repo_dir <- rstudioapi::getActiveProject()
setwd(repo_dir) 

input_dir <- file.path(repo_dir, 'output', 'models')
output_dir <- file.path(repo_dir, 'output', 'figures')

run_time <- Sys.time() %>%
  substr(., 1, 20) %>%
  str_replace_all(., ' ', '_') %>%
  str_replace_all(., ':', '-')

# move figures ------------------------------------------------------------

file.copy(from = file.path(input_dir, 'classification', 'class_calibration.pdf'),
          to = file.path(output_dir, paste0('figure5a_classification_calibration_', run_time,'.pdf')))
file.copy(from = file.path(input_dir, 'regression', 'reg_calibration_diff.pdf'),
          to = file.path(output_dir, paste0('figure5b_regression_calibration_', run_time,'.pdf')))
