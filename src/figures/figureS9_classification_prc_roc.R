#####
# Generate Figure 9 PR curve and ROC curve for classification in appendix
# Input: 
# output/models/classification
# Output: 
# output/figures/figure9a_pr_curve_<run_time>.pdf
# output/figures/figure9b_roc_curve_<run_time>.pdf
#####

# Note that the figures were generated in the prediction process in src/prediction/2_run_random_forest.R script
# this script redirects the figures to the output/figures folder

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

file.copy(from = file.path(input_dir, 'classification', 'pr_curve.pdf'),
          to = file.path(output_dir, paste0('figure9a_pr_curve_', run_time,'.pdf')))
file.copy(from = file.path(input_dir, 'classification', 'roc_curve.pdf'),
          to = file.path(output_dir, paste0('figure9b_roc_curve_', run_time,'.pdf')))
