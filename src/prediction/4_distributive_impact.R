#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Author: Elinor Benami
# edited by Hongjin (Nicole) Lin
# Project name: SNC Distributive Implications (FaCCT)
# Date Last Updated: # Mon Jan 18 11:23:08 2021 ------------------------------
# R version: 4.0
# Purpose: Evaluate locational changes with predictions at the CBG level
# Input files: 
# 1. Predicted + Actual Effluent from Random Forest Regression and Classification
# 2. CBG level census data

## Notes: 
# Mode link --> https://app.mode.com/editor/reglab/reports/d13f05ac2014
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setwd('~')

pathdata <- file.path('Dropbox\ (Stanford\ Law\ School)', 'snc', 'data')
pathrepo <- rstudioapi::getActiveProject()

source(file.path(pathrepo, 'src', 'prediction', '0_dependencies.R'))
source(file.path(pathrepo, 'src', 'ggsave_latex.R'))

## Add custom themes
clean_chart_clutter <- 
  theme(    
    panel.grid.major = element_blank(),      # Remove panel grid lines
    panel.grid.minor = element_blank(),      # Remove panel grid lines
    panel.background = element_blank(),      # Remove panel background
    axis.text.x = element_text(angle = 0, size = 11, vjust = 0.75), # Rotate x axis label 
    axis.text.y = element_text(angle = 0, size = 11, vjust = 0.75), # Rotate x axis label 
    axis.title.y = element_text(angle = 0, size = 11, vjust = 0.5),      # Rotate y axis so don't have to crank head
    axis.line = element_line(colour = "grey"),       # Add axis line
    legend.position="bottom"  #@,
    # text = element_text(size = 16, family = myFont1)
  ) 

# %%%%%%%%%%%%%%%%%%%%%%%%%%%
# Set global params ----- 
# %%%%%%%%%%%%%%%%%%%%%%%%%%%
# Determine percent of facilities to target -- out of those anticipated to be in SNC
count_pct <- 0.5
prob_threshold <- 0.5*100

run_time <- Sys.time() %>%
  substr(., 1, 20) %>%
  str_replace_all(., ' ', '_') %>%
  str_replace_all(., ':', '-')

# %%%%%%%%%%%%%%%%%%%%%%%%%%%
# Load Data ----- 
# %%%%%%%%%%%%%%%%%%%%%%%%%%%
# 0a. Permit Parameter Data 
context <- read_csv(file.path(pathdata, 'dmr', "data_munge-4_context_permits_params_facs_qncr-2020-09-21.csv")) # 648015 x 19

# 0b. fac-permit_latlong conversion 
fac_permit <- read_csv(file.path(pathdata, 'dmr', 'raw', "fac_permit_convert_25sept2020_post2015.csv")) %>% select(npdes_permit_id, facility_name, facility_uin)

# 3. Geocoded CBG 
demo_cbg <- read_csv(file.path(pathdata, "6_facilities_geocoded_census_attributes_6_Oct_2020.csv")) %>% clean_names() %>% rename(permit_state = state_name)

#1. Reg & Classification Together
predictions <- read_csv(file.path(pathrepo,
                                  'data', 
                                  'prediction', 
                                  'rf_results_v2_2021-01-13_11-51-39.csv')) %>% clean_names() 

# %%%%%%%%%%%%%%%%%%%%%%%%%%%
# Merge Predictions & Demo Data --------------------------------------------
# %%%%%%%%%%%%%%%%%%%%%%%%%%%
predictions_permit_demo <-
  predictions %>% 
  select(-individual_permit_flag) %>%  # removing from here b/c exact same col in merge
  left_join(demo_cbg, by = c("npdes_permit_id")) %>% 
  rename(median_income = median_hh_income,
         pop_total = population) %>% # 35,545
  mutate(prop_nonwhite = prop_nonwhite*100,
         share_ba = share_ba*100,
         prediction_eff_violation_prob = prediction_eff_violation_prob*100)

total_obs <- nrow(predictions_permit_demo); total_obs

kept <- predictions_permit_demo %>% drop_na(median_income, share_ba, prop_nonwhite)
total_remaining <- nrow(kept); total_remaining #29710 (old); newer 37,798; newer 38394

# 1. Determine how many facilities would be needed to reduce SNC rate by half by evaluating how many predicted
predicted_snc <- 
  predictions_permit_demo %>% 
  filter(prediction_eff_violation_prob > prob_threshold # 2144
         #TODO: only to individual permits?
  ) %>% 
  drop_na(median_income, share_ba, prop_nonwhite) #1467 remaining

unique_permits <- n_distinct(predicted_snc$npdes_permit_id); unique_permits # 1532; newer 2735
unique_facs <- n_distinct(predicted_snc$facility_name); unique_facs #1525; newer 2726

target_min <- min(unique_permits, unique_facs) * count_pct 
target <- round(target_min); target

# 2. Select which facilities/permits to target by sorting the overages and risk score -----

# Regression rank ----
predictions_target_volume <-
  predictions_permit_demo %>% 
  drop_na(median_income, share_ba, prop_nonwhite) %>% # dropped 1897 rows (7%); dropped 2628 rows (7%)
  filter(prediction_eff_violation_prob >= prob_threshold #filter to only those that could plausibly be in SNC.
         # individual filter?
  ) %>% 
  arrange(-prediction_cat_1and2_weighted_exceedence_pct_sum) %>% # sorts from highest to lowest
  head(n = target) #731 --> now 931!

# Classification rank ----
predictions_target_class <-
  predictions_permit_demo %>% 
  drop_na(median_income, share_ba, prop_nonwhite) %>% # dropped 1897 rows (7%); dropped 2628 rows (7%)
  filter(prediction_eff_violation_prob > prob_threshold #,
         # individual_permit_flag == 1 
  ) %>% 
  arrange(-prediction_eff_violation_prob) %>% # sorts from highest to lowest
  head(n = target) #731--> now 931!

# 3. Review overlap in targeting between each protocol
predictions_target_demo <-
  predictions_permit_demo %>% 
  mutate(target_class = ifelse(npdes_permit_id %in% predictions_target_class$npdes_permit_id, 1, 0),
         target_reg = ifelse(npdes_permit_id %in% predictions_target_volume$npdes_permit_id, 1, 0))

tab_overlap <- predictions_target_demo %>% tabyl(target_reg, target_class) %>% adorn_totals(); tab_overlap
predictions_target_demo %>% tabyl(target_reg) %>% adorn_totals() 

# calculate number of overlapping facilities
agreement <- tab_overlap %>% filter(target_reg == 1) %>% select(3) %>% pull()
scales::percent(agreement/target)

# --- how many predicted to be in SNC (possible set)
estimated_snc_set <- 
  predictions_permit_demo %>%
  drop_na(median_income, share_ba, prop_nonwhite) %>% # dropped 1897 rows (7%); dropped 2628 rows (7%)
  filter(prediction_eff_violation_prob > prob_threshold#,
         # individual_permit_flag == 1 ) 
  )

n_estimated_snc <- nrow(estimated_snc_set); n_estimated_snc

agreement/n_estimated_snc

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Develop secondary performance measure of percent exceedance versus synthetic SNC rate
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# there's probably a quicker/easier way to do this than what I did below, but gets the job done. 
# future code reviewers -- ideas welcome!

# Classification Model: Volume Performance
class_vol_true <- mean(predictions_target_class$true_cat_1and2_weighted_exceedence_pct_sum) # 132.65

# Regression Model: Volume Performance
reg_vol_true <- mean(predictions_target_volume$true_cat_1and2_weighted_exceedence_pct_sum) # 221.546

# Class Model: Class Performance
class_eff_true <- mean(predictions_target_class$true_eff_violation) #0.96

# Regression Model: Class Performance
reg_eff_true <- mean(predictions_target_volume$true_eff_violation) # 79.16

onerow <- c("Model", "Overage %", "SNC Status")
tworow <- c("Regression", prettyNum(reg_vol_true, digits = 1), scales::percent(reg_eff_true))
threerow <- c("Classification", prettyNum(class_vol_true, digits = 1), scales::percent(class_eff_true))

twobytwo <-
  rbind(onerow, tworow, threerow) %>% 
  as.data.frame() %>% 
  row_to_names(1)

rownames(twobytwo) <- NULL; twobytwo

print(xtable(twobytwo, booktabs = TRUE,
             caption = paste0(" Comparing Model Performance on Substantive Measures:",
                              " The Regression model identifies more permittees with",
                              " large predicted and actual overages, aggregated over all parameters,",
                              " and the classification model identifies more permittees with the effluent SNC status flag."
             ),
             label = "tab:performance_substantive"),
      file=file.path(pathrepo,
                    'output',
                    'tables',
                    paste0('table3_performance_measure_twobytwo_', run_time,'.tex')),
      include.rownames=FALSE
)



# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Run t-tests (av diffs) and ks tests ----
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
all_the_tests <- function(var_of_interest){
  df_x <- predictions_target_demo %>% dplyr::filter(target_class == 1) %>% dplyr::select(!!var_of_interest) %>% pull()
  df_y <- predictions_target_demo %>% dplyr::filter(target_reg == 1) %>% dplyr::select(!!var_of_interest) %>% pull()
  
  wt <- wilcox.test(df_x, df_y) %>% broom::tidy()
  kst <- ks.test(df_x, df_y) %>% broom::tidy()
  tt <- t.test(df_x, df_y ) %>% broom::tidy()
  
  return(list(wt, kst, tt))
}

tests_income <- all_the_tests(quo(median_income)); tests_income
tests_race <- all_the_tests(quo(prop_nonwhite)); tests_race; 
tests_minority <- all_the_tests(quo(fac_percent_minority)); tests_minority
tests_ed <- all_the_tests(quo(share_ba)); tests_ed
tests_popden <- all_the_tests(quo(fac_pop_den)); tests_popden

t.test(predictions_target_class$median_income, predictions_target_volume$median_income)
t.test(predictions_target_class$prop_nonwhite, predictions_target_volume$prop_nonwhite)
t.test(predictions_target_class$share_ba, predictions_target_volume$share_ba)
t.test(predictions_target_class$prop_nonwhite, predictions_target_volume$prop_nonwhite)
t.test(predictions_target_class$fac_pop_den, predictions_target_volume$fac_pop_den)
wilcox.test(predictions_target_class$fac_pop_den, predictions_target_volume$fac_pop_den)

##### QQ Plot ----- 
predictions_target_demo_long <-
  predictions_target_demo %>%
  gather(key = "key", value = "value", target_class, target_reg) %>%
  filter(value == 1) %>% 
  mutate(newkey = ifelse(key == "target_class", "Class", "Reg"))

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Graph Permits Targeted Under Each Scenario ----
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
predictions_target_demo_long %>% 
  ggplot(aes(sample = prop_nonwhite, color = key, shape = key)) +
  stat_qq(alpha = 0.8, position=position_jitter(h=0.1, w=0.1)
  ) +
  clean_chart_clutter +
  theme(plot.title = element_text(size=14)) +
  labs(title = "Share Minority (CBG)",
       x = "Model Distribution",
       y = "Percent")

qqplot_comparison <- function(variable_select, title_text, caption_text = "", subtitle_text = "", 
                              title.size = 10, subtitle.size = 9, transform = FALSE, trans_num = 1000,
                              x_text = 'Classification', y_text = 'Regression'){
  xvar <- enquo(variable_select)
  
  if(transform == TRUE){
    xvec = predictions_target_class %>% select(!!xvar) %>% pull()
    yvec = predictions_target_volume %>% select(!!xvar) %>% pull()
    xvec = xvec/trans_num
    yvec = yvec/trans_num
  } else {
    xvec = predictions_target_class %>% select(!!xvar) %>% pull()
    yvec = predictions_target_volume %>% select(!!xvar) %>% pull()
  }
  
  xylim <- range(c(xvec, yvec))
  
  ggplot() + 
    geom_point(aes(x=sort(xvec), 
                   y=sort(yvec)), alpha = 0.2, size = 0.75) + 
    geom_abline(intercept=0, slope=1) +
    clean_chart_clutter +
    theme(plot.title = element_text(size=title.size),
          plot.subtitle = element_text(size=subtitle.size),
          plot.caption = element_text(hjust = 0)
    ) +
    labs(title = title_text,
         subtitle = subtitle_text,
         caption = caption_text,
         x = x_text,
         y = y_text
    ) +
    coord_fixed(ratio = 1, xlim=xylim, ylim = xylim, expand = TRUE) 
}


income_qqplot <-
  qqplot_comparison(variable_select = median_income, 
                    title_text = paste0("Median Income (CBG)"),
                    transform = TRUE,
                    x_text = 'Classification (1000s)',
                    y_text = 'Regression\n(1000s)',
                    # trans_num = 100,
                    subtitle_text = paste0(
                      "Rank Sum W = ", scales::comma(tests_income[[1]][["statistic"]]), 
                      " (", ifelse(tests_income[[1]][["p.value"]] < 0.01, '<0.01',
                                   round(tests_income[[1]][["p.value"]], 2)), ")",
                      # t-test of means
                      "\nT-Test = ", scales::comma(tests_income[[3]][["estimate"]]), 
                      " (", ifelse(tests_income[[3]][["p.value"]] < 0.01, '<0.01',
                                   round(tests_income[[3]][["p.value"]], 2)), ")",
                      # ks-test of distribution
                      "\nKS Test = ", format(tests_income[[2]][["statistic"]], scientific = FALSE, digits = 1),
                      " (", ifelse(tests_income[[2]][["p.value"]] < 0.01, '<0.01',
                                   round(tests_income[[2]][["p.value"]], 2)), ")")
  ); income_qqplot


race_qqplot <-
  qqplot_comparison(variable_select = prop_nonwhite, 
                    title_text = paste0("Percent Minority (CBG)"),
                    # transform = TRUE,
                    # trans_num = 100,
                    subtitle_text = paste0(
                      "Rank Sum W = ", scales::comma(tests_race[[1]][["statistic"]]), 
                      " (", ifelse(tests_race[[1]][["p.value"]] < 0.01, '<0.01',
                                   round(tests_race[[1]][["p.value"]], 2)), ")",
                      # t-test of means
                      "\nT-Test = ", scales::comma(tests_race[[3]][["estimate"]]), 
                      " (", ifelse(tests_race[[3]][["p.value"]] < 0.01, '<0.01',
                                   round(tests_race[[3]][["p.value"]], 2)), ")",
                      # ks-test of distribution
                      "\nKS Test = ", format(tests_race[[2]][["statistic"]], scientific = FALSE, digits = 1),
                      " (", ifelse(tests_race[[2]][["p.value"]] < 0.01, '<0.01',
                                   round(tests_race[[2]][["p.value"]], 2)), ")")
  ); race_qqplot


xvec = predictions_target_class %>% select(fac_pop_den) %>% pull()
yvec = predictions_target_volume %>% select(fac_pop_den) %>% pull()
xymax <- max(xvec, yvec)
xylim <- range(NA, xymax)

popden_qqplot <-
  ggplot() + 
  geom_point(
    aes(x=sort(xvec), 
        y=sort(yvec)), alpha = 0.2, size= 0.75) + 
  geom_abline(intercept=0, slope=1) +
  clean_chart_clutter +
  labs(title = "Population Density",
       x = 'Classification \n(Log Scale)',
       y = 'Regression \n(Log Scale)',
       subtitle_text = paste0(
         "People/Sq. Mile in a 3 mile radius",
         "\nRank Sum W = ", scales::comma(tests_popden[[1]][["statistic"]]), 
         " (", ifelse(tests_popden[[1]][["p.value"]] < 0.01, '<0.01', 
                      round(tests_popden[[1]][["p.value"]], 2)), ")",
         # t-test of means
         "\nT-Test = ", scales::comma(tests_popden[[3]][["estimate"]]), 
         " (", ifelse(tests_popden[[3]][["p.value"]] < 0.01, '<0.01', 
                      round(tests_popden[[3]][["p.value"]], 2)), ")",
         # ks-test of distribution
         "\nKS Test = ", format(tests_popden[[2]][["statistic"]], scientific = TRUE, digits = 2),
         " (", ifelse(tests_popden[[2]][["p.value"]] < 0.01, '<0.01',
                      round(tests_popden[[2]][["p.value"]], 2)), ")")
  ) +
  # scale_x_continuous(trans="log10", limits=c(NA,xymax)) +
  # scale_y_continuous(trans="log10", limits=c(NA,xymax)); popden_qqplot
  scale_x_log10() +
  scale_y_log10() +
  theme(
    plot.title = element_text(size = 10),
    plot.subtitle = element_text(size = 9),
    plot.caption = element_text(hjust = 0),
    axis.text.x = element_text(angle = 45, hjust = 1)
  ) +
  coord_fixed(ratio = 1, xlim = xylim, ylim = xylim); popden_qqplot

note_text <- paste0("\nTargeting 50% of permits sorted by", #in the highest probability of SNC",
                    #"\nThreshold for Predicted SNC is above ", prob_threshold * 100, "% likelihood of SNC",
                    # "\nand highest predicted exceedence pct aggregated across all possible parameters for Q1 2020",
                    " predicted exceedence percent \nAggregated",
                    " across all SNC-eligible parameters for FY2020-Q1",
                    #                    " across all possible parameters for FY2020-Q1",
                    #" (and linearized)", 
                    "\nUnique permittees = ", scales::comma(total_remaining),
                    "; Targeted permits = ", scales::comma(target)
                    # "\nUnique permits = ", scales::comma(n_distinct(predictions_target_demo$npdes_permit_id)),
                    # "; Targeted permits = ", scales::comma(target)
)

# library(egg)
compare_qqplot <- 
  ggarrange(race_qqplot, #minority_qqplot,
            income_qqplot, #ed_qqplot, 
            popden_qqplot,
            ncol = 1, nrow = 3#,  
            # common.legend = TRUE,
            # legend = "bottom"
  ) #%>% 

compare_qqplot


# #%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# ## Oracle Version of QQ Plot------------------------------------------ 
# #%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# 2. Select which facilities/permits to target by sorting the overages and risk score -----

# Regression rank ----
oracle_target_volume <-
  predictions_permit_demo %>% 
  drop_na(median_income, share_ba, prop_nonwhite) %>% # dropped 1897 rows (7%); dropped 2628 rows (7%)
  # filter(true_historical_esnc == TRUE
  filter(true_eff_violation == TRUE
         #prediction_eff_violation_prob >= prob_threshold #filter to only those that could plausibly be in SNC.
         # individual filter?
  ) %>% 
  arrange(-true_cat_1and2_weighted_exceedence_pct_sum) %>% # sorts from highest to lowest
  head(n = target) 

# Classification rank ----
oracle_target_class <-
  predictions_permit_demo %>% 
  drop_na(median_income, share_ba, prop_nonwhite) %>% # dropped 1897 rows (7%); dropped 2628 rows (7%)
  #filter(true_historical_esnc == TRUE
  filter(true_eff_violation == TRUE
         # filter(prediction_eff_violation_prob > prob_threshold #,
         # individual_permit_flag == 1 
  ) %>% 
  arrange(true_cat_1and2_weighted_exceedence_pct_sum) %>% # sorts from lowest to highest (``Pathologically the lowest")
  head(n = target)


# 3. Review overlap in targeting between each protocol
oracle_target_demo <-
  predictions_permit_demo %>% 
  mutate(target_class = ifelse(npdes_permit_id %in% oracle_target_class$npdes_permit_id, 1, 0),
         target_reg = ifelse(npdes_permit_id %in% oracle_target_volume$npdes_permit_id, 1, 0))

oracle_target_demo %>% tabyl(target_reg, target_class) %>% adorn_totals() 
oracle_target_demo %>% tabyl(target_reg) %>% adorn_totals() 

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# E. Run t-tests (av diffs) and ks tests ----
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
all_the_tests_oracle <- function(var_of_interest){
  df_x <- oracle_target_demo %>% dplyr::filter(target_class == 1) %>% dplyr::select(!!var_of_interest) %>% pull()
  df_y <- oracle_target_demo %>% dplyr::filter(target_reg == 1) %>% dplyr::select(!!var_of_interest) %>% pull()
  
  wt <- wilcox.test(df_x, df_y) %>% broom::tidy()
  kst <- ks.test(df_x, df_y) %>% broom::tidy()
  tt <- t.test(df_x, df_y ) %>% broom::tidy()
  
  return(list(wt, kst, tt))
}

oracle_tests_income <- all_the_tests_oracle(quo(median_income)); oracle_tests_income
oracle_tests_race <- all_the_tests_oracle(quo(prop_nonwhite)); oracle_tests_race
oracle_tests_ed <- all_the_tests_oracle(quo(share_ba)); oracle_tests_ed
oracle_tests_popden <- all_the_tests_oracle(quo(fac_pop_den)); oracle_tests_popden


t.test(oracle_target_class$median_income, oracle_target_volume$median_income)
t.test(oracle_target_class$prop_nonwhite, oracle_target_volume$prop_nonwhite)
t.test(oracle_target_class$share_ba, oracle_target_volume$share_ba)


oracle_target_demo_long <-
  oracle_target_demo %>%
  gather(key = "key", value = "value", target_class, target_reg) %>%
  filter(value == 1) %>% 
  mutate(newkey = ifelse(key == "target_class", "Class", "Reg"))


qqplot_comparison_oracle <- function(variable_select, title_text, caption_text = "", subtitle_text = "", 
                                     title.size = 10, subtitle.size = 9, transform = FALSE, trans_num = 1000,
                                     x_text = 'Oracle Classification', y_text = 'Oracle\nRegression'){
  xvar <- enquo(variable_select)
  
  if(transform == TRUE){
    xvec = oracle_target_class %>% select(!!xvar) %>% pull()
    yvec = oracle_target_volume %>% select(!!xvar) %>% pull()
    xvec = xvec/trans_num
    yvec = yvec/trans_num
  } else {
    xvec = oracle_target_class %>% select(!!xvar) %>% pull()
    yvec = oracle_target_volume %>% select(!!xvar) %>% pull()
  }
  
  xylim <- range(c(xvec, yvec))
  
  ggplot() + 
    geom_point(aes(x=sort(xvec), 
                   y=sort(yvec)), alpha = 0.2, size = 0.75) + 
    geom_abline(intercept=0, slope=1) +
    clean_chart_clutter +
    theme(plot.title = element_text(size=title.size),
          plot.subtitle = element_text(size=subtitle.size),
          plot.caption = element_text(hjust = 0)
    ) +
    labs(title = title_text,
         subtitle = subtitle_text,
         caption = caption_text,
         x = x_text,
         y = y_text
    ) +
    coord_fixed(ratio = 1, xlim=xylim, ylim = xylim, expand = TRUE) 
}

oracle_income_qqplot <-
  qqplot_comparison_oracle(variable_select = median_income, 
                           title_text = paste0("Median Income (CBG)"),
                           transform = TRUE,
                           x_text = 'Oracle Classification (1000s)',
                           y_text = 'Oracle\nRegression\n(1000s)',
                           # trans_num = 100,
                           subtitle_text = paste0(
                             "Rank Sum W = ", scales::comma(oracle_tests_income[[1]][["statistic"]]), 
                             " (", ifelse(oracle_tests_income[[1]][["p.value"]] < 0.01, '<0.01',
                                          round(oracle_tests_income[[1]][["p.value"]], 2)), ")",
                             # t-test of means
                             "\nT-Test = ", scales::comma(oracle_tests_income[[3]][["estimate"]]), 
                             " (", ifelse(oracle_tests_income[[3]][["p.value"]] < 0.01, '<0.01',
                                          round(oracle_tests_income[[3]][["p.value"]], 2)), ")",
                             # ks-test of distribution
                             "\nKS Test = ", format(oracle_tests_income[[2]][["statistic"]], scientific = FALSE, digits = 1),
                             " (", ifelse(oracle_tests_income[[2]][["p.value"]] < 0.01, '<0.01',
                                          round(oracle_tests_income[[2]][["p.value"]], 2)), ")")
  ); oracle_income_qqplot


oracle_race_qqplot <-
  qqplot_comparison_oracle(variable_select = prop_nonwhite, 
                           title_text = paste0("Percent Minority (CBG)"),
                           # transform = TRUE,
                           # trans_num = 100,
                           subtitle_text = paste0(
                             "Rank Sum W = ", scales::comma(oracle_tests_race[[1]][["statistic"]]), 
                             " (", ifelse(oracle_tests_race[[1]][["p.value"]] < 0.01, '<0.01',
                                          round(oracle_tests_race[[1]][["p.value"]], 2)), ")",
                             # t-test of means
                             "\nT-Test = ", scales::comma(oracle_tests_race[[3]][["estimate"]]), 
                             " (",  ifelse(oracle_tests_race[[3]][["p.value"]] < 0.01, '<0.01',
                                           round(oracle_tests_race[[3]][["p.value"]], 2)), ")",
                             # ks-test of distribution
                             "\nKS Test = ", format(oracle_tests_race[[2]][["statistic"]], scientific = FALSE, digits = 1),
                             " (",  ifelse(oracle_tests_race[[2]][["p.value"]] < 0.01, '<0.01',
                                           round(oracle_tests_race[[2]][["p.value"]], 2)), ")")
  ); oracle_race_qqplot


xvec_oracle = oracle_target_class %>% select(fac_pop_den) %>% pull()
yvec_oracle = oracle_target_volume %>% select(fac_pop_den) %>% pull()
xymax_oracle <- max(xvec_oracle, yvec_oracle)
xylim_oracle <- range(NA, xymax_oracle)

oracle_popden_qqplot <-
  ggplot() + 
  geom_point(
    aes(x=sort(xvec_oracle), 
        y=sort(yvec_oracle)), alpha = 0.2, size= 0.75) + 
  geom_abline(intercept=0, slope=1) +
  clean_chart_clutter +
  labs(title = "Population Density",
       x = 'Oracle Classification \n(Log Scale)',
       y = 'Oracle\nRegression \n(Log Scale)',
       subtitle_text = paste0(
         "People/Sq. Mile in a 3 mile radius",
         "\nRank Sum W = ", scales::comma(oracle_tests_popden[[1]][["statistic"]]), 
         " (",  ifelse(oracle_tests_popden[[1]][["p.value"]] < 0.01, '<0.01',
                       round(oracle_tests_popden[[1]][["p.value"]], 2)), ")",
         # t-test of means
         "\nT-Test = ", scales::comma(oracle_tests_popden[[3]][["estimate"]]), 
         " (", ifelse(oracle_tests_popden[[3]][["p.value"]] < 0.01, '<0.01',
                      round(oracle_tests_popden[[3]][["p.value"]], 2)), ")",
         # ks-test of distribution
         "\nKS Test = ", format(oracle_tests_popden[[2]][["statistic"]], scientific = FALSE, digits = 1),
         " (", ifelse(oracle_tests_popden[[2]][["p.value"]] < 0.01, '<0.01',
                      round(oracle_tests_popden[[2]][["p.value"]], 2)), ")")
  ) +
  # scale_x_continuous(trans="log10", limits=c(NA,xymax)) +
  # scale_y_continuous(trans="log10", limits=c(NA,xymax)); popden_qqplot
  scale_x_log10() +
  scale_y_log10() +
  theme(
    plot.title = element_text(size = 10),
    plot.subtitle = element_text(size = 9),
    plot.caption = element_text(hjust = 0),
    axis.text.x = element_text(angle = 45, hjust = 1)
  ) +
  coord_fixed(ratio = 1, xlim = xylim_oracle, ylim = xylim_oracle); oracle_popden_qqplot


### Bring all the graphics together in a 2 x 3 ----

compare_qqplot_all <- 
  ggarrange(oracle_race_qqplot, race_qqplot,
            oracle_income_qqplot, income_qqplot, 
            oracle_popden_qqplot, popden_qqplot,
            ncol = 2, nrow = 3
            # common.legend = TRUE,
            # legend = "bottom"
  )
compare_qqplot_all

ggsave(file.path(pathrepo,
                 'output', 
                 'figures',
                 paste0('figure6_distributive_impact_', run_time, '.pdf')),
       height = 11, width = 10)


# summary stats ----------------------------------------------------------

## Oracle 
targeted_only <-
  oracle_target_demo %>% 
  filter(target_class == 1|target_reg == 1)

oracle_target_volume %>%
       select(
         prop_nonwhite, median_income,fac_pop_den,
         ejscreen_flag_us
       ) %>% 
       dfSummary(graph.magnif = 0.75, valid.col = FALSE)


oracle_target_class %>%
       select(
         prop_nonwhite, median_income,fac_pop_den,
         ejscreen_flag_us
       ) %>% 
       dfSummary(graph.magnif = 0.75, valid.col = FALSE)

## predictions

targeted_only <-
  predictions_target_demo %>% 
  filter(target_class == 1|target_reg == 1)

predictions_target_volume %>%
  select(
    prop_nonwhite, median_income,fac_pop_den,
    ejscreen_flag_us
  ) %>% 
  dfSummary(graph.magnif = 0.75, valid.col = FALSE)


predictions_target_class %>%
  select(
    prop_nonwhite, median_income,fac_pop_den,
    ejscreen_flag_us
  ) %>% 
  dfSummary(graph.magnif = 0.75, valid.col = FALSE)


