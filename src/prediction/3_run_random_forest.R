
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Project name: SNC Distributive Implications
# Purpose: Generate list of high risk facilities using Classification vs Regression and performance
# author: Hongjin (Nicole) Lin, RegLab, Stanford Law School (hongjinl@law.stanford.edu)
# adapted from SNC_Prediction.R and mode report by Reid and Elinor (link to mode report https://app.mode.com/editor/reglab/reports/d13f05ac2014)

# Instruction:
# To run the script, you must have access to RegLab's AWS database. 
# 1. clone the repo to your local machine 
# 2. direct to the repo directory
# 3. enter Rscript scr/prediction/2_run_random_forest.R in the command line

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


# set up  -----------------------------------------------------------------

# # set repo dir regvcalss as the working directory when running in rstudio
repo_dir <- rstudioapi::getActiveProject()
setwd(repo_dir)

source(file.path(repo_dir, 'src', 'prediction', '0_dependencies.R'))

# enable parallel processing
cores <- floor(0.6*detectCores())
cl <- makeCluster(cores)
registerDoParallel(cores)
getDoParWorkers()

# make and set model output directories
data_dir <- file.path('data', 'prediction')
output_dir <- file.path('output', 'models')
class_dir <- file.path(output_dir, 'classification')
reg_dir <- file.path(output_dir, 'regression')
sapply(c(data_dir, output_dir, class_dir, reg_dir),
       function(dir){
         if (!dir.exists(dir)){
           dir.create(dir)
         }
       })

# get timestamp for results files
run_time <- Sys.time() %>%
  substr(., 1, 20) %>%
  str_replace_all(., ' ', '_') %>%
  str_replace_all(., ':', '-')

# database connection credentials 
# for instructions on how to save credentials as environment variables: https://asconfluence.stanford.edu/confluence/display/REGLAB/PostgreSQL+Database#PostgreSQLDatabase-ManagingUsersandRoles
db_name <- Sys.getenv('EPA_DB_NAME')
db_host <- Sys.getenv('EPA_DB_HOST')
db_port <- Sys.getenv('EPA_DB_PORT')
db_username <- Sys.getenv('EPA_DB_USER')
db_password <- Sys.getenv('EPA_DB_PASSWORD')

table_name <- 'arima_forecasts_snc_dist'

# connect to the AWS database
con <- dbConnect(RPostgres::Postgres(),
                 dbname=db_name,
                 host=db_host,
                 port=db_port ,
                 user=db_username,
                 password=db_password)

## set gloabl variables
# target quarter: 20201 
target_quarter <- "20201"
# true values from the previous quarter will be used
prediction_timepoints <- c("2019-07-31", "2019-08-31", "2019-09-30", "2019-10-31", "2019-11-30", "2019-12-31")

# use for classification training
previous_quarter <- "20194"
previous_prediction_timepoints <- c("2019-04-30", "2019-05-31", "2019-06-30", "2019-07-31", "2019-08-31", "2019-09-30")

# compliance history observation window
history_window_length <- 2 # years
history_window_start <- as.character(as.numeric(target_quarter) - history_window_length*10)
train_history_window_start <- as.character(as.numeric(previous_quarter) - history_window_length*10)

# series where an overage is exceeding the threshold
limit_value_qualifier_code <- c('<', '<=')

# colors for plotting
color1 <- '#E6945C'
color2 <- '#5CA7E6'

# random seed
seed <- 333

message(paste0("======= Generating prediction output for study design: ", design_id, "========="))

# read data from AWS database ----------------------------------------------

# 1. forecasts data for the target quarter and the previous quarter
read_forecasts <- function(quarter){
  
  if (quarter == 'target'){
    query_prediction_timepoints <- prediction_timepoints
  } else if (quarter == 'train'){
    query_prediction_timepoints <- previous_prediction_timepoints
  }
  
  forecasts_query <- paste0("
                            WITH sub_permits AS(
                             SELECT DISTINCT npdes_permit_id
                             FROM icis.permits
                             WHERE latest_version_flag
                            ),
                            
                            monthly_reporters AS(
                             SELECT DISTINCT npdes_permit_id
                             FROM icis.dmr_submissions 
                             WHERE monitoring_period_end_date in (", paste(paste0("'",query_prediction_timepoints,"'"), collapse=','), ")
                             AND nmbr_of_submission_desc = 'monthly'
                            ),
                            
                            parameters AS (
                              SELECT DISTINCT parameter_code,
                               CASE WHEN snc_flag IS NULL THEN '3' 
                                   ELSE snc_flag END AS snc_flag
                              FROM ontologies.icis__ref_parameter
                            )
                            
                            SELECT *
                            FROM model_outputs.", table_name,"
                            INNER JOIN sub_permits
                            USING (npdes_permit_id)
                            LEFT JOIN parameters
                            USING (parameter_code)
                            WHERE prediction_timepoint in (", paste(paste0("'",query_prediction_timepoints,"'"), collapse=','), ")
                            AND model_status_code in ('FH')
                            AND true_value is not NULL
                            AND limit_value_qualifier_code in (", paste(paste0("'",limit_value_qualifier_code,"'"), collapse=','), ")"
            )
  df <- dbSendQuery(con, forecasts_query) %>%
    dbFetch() %>%
    arrange(series_id, prediction_timepoint) %>%
    # convert exceedence < 0 to 0
    mutate(prediction_exceedence_pct = ifelse(prediction_exceedence_pct > 0, prediction_exceedence_pct, 0)) %>%
    mutate(year_quarter = quarter(prediction_timepoint, with_year = TRUE, fiscal_start = 10)) %>%
    mutate(year_quarter = str_remove(as.character(year_quarter), '[.]'))
  
  return(df)
}

forecasts <- read_forecasts('target')
table(forecasts$prediction_timepoint)
## check duplicates - no duplicates if # of rows in tmp and # of rows in forecasts are the same
tmp <- forecasts %>%
  distinct(series_id, npdes_permit_id, prediction_timepoint)
nrow(tmp) == nrow(forecasts)

train_forecasts <- read_forecasts('train')
table(train_forecasts$prediction_timepoint)
## check duplicates - no duplicates
tmp <- train_forecasts %>%
  distinct(series_id, npdes_permit_id, prediction_timepoint)
nrow(tmp) == nrow(train_forecasts)

# 2. permit level fixed variables
# only take in permits that had submitted DMR effluent records in the desinated period
permit_query <- paste0("
                        WITH parameters AS (
                          SELECT DISTINCT parameter_code,snc_flag
                          FROM ontologies.icis__ref_parameter
                        ),
                        
                        prediction_sample AS (
                         SELECT DISTINCT npdes_permit_id
                         FROM model_outputs.", table_name,"
                         LEFT JOIN parameters
                         USING (parameter_code)
                         WHERE prediction_timepoint in (", paste(paste0("'",prediction_timepoints,"'"), collapse=','), ")
                         AND model_status_code in ('FH')
                         AND true_value is NOT NULL
                         AND limit_value_qualifier_code in (", paste(paste0("'",limit_value_qualifier_code,"'"), collapse=','), ")
                        ),
                        
                        monthly_reporters AS (
                         SELECT DISTINCT npdes_permit_id
                         FROM icis.dmr_submissions 
                         WHERE monitoring_period_end_date in (", paste(paste0("'",prediction_timepoints,"'"), collapse=','), ")
                         AND nmbr_of_submission_desc = 'monthly'
                        ),
                        
                        permits AS (
                         SELECT 
                           npdes_permit_id, 
                           facility_type_indicator, 
                           individual_permit_flag, 
                           major_minor_status_flag, 
                           total_design_flow_nmbr,
                           actual_average_flow_nmbr,
                           wastewater_permit_flag,
                           sewage_permit_flag
                         FROM icis.permits
                         WHERE latest_version_flag
                        ), 
                        
                        facilities AS (
                         SELECT 
                          npdes_permit_id, 
                          facility_type_code,
                          impaired_waters
                         FROM icis.facilities
                        )
                        
                        SELECT *
                        FROM prediction_sample 
                        INNER JOIN permits
                        USING (npdes_permit_id)
                        LEFT JOIN facilities
                        USING (npdes_permit_id);
")

permit <- dbSendQuery(con, permit_query) %>%
  dbFetch() %>%
  arrange(npdes_permit_id)

# 3. historical status
# only keep permits that had historical statuses in the targeted quarter
read_history <- function(quarter){
  
  if (quarter == 'target'){
    query_prediction_timepoints <- prediction_timepoints
    query_history_window_start <- history_window_start
    query_quarter <- target_quarter
  } else if (quarter == 'train'){
    query_prediction_timepoints <- previous_prediction_timepoints
    query_history_window_start <- train_history_window_start
    query_quarter <- previous_quarter
  }
  
  query <- paste0("
                    WITH parameters AS (
                      SELECT DISTINCT parameter_code, snc_flag
                      FROM ontologies.icis__ref_parameter
                    ),

                    prediction_sample AS (
                     SELECT DISTINCT npdes_permit_id
                     FROM model_outputs.", table_name,"
                     LEFT JOIN parameters
                     USING (parameter_code)
                     WHERE prediction_timepoint in (", paste(paste0("'",query_prediction_timepoints,"'"), collapse=','), ")
                     AND model_status_code in ('FH')
                     AND true_value is NOT NULL
                     AND limit_value_qualifier_code in (", paste(paste0("'",limit_value_qualifier_code,"'"), collapse=','), ")
                    ),
                     
                    sub_permits AS(
                     SELECT DISTINCT npdes_permit_id
                     FROM icis.permits
                     WHERE latest_version_flag
                    ),
                    
                    monthly_reporters AS(
                     SELECT DISTINCT npdes_permit_id
                     FROM icis.dmr_submissions 
                     WHERE monitoring_period_end_date in (", paste(paste0("'",query_prediction_timepoints,"'"), collapse=','), ")
                     AND nmbr_of_submission_desc = 'monthly'
                    ),
                    
                    history AS (
                    SELECT 
                     npdes_permit_id,
                     fiscal_quarter,
                     hlrnc
                     FROM icis.npdes_latest_reported_qncr_history
                     WHERE fiscal_quarter >= '", query_history_window_start, "' AND fiscal_quarter <= '", query_quarter, "'
                    )
                    
                    SELECT *
                    FROM prediction_sample
                    INNER JOIN sub_permits
                    USING (npdes_permit_id)
                    LEFT JOIN history
                    USING (npdes_permit_id);
                    ")
  
  df <- dbSendQuery(con, query) %>%
    dbFetch() %>%
    # there are about 800 permits that are not present in the historical status table
    # they might be newly added permits in 2020
    # we will remove them for now
    filter(!is.na(fiscal_quarter)) %>%
    spread(fiscal_quarter, hlrnc) %>%
    # arrange and rename quarters
    select(sort(names(.), decreasing = TRUE)) %>%
    rename_at(vars(starts_with('20')), function(x) paste0('status_', seq(0, 4*history_window_length, 1), '_quarter_before')) %>%
    rename(true_qncr_status = status_0_quarter_before) %>%
    # true label used for classification
    mutate(true_qncr_esnc = true_qncr_status %in% c('E', 'X')) %>%
    mutate(year_quarter = query_quarter)
  
  return(df)
}

history <- read_history('target')
tmp <- history %>%
  distinct(npdes_permit_id, year_quarter)
nrow(tmp) == nrow(history)
table(history$year_quarter)

train_history <- read_history('train')
tmp <- train_history %>%
  distinct(npdes_permit_id, year_quarter)
nrow(tmp) == nrow(train_history)
table(train_history$year_quarter)

# Preprosessing -----------------------------------------------------------
### Final version:
## 1) limit to only group I/II parameters: these are the pollutants that count towards SNC
## 2) remove known data errors: records with exceedence percentage = 2147483650 or 99999
## 3) winsorize at 99998: caping ~0.3% of values

original_forecasts <- forecasts %>%
  bind_rows(train_forecasts, .) %>%
  distinct()


treated_forecasts <- original_forecasts %>%
  filter(snc_flag %in% c(1,2)) %>%
  filter(!(true_exceedence_pct %in% c(2147483650, 99999))) %>%
  mutate(prediction_violation_flag = ((limit_value_qualifier_code == '<=') & (prediction_value > limit_value_standard_units)) |
           ((limit_value_qualifier_code == '<') & (prediction_value >= limit_value_standard_units))
  ) %>%
  # calculate predicted exceedence percentage based on limit value qualifiers and limit values
  # if limit value = 0, then the exceedence pct is predicted - limit (when qualifier is <=, <, or =, which are 99.9977% of the case)
  mutate(prediction_exceedence_pct = (prediction_value - limit_value_standard_units)/limit_value_standard_units) %>%
  mutate(prediction_exceedence_pct = 100*prediction_exceedence_pct) %>%
  # replace negative exceedence pct with NA 
  mutate(prediction_exceedence_pct = ifelse(prediction_violation_flag, prediction_exceedence_pct, NA)) %>%
  mutate(true_exceedence_pct = replace_na(true_exceedence_pct, 0),
         prediction_exceedence_pct = replace_na(prediction_exceedence_pct, 0))
  
  
# ~0.3% of values with over 99998 exceedance percentages
true_cap <- 99998
pred_cap <- 99998

treated_forecasts <- treated_forecasts %>%
  mutate(true_exceedence_pct = ifelse(true_exceedence_pct > true_cap, true_cap, true_exceedence_pct),
         prediction_exceedence_pct = ifelse(prediction_exceedence_pct > pred_cap, pred_cap, prediction_exceedence_pct))

# records removed:
nrow(original_forecasts) - nrow(treated_forecasts) 
length(unique(original_forecasts$npdes_permit_id)) - length(unique(treated_forecasts$npdes_permit_id)) 

# time-series level confusion matrix
table(treated_forecasts$true_violation_flag, treated_forecasts$prediction_violation_flag)

# arima rmse
rmse(treated_forecasts$true_value, treated_forecasts$prediction_value)
rmse(treated_forecasts$true_value, treated_forecasts$imputed_mean)

# Build prediction data ---------------------------------------------------

# 1. construct outcome variables
## 1) classification: constructed esnc status
## a) for target quarter
true_class_labels_target <- treated_forecasts %>%
  # filter to true values in 2019Q4 and 2020Q1
  filter(year_quarter %in% c(previous_quarter, target_quarter)) %>%
  # in practice it is parameter, location, monthly/ nonmonthly, monitoring period end date
  group_by(npdes_permit_id, parameter_code, snc_flag, prediction_timepoint) %>%
  summarise(true_TRC_violation_count = sum((snc_flag == 1 & true_exceedence_pct > 40) |
                                             (snc_flag == 2 & true_exceedence_pct > 20), na.rm = TRUE),
            true_chronic_violation_count = sum(true_exceedence_pct > 0, na.rm = TRUE)) %>%
  mutate(true_esnc_flag = true_TRC_violation_count >= 2 | true_chronic_violation_count >= 4) %>%
  group_by(npdes_permit_id) %>%
  summarise(true_synthetic_esnc = sum(true_esnc_flag, na.rm = TRUE) > 0) %>%
  mutate(year_quarter = target_quarter)
# number of synthetic esnc
sum(true_class_labels_target$true_synthetic_esnc)
# number of historical esnc
sum(history$true_qncr_esnc)
## synthetic esnc more than historical esnc

## b) for training quarter
true_class_labels_train <- treated_forecasts %>%
  # filter to true values in 2019Q3 and target 2019Q4
  filter(year_quarter %in% c('20193', previous_quarter)) %>%
  # in practice it is parameter, location, monthly/ nonmonthly, monitoring period end date
  group_by(npdes_permit_id, parameter_code, snc_flag, prediction_timepoint) %>%
  summarise(true_TRC_violation_count = sum((snc_flag == 1 & true_exceedence_pct > 40) |
                                             (snc_flag == 2 & true_exceedence_pct > 20), na.rm = TRUE),
            true_chronic_violation_count = sum(true_exceedence_pct > 0, na.rm = TRUE)) %>%
  mutate(true_esnc_flag = true_TRC_violation_count >= 2 | true_chronic_violation_count >= 4) %>%
  group_by(npdes_permit_id) %>%
  summarise(true_synthetic_esnc = sum(true_esnc_flag, na.rm = TRUE) > 0) %>%
  mutate(year_quarter = previous_quarter)
sum(true_class_labels_train$true_synthetic_esnc)
sum(train_history$true_qncr_esnc)

true_class_labels <- bind_rows(true_class_labels_target, true_class_labels_train)

## 2) for regression: weighted exceedence pct sum for cat 1 and 2 pollutants
true_reg_values <- treated_forecasts %>%
  group_by(npdes_permit_id, year_quarter) %>%
  # weighted sums of exceedence pct
  summarise(cat_1and2_true_weighted_exceedence_pct_sum = sum(ifelse(snc_flag == 1,
                                                          true_exceedence_pct/40,
                                                          true_exceedence_pct/20), na.rm = TRUE),
            cat_1and2_true_exceedence_pct_sum = sum(true_exceedence_pct, na.rm = TRUE)) 

# 2. forecast data
# arima forecasts aggregated data 
tmp <- treated_forecasts %>%
  group_by(npdes_permit_id, year_quarter) %>%
  summarise(measurement_count = n(),
            true_TRC_violation_count = sum((snc_flag == 1 & true_exceedence_pct > 40) |
                                             (snc_flag == 2 & true_exceedence_pct > 20), na.rm = TRUE),
            true_chronic_violation_count = sum(true_exceedence_pct > 0, na.rm = TRUE),
            prediction_TRC_violation_count = sum((snc_flag == 1 & prediction_exceedence_pct > 40) |
                                                   (snc_flag == 2 & prediction_exceedence_pct > 20), na.rm = TRUE),
            prediction_chronic_violation_count = sum(prediction_exceedence_pct > 0, na.rm = TRUE),
            true_cat_1and2_exceedence_pct_sum = sum(true_exceedence_pct, na.rm = TRUE),
            true_cat_1and2_weighted_exceedence_pct_sum = sum(ifelse(snc_flag == 1, 
                                                                   true_exceedence_pct/40,
                                                                   true_exceedence_pct/20), na.rm = TRUE),
            prediction_cat_1and2_exceedence_pct_sum = sum(prediction_exceedence_pct, na.rm = TRUE),
            prediction_cat_1and2_weighted_exceedence_pct_sum = sum(ifelse(snc_flag == 1, 
                                                                          prediction_exceedence_pct/40,
                                                                          prediction_exceedence_pct/20), na.rm = TRUE)) 

# true for 20194 and prediction for 20201
forecasts_data_target <- tmp %>%
  filter(year_quarter %in% c(previous_quarter, target_quarter)) %>%
  mutate(year_quarter = ifelse(year_quarter == previous_quarter, 'previous', 'target')) %>%
  # add previous or target prefix to the column names
  gather(variable, value, -(npdes_permit_id:year_quarter)) %>%
  unite(temp, year_quarter, variable) %>%
  spread(temp, value) %>%
  # select true values for the previous quarter and prediction for the target quarter
  select(matches('previous_true|target_prediction|previous_measurement|target_measurement')) %>%
  mutate(year_quarter = target_quarter)
  # it is perhaps necessary to remove permits that don't have target predictions..

# true for 20193 and prediction for 20194
forecasts_data_train <- tmp %>%
  filter(year_quarter %in% c('20193', '20194')) %>%
  mutate(year_quarter = ifelse(year_quarter == '20193', 'previous', 'target')) %>%
  gather(variable, value, -(npdes_permit_id:year_quarter)) %>%
  unite(temp, year_quarter, variable) %>%
  spread(temp, value) %>%
  select(matches('previous_true|target_prediction|previous_measurement|target_measurement')) %>%
  mutate(year_quarter = previous_quarter)

forecasts_data <- bind_rows(forecasts_data_target, forecasts_data_train)

# 3. facility-level meta data

permit_data <- permit %>%
  # there are about 1,500 permits that have two sector codes associated with it
  # we will take the first sector for now
  group_by(npdes_permit_id) %>%
  mutate(row_number = row_number()) %>%
  filter(row_number == 1) %>%
  select(-row_number)

# 4. historical status data
history_data <- bind_rows(history, train_history)

# Finally - join labels, forecasts data, historical status data, and permit-level meta data
data <- true_class_labels %>%
  left_join(true_reg_values) %>%
  left_join(forecasts_data) %>%
  left_join(permit_data) %>%
  left_join(history_data) 

# Process prediction data -------------------------------------------------

# 1. treat missing values
treated_data <- data %>%
  mutate(individual_permit_flag = as.character(individual_permit_flag)) %>%
  # create a factor for missing values
  mutate_if(is.character, replace_na, 'missing') %>%
  ungroup() %>%
  mutate_if(is.character, as.factor) %>%
  # impute 0 for missing numeric values
  mutate_if(is.numeric, replace_na, 0) %>%
  # true label used for prediction
  mutate(true_synthetic_esnc = as.factor(true_synthetic_esnc),
         true_qncr_esnc = as.factor(true_qncr_esnc)) %>%
  # arrange column names to put true values and labels to front
  select(npdes_permit_id, 
         year_quarter, 
         true_synthetic_esnc,
         true_qncr_esnc,
         true_qncr_status,
         cat_1and2_true_weighted_exceedence_pct_sum,
         cat_1and2_true_exceedence_pct_sum,
         everything())

# save prediction data as both csv and rds files (csv for quicker access, rds for better coding efficiency)
write_csv(treated_data, file.path(data_dir, paste0('prediction_data.csv')))
saveRDS(treated_data, file.path(data_dir, paste0('prediction_data.rds')))

# 2. Split into training and testing data

train_data <- treated_data %>%
  filter(year_quarter == previous_quarter)
test_data <- treated_data %>%
  filter(year_quarter == target_quarter)
nrow(train_data)
nrow(test_data)

# save prediction features (removing label features)
all_features <- names(treated_data)
pred_features <- all_features[!all_features %in% c('npdes_permit_id',
                                                   'year_quarter',
                                                   'true_synthetic_esnc',
                                                   'true_qncr_esnc',
                                                   'true_qncr_status',
                                                   'cat_1and2_true_weighted_exceedence_pct_sum',
                                                   'cat_1and2_true_exceedence_pct_sum')]

# Modeling: Classification RF -------------------------------------------------------

# 1. train model
outcome_feature <- 'true_synthetic_esnc'

# set tune grid: same for both classification and regression
control <- trainControl(method = 'repeatedcv',
                        number = 10,
                        repeats = 3,
                        search = 'grid')
tune_grid <- expand.grid(mtry = seq(from = 1, to = length(pred_features), 3))

start_time <- Sys.time()
set.seed(seed)
best_rf <- train(x = train_data[, c(pred_features)],
                 y = pull(train_data, outcome_feature),
                 method = 'rf',
                 importance = TRUE,
                 trControl = control,
                 tuneGrid = tune_grid)
end_time <- Sys.time()
message(paste0('==> tunning took ', difftime(end_time, start_time, units = 'mins'), ' mins.'))

saveRDS(best_rf, file.path(class_dir, 'best_rf.rds'))

# 2. output prediction results as binary class and probability
preds <- predict(best_rf, newdata = test_data[, c(pred_features)])
probs <- predict(best_rf, newdata = test_data[, c(pred_features)], type = 'prob')

# 3. output permits by risk scores
class_permits <- test_data %>%
  select(npdes_permit_id, individual_permit_flag, year_quarter, true_synthetic_esnc, true_qncr_esnc, true_qncr_status) %>%
  mutate(prediction_synthetic_esnc = preds,
         prediction_synthetic_esnc_prob = probs[,'TRUE']) %>%
  arrange(-prediction_synthetic_esnc_prob)

write_csv(class_permits, file.path(class_dir, 'permit_scores.csv'))

# 4. output performance
# 1) confusion matrix
performance <- table(preds, test_data$true_synthetic_esnc)
# 2) other metrics
accuracy <- mean(preds == test_data$true_synthetic_esnc) 
recall <- performance[2,2]/sum(performance[,2]) 
precision <- performance[2,2]/sum(performance[2,]) 
# 3) prc and roc 
pr <- pr.curve(scores.class0 = probs[test_data$true_synthetic_esnc == 'TRUE','TRUE'],
               scores.class1 = probs[test_data$true_synthetic_esnc == 'FALSE','TRUE'],
               curve = T)
pdf(file = file.path(class_dir, 'pr_curve.pdf'))
plot(pr)
dev.off()

roc <- roc.curve(scores.class0 = probs[test_data$true_synthetic_esnc == 'TRUE','TRUE'],
                 scores.class1 = probs[test_data$true_synthetic_esnc == 'FALSE','TRUE'],
                 curve = T)
pdf(file = file.path(class_dir, 'roc_curve.pdf'))
plot(roc)
dev.off()
pdf(NULL)

# 4) feature importance
feature_importance <- importance(best_rf) %>%
  data.frame() %>%
  mutate(feature = row.names(.)) %>%
  arrange(-MeanDecreaseGini) %>%
  select(feature, MeanDecreaseGini)

feature_importance <- varImp(best_rf)$importance %>%
  data.frame() %>%
  mutate(feature = row.names(.)) %>%
  mutate(importance_score = `TRUE.`) %>%
  arrange(-importance_score) %>%
  select(feature,importance_score)

# 5) save performance tables
con_matrix <- data.frame(performance)
names(con_matrix ) <- c('pred','true','freq')
con_matrix <- con_matrix %>%
  spread(true, freq)
write_csv(con_matrix, file.path(class_dir, 'confusion_matrix.csv'))

write_csv(feature_importance, file.path(class_dir, 'feature_importance.csv'))

class_performance <- data.frame(
                           'train_size' = nrow(treated_data[treated_data$year_quarter == previous_quarter,]),
                           'test_size' = nrow(treated_data[treated_data$year_quarter == target_quarter,]),
                           'accuracy' = accuracy,
                           'recall' = recall,
                           'precision' = precision,
                           'auprc' = pr$auc.integral,
                           'auroc' = roc$auc)

write_csv(class_performance, file.path(class_dir, 'performance.csv'))

# 6) calibration plot
bins <- 10
plot_df <- class_permits %>%
  mutate(quantile_rank = (floor(prediction_synthetic_esnc_prob*bins))%%bins + 1) %>%
  mutate(true_synthetic_esnc = as.logical(true_synthetic_esnc)) %>%
  group_by(quantile_rank) %>%
  summarise(pred_avg = mean(prediction_synthetic_esnc_prob),
            true_avg = mean(true_synthetic_esnc)
            )

ggplot(plot_df, aes(x = pred_avg, y = true_avg)) +
  geom_point(size = 5, color = color2) +
  geom_line(color = color2) +
  geom_abline(intercept = 0, slope = 1, color = 'grey40', linetype = 'dashed') +
  labs(x = 'Predicted Probability of Effluent SNC',
       y = 'Fraction of True Effluent SNC') +
  theme_bw()
ggsave(file.path(class_dir, 'class_calibration.pdf'),
       height = 5, width = 5)

# Modeling: Regression -----------------------------------------------------------
## using RF regressor with the same features in classification 

# 1. train model
outcome_feature <- 'cat_1and2_true_weighted_exceedence_pct_sum'

start_time <- Sys.time()
set.seed(seed)
best_rf <- train(x = train_data[, c(pred_features)],
                 y = pull(train_data, outcome_feature),
                 method = 'rf',
                 importance = TRUE,
                 trControl = control,
                 tuneGrid = tune_grid) # grid defined in the classification step
end_time <- Sys.time()
message(paste0('==> tunning took ', difftime(end_time, start_time, units = 'mins'), ' mins.'))

saveRDS(best_rf, file.path(reg_dir, 'best_rf.rds'))

# 2. output regressions predictions
preds <- predict(best_rf, newdata = test_data[, c(pred_features)])

# 3. output permit risk scores
reg_scores <- test_data %>%
  select(npdes_permit_id, 
         individual_permit_flag, 
         year_quarter, 
         cat_1and2_true_weighted_exceedence_pct_sum, 
         target_prediction_cat_1and2_weighted_exceedence_pct_sum,
         cat_1and2_true_exceedence_pct_sum,
         target_prediction_cat_1and2_weighted_exceedence_pct_sum) %>%
  mutate(cat_1and2_prediction_weighted_exceedence_pct_sum = preds) %>%
  mutate(errors = cat_1and2_prediction_weighted_exceedence_pct_sum - cat_1and2_true_weighted_exceedence_pct_sum)

write_csv(reg_scores, file.path(reg_dir, 'permit_scores.csv'))

# 3. output performance
## 1) RMSE
### RF model
(reg_rmse <- rmse(reg_scores$cat_1and2_true_weighted_exceedence_pct_sum, reg_scores$cat_1and2_prediction_weighted_exceedence_pct_sum))
### ARIMA 
(arima_rmse <- rmse(reg_scores$cat_1and2_true_weighted_exceedence_pct_sum, reg_scores$target_prediction_cat_1and2_weighted_exceedence_pct_sum))
### Imputing means
reg_scores <- reg_scores %>%
  mutate(dummy_outcome = mean(reg_scores$cat_1and2_true_weighted_exceedence_pct_sum))
(mean_rmse <- rmse(reg_scores$cat_1and2_true_weighted_exceedence_pct_sum, reg_scores$dummy_outcome))

## with the top 3% removed:
cap <- quantile(reg_scores$cat_1and2_prediction_weighted_exceedence_pct_sum, 0.97)
tmp <- reg_scores %>%
  filter(cat_1and2_prediction_weighted_exceedence_pct_sum <= cap)
# RF model
(reg_rmse_3pctremoved <- rmse(tmp$cat_1and2_true_weighted_exceedence_pct_sum, tmp$cat_1and2_prediction_weighted_exceedence_pct_sum))
# imputing means
(mean_rmse_3pctremoved <- rmse(tmp$cat_1and2_true_weighted_exceedence_pct_sum, tmp$dummy_outcome))

## b) RMLSE
tmp <- reg_scores %>%
  mutate(cat_1and2_true_weighted_exceedence_pct_sum = log(cat_1and2_true_weighted_exceedence_pct_sum + 1),
         cat_1and2_prediction_weighted_exceedence_pct_sum = log(cat_1and2_prediction_weighted_exceedence_pct_sum + 1),
         target_prediction_cat_1and2_weighted_exceedence_pct_sum = log(target_prediction_cat_1and2_weighted_exceedence_pct_sum + 1),
         dummy_outcome = log(mean(reg_scores$cat_1and2_true_weighted_exceedence_pct_sum) + 1)) %>%
  filter(!is.na(cat_1and2_true_weighted_exceedence_pct_sum),
         !is.na(cat_1and2_prediction_weighted_exceedence_pct_sum),
         !is.na(target_prediction_cat_1and2_weighted_exceedence_pct_sum),
         !is.na(dummy_outcome)
         )
# RF
(reg_rmsle <- rmse(tmp$cat_1and2_true_weighted_exceedence_pct_sum, tmp$cat_1and2_prediction_weighted_exceedence_pct_sum))
# arima
(arima_rmsle <- rmse(tmp$cat_1and2_true_weighted_exceedence_pct_sum, tmp$target_prediction_cat_1and2_weighted_exceedence_pct_sum))
# mean
(mean_rmsle <- rmse(tmp$cat_1and2_true_weighted_exceedence_pct_sum, tmp$dummy_outcome))

## save results:
reg_performance <- data.frame(train_size = nrow(train_data),
                              test_size = nrow(test_data),
                              reg_rmse = reg_rmse,
                              arima_rmse = arima_rmse,
                              mean_rmse = mean_rmse,
                              reg_rmse_3pctremoved = reg_rmse_3pctremoved,
                              mean_rmse_3pctremoved = mean_rmse_3pctremoved,
                              reg_rmsle = reg_rmsle,
                              arima_rmsle = arima_rmsle,
                              mean_rmsle = mean_rmsle)
write_csv(reg_performance, file.path(reg_dir, 'performance.csv'))

## b) feature importance
feature_importance <- importance(best_rf) %>%
  data.frame() %>%
  mutate(feature = row.names(.)) %>%
  arrange(-IncNodePurity) %>%
  select(feature, IncNodePurity)
write_csv(feature_importance, file.path(reg_dir, 'feature_importance.csv'))

## c) plot residual histogram
ggplot(reg_scores, aes(x = errors)) +
  geom_histogram(bins = 100) +
  labs(x = 'Residuals',
       y = 'Series Count',
       title = 'Histogram of Model Residuals')+
  theme_bw()
ggsave(file.path(reg_dir, 'residual_histogram.pdf'),
       height = 4,
       width = 6)

## d) calibration plot
bins <- 10

plot_df <- reg_scores %>%
  mutate(quantile_rank = ntile(reg_scores$cat_1and2_prediction_weighted_exceedence_pct_sum, bins)) %>%
  group_by(quantile_rank) %>%
  summarise(true_avg = mean(cat_1and2_true_weighted_exceedence_pct_sum, na.rm = TRUE),
            pred_avg = mean(cat_1and2_prediction_weighted_exceedence_pct_sum, na.rm = TRUE)) %>%
  mutate(diff = (true_avg - pred_avg)) 

ggplot(plot_df, aes(x = quantile_rank, y = diff)) +
  geom_point(size = 5, color = color2) +
  geom_hline(yintercept = 0, color = 'grey40', linetype = 'dashed') +
  labs(x = 'Decile of Predicted Weighted Exceedance Percantages',
       y = 'Difference Between Average Observed Value and Predicted Value') +
  #ylim(-2, 2) +
  theme_bw() +
  theme(legend.position = 'none')
ggsave(file.path(reg_dir, 'reg_calibration_diff.pdf'),
       height = 5, width = 5)

# bin averages
bin_avg <- data.frame(bin = seq(1, bins),
                      pred_avg = plot_df$pred_avg,
                      true_pred_diff = plot_df$diff)
write_csv(bin_avg, file.path(reg_dir, 'calibration_bin_avg.csv'))

# Output aggregrated result table -----------------------------------------------------

prediction_reg_scores <- reg_scores %>%
  rename(true_cat_1and2_weighted_exceedence_pct_sum = cat_1and2_true_weighted_exceedence_pct_sum,
         prediction_cat_1and2_weighted_exceedence_pct_sum = cat_1and2_prediction_weighted_exceedence_pct_sum,
         true_cat_1and2_exceedence_pct_sum = cat_1and2_true_exceedence_pct_sum)

prediction_class_permits <- class_permits %>%
  rename(true_eff_violation = true_synthetic_esnc,
         true_historical_esnc = true_qncr_esnc,
         prediction_eff_violation = prediction_synthetic_esnc,
         prediction_eff_violation_prob = prediction_synthetic_esnc_prob) %>%
  mutate(true_eff_violation = as.logical(true_eff_violation),
         prediction_eff_violation = as.logical(prediction_eff_violation)) %>%
  mutate(individual_permit_flag = as.factor(individual_permit_flag),
         year_quarter = as.factor(year_quarter))

prediction_result <- prediction_reg_scores %>%
  left_join(prediction_class_permits) %>%
  select(npdes_permit_id, 
         individual_permit_flag, 
         year_quarter, 
         true_historical_esnc,
         true_eff_violation, 
         prediction_eff_violation, 
         prediction_eff_violation_prob,
         true_cat_1and2_weighted_exceedence_pct_sum,
         prediction_cat_1and2_weighted_exceedence_pct_sum,
         true_cat_1and2_exceedence_pct_sum)

write_csv(prediction_result, file.path(data_dir, paste0('rf_results_', run_time,'.csv')))

stopCluster(cl)

message('======Finish output=========')
