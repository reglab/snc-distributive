
#########################
# Run ARIMA script on LC-R 
# Stanford Law School's High Performance Computing Cluster
# for 2019Q3 - 2020Q1 results
# author: Hongjin (Nicole) Lin, RegLab, Stanford Law School (hongjinl@law.stanford.edu)

# Requirements: 
# 1. connected to the RegLab's EPA database

# Instructions:
# In terminal: 
# 1. Clone the repo to your local machine
# 2. Go to the repo folder
# 3. Run: Rscript src/prediction/1_run_arima.R <database name> <numer of CPU to use> <name of the main output csv> <name of output databse table> <states to generate results for>
# Input: 
# 1. database name, options: ['reglabepa', 'reglabepa_snc_distributive_implications_20201019']
# 2. number of CPU to use: options: ['max', 'half', any other number depending on the number of CPUs available]
# 3. name of the main csv to be pushed to the database
# 4. name of the database table for the arima output, options: ["arima_forecasts_snc_dist_v2", "arima_forecasts_snc_dist_v3"]
# 5. states to generate output for, options: ['all', any list of states in the format: XX,YY,ZZ,...]
# Output: 
# 1. log file in logs
# 2. arima output files on your local machine 

########################

# set up ------------------------------------------------------------------

HOME_DIR <- Sys.getenv('HOME')
setwd(HOME_DIR)

# import packages
repo_dir <- rstudioapi::getActiveProject()
setwd(repo_dir)
source(file.path(repo_dir, 'src', 'prediction', '0_dependencies.R'))

# file directories (hard coded to local folders)
output_dir <- file.path('EPA', 'Data', 'processed')
arima_dir <- file.path(output_dir, 'arima_output')
if (!dir.exists(arima_dir)){
  dir.create(arima_dir)
}

# log output
total_start_time <- Sys.time()
runtime <- total_start_time %>%
  substr(., 1, 20) %>%
  str_replace_all(., ' ', '_') %>%
  str_replace_all(., ':', '-')

log_file <- file(file.path(repo_dir, "logs", paste0("runarima_", runtime, ".txt")))
sink(log_file, append=TRUE)
sink(log_file, append=TRUE, type="output")
sink(log_file, append=TRUE, type="message")

# read input arguments 
args <- commandArgs(trailingOnly = TRUE)
## 1) argument 1: database name, options: ['reglabepa', 'reglabepa_snc_distributive_implications_20201019']
db_name <- args[1]
if (!(db_name %in% c('reglabepa', 'reglabepa_snc_distributive_implications_20201019'))){
  stop('Invalid database name. Choose from "reglabepa" or "reglabepa_snc_distributive_implications_20201019".')
}
## 2) argument 2: number of CPU to use: options: ['max', 'half', any other number depending on the number of CPUs available]
n_cores_arg <- args[2]
if (!(n_cores_arg %in% c('max', 'half') | is.numeric(n_cores_arg))){
  stop('Invalid number of CPUs to use. Choose from "max", "half", or any other number depending on the number of CPUs available.')
} else if (n_cores_arg == 'max'){
  # avoid monopolizing resources
  n_cores <- ifelse(is.na(detectCores()), 100, floor(detectCores()*0.9))
} else if (n_cores_arg == 'half'){
  n_cores <- ifelse(is.na(detectCores()), 100, floor(detectCores()*0.5))
} else {
  n_cores <- as.numeric(n_cores_arg)
}

## 3) argument 3: name of the main csv to be pushed to the database, not inlcuding file extension name ".csv"
arima_file_name <- args[3]
if (str_detect(arima_file_name, '.csv')){
  stop('Invalid output file name. Remove ".csv" from file name.')
}
## 4) argument 4: name of the database table for the arima output, options: ["arima_forecasts_snc_dist_v2", "arima_forecasts_snc_dist_v3"]
arima_dbtable_name <- args[4]
if (!(arima_dbtable_name %in% c('arima_forecasts_snc_dist_v2', 'arima_forecasts_snc_dist_v3'))){
  stop('Invalid output database table name. Choose from "arima_forecasts_snc_dist_v2" and "arima_forecasts_snc_dist_v3".')
}
## 5) argument 5: states to generate output for, options: ['all', any list of states in the format: XX,YY,ZZ,...]
state_list <- args[5]
if (state_list != 'all'){
  states <- unlist(strsplit(state_list, ","))
}

message(paste0('Start outputing arima forecasts with DMR records from ', db_name,
               ' using ', n_cores, ' CPUs. ',
               'ARIMA output will be saved as ', arima_file_name, 
               '.csv, which will be later uploaded to RegLab EPA database as ', 
               'model_inputs.', arima_dbtable_name, '\n',
               '======== Generating output for ', state_list, ' states. =========='))

# make connection to database ---------------------------------------------

# credentials should have been saved as environment variables
db_host <- Sys.getenv('EPA_DB_HOST')
db_port <- Sys.getenv('EPA_DB_PORT')
db_username <- Sys.getenv('EPA_DB_USER')
db_password <- Sys.getenv('EPA_DB_PASSWORD')

con <- dbConnect(RPostgres::Postgres(),
                 dbname=db_name, 
                 host=db_host,
                 port=db_port ,
                 user=db_username,
                 password=db_password)

# read all dmr to disk ----------------------------------------------------

message('==> Start reading dmr')

query <- "
select  series_id,
        npdes_permit_id,
        permit_state,
        parameter_code,
        parameter_desc,
        snc_type_group,
        statistical_base_monthly_avg,
        limit_value_standard_units,
        limit_unit_desc,
        limit_value_type_code,
        limit_value_qualifier_code,
        monitoring_period_end_date,
        dmr_value_standard_units,
        exceedence_pct,
        effluent_violation_flag
from icis.dmrs_dedupped
where dmr_value_standard_units is not null
and dmr_value_standard_units > 0
and limit_value_standard_units > 0
and limit_value_qualifier_code in ('<', '<=')
;"

# limit to a subset of rows when testing
# query <- paste0(query, '\n limit 10000')

start_time <- Sys.time()
db_query <- dbSendQuery(con, query)
dmr <- dbFetch(db_query) %>%
  arrange(series_id, monitoring_period_end_date)
end_time <- Sys.time()
# took about 0.2 min to read in CA
message(paste0('==> Finish reading DMR records: took ', difftime(end_time, start_time, units = 'mins'), ' mins.'))

# # save to disk
# start_time <- Sys.time()
# saveRDS(dmr, file = file.path(output_dir, paste0('aws_dmr_', runtime, '.rds')))
# end_time <- Sys.time()
# message(paste0('==> Finish writing to file: took ', difftime(end_time, start_time, units = 'mins'), ' mins.'))

# treat dmr data  ---------------------------------------------------------
# remove dmr values with known error codes in the exceedance percentages column

treat_dmr <- function(dmr){
  message('==> Removing dmr values with known error codes in the exceedance percentages column ')
  
  # 33925 records removed: 0.05% of records
  treated_dmr <- dmr %>%
    filter(!(exceedence_pct %in% c(2147483650, 99999)))
  
  # recalculate exceedence pct to keep consistent with the prediction exceedence calculation
  # note that there are few cases of mismatches in the true exceedence pct: about 3% don't match
  treated_dmr <- treated_dmr %>%
    mutate(effluent_violation_flag = ((limit_value_qualifier_code == '<=') & (dmr_value_standard_units > limit_value_standard_units)) |
             ((limit_value_qualifier_code == '<') & (dmr_value_standard_units >= limit_value_standard_units))
    ) %>%
    # calculate predicted exceedence percentage based on limit value qualifiers and limit values
    mutate(exceedence_pct = (dmr_value_standard_units - limit_value_standard_units)/limit_value_standard_units) %>%
    mutate(exceedence_pct = 100*exceedence_pct) %>%
    # replace negative exceedence pct with NA 
    mutate(exceedence_pct = ifelse(effluent_violation_flag, exceedence_pct, NA)) 
  
  treated_dmr
}

# run arima and save intermedia csv ---------------------------------------

run_arima <- function(state_dmr,
                      sample_id,
                      prediction_period_start_date,
                      prediction_period_end_date,
                      training_period_start_date,
                      training_period_end_date){
  
  # subset to dmr for the series
  sub_dmr <- state_dmr %>%
    filter(series_id == sample_id) %>%
    mutate(missing_dmr_value = is.na(dmr_value_standard_units))
  
  if (training_period_start_date == ymd("1000-01-01")){
    sub_training_period_start_date <- min(sub_dmr$monitoring_period_end_date, na.rm = TRUE)
  } else{
    sub_training_period_start_date <- training_period_start_date
  }
  
  # training data
  train <- sub_dmr %>%
    filter(monitoring_period_end_date >= sub_training_period_start_date) %>%
    filter(monitoring_period_end_date <= training_period_end_date) 
  
  arima_output <- sub_dmr %>%
    select(series_id,
           npdes_permit_id,
           permit_state,
           parameter_code,
           parameter_desc,
           snc_type_group,
           statistical_base_monthly_avg,
           limit_value_standard_units,
           limit_unit_desc,
           limit_value_type_code,
           limit_value_qualifier_code) %>%
    distinct() %>%
    # assume each series would have a value reported each monthly
    cbind(data.frame(prediction_timepoint = seq(prediction_period_start_date + 1, prediction_period_end_date + 1, by = "months") - 1)) %>%
    # put in training start and end dates
    mutate(training_period_start_date = sub_training_period_start_date,
           training_period_end_date = training_period_end_date) %>%
    # keep all output columns consistent 
    mutate(model_status_code = NA, 
           model_status_desc = NA,
           fail_arima = NA,
           prediction_value = NA,
           prediction_violation_flag = NA,
           prediction_exceedence_pct = NA, 
           prediction_sd = NA,
           prediction_violation_probability = NA,
           imputed_mean = NA,
           true_value = NA,
           true_exceedence_pct = NA,
           true_violation_flag = NA,
           model_run_start_timestamp = NA, 
           model_run_end_timestamp = NA
    )
  
  n_prediction <- nrow(arima_output)
  
  ## only run arima if:
  # 1) there were records within the history window
  ## or
  # 1) there were records of the same prediction window in the past year, or
  # 2) there were records within the past year before the prediction window
  # still run arima but indicate that it might not be active anymore
  
  no_history <- nrow(train) == 0 | nrow(train) == sum(train$missing_dmr_value)
  
  data_last_year <- train %>%
    filter(monitoring_period_end_date >= prediction_period_start_date - years(1)) %>%
    filter(monitoring_period_end_date <= prediction_period_end_date - years(1))
  no_data_last_year <- nrow(data_last_year) == 0 | nrow(data_last_year) == sum(data_last_year$missing_dmr_value)
  
  data_recent <- train %>%
    filter(monitoring_period_end_date >= prediction_period_start_date - years(1))
  no_recent_data <- nrow(data_recent) == 0 | nrow(data_recent) == sum(data_recent$missing_dmr_value)
  
  if (no_history){
    arima_output <- arima_output %>%
      mutate(model_status_code = 'NH') %>%
      mutate(model_status_desc = 'no historical data for the given prediction window')
  } else {
    target <- sub_dmr %>%
      filter(monitoring_period_end_date >= prediction_period_start_date) %>%
      filter(monitoring_period_end_date <= prediction_period_end_date) %>%
      rename(prediction_timepoint = monitoring_period_end_date) 
    
    ## run arima
    # # convert vector (ordered) to time series records
    records <- train$dmr_value_standard_units
    records <- ts(records)
    
    # in cases where there are majority NA values, arima fails
    arima_run <-  tryCatch({
      # the auto.arima function uses cross validation to find the best arima model for the given time series
      model <- auto.arima(records)
      # check wether arima fails to output non-zero mean forecasts
      fail_arima <- length(model$coef) == 0
      model_forecast <- forecast(model, h = n_prediction)
      pred_mean <- as.numeric(model_forecast$mean)
      pred_sd <- sqrt(model$sigma2)
      TRUE
    }, error = function(e){
      FALSE
    })
    
    # if arima run failed: input missing values to prediction values and sd
    if (!arima_run){
      pred_mean <- NA
      pred_sd <- NA
    }
    
    arima_output$fail_arima <- fail_arima
    arima_output$prediction_value <- pred_mean
    arima_output$prediction_sd <- pred_sd
    arima_output$imputed_mean <- mean(records, na.rm = TRUE)
    
    arima_output <- arima_output %>%
      mutate(prediction_violation_flag = ((limit_value_qualifier_code == '<=') & (prediction_value > limit_value_standard_units)) |
               ((limit_value_qualifier_code == '<') & (prediction_value >= limit_value_standard_units))
      ) %>%
      # calculate predicted exceedence percentage based on limit value qualifiers and limit values
      # if limit value = 0, then the exceedence pct is predicted - limit (when qualifier is <=, <, or =, which are 99.9977% of the case)
      mutate(prediction_exceedence_pct = (prediction_value - limit_value_standard_units)/limit_value_standard_units) %>%
      mutate(prediction_exceedence_pct = 100*prediction_exceedence_pct) %>%
      # replace negative exceedence pct with NA 
      mutate(prediction_exceedence_pct = ifelse(prediction_violation_flag, prediction_exceedence_pct, NA)) %>%
      mutate(prediction_violation_probability = ifelse(prediction_sd == 0,
                                                       ifelse(prediction_violation_flag, 1, 0),
                                                       ifelse((limit_value_qualifier_code == '<=') | (limit_value_qualifier_code == '<'),
                                                              pnorm(limit_value_standard_units, mean = prediction_value, sd = prediction_sd, lower.tail = FALSE),
                                                              pnorm(limit_value_standard_units, mean = prediction_value, sd = prediction_sd, lower.tail = TRUE )))) 
    
    ## return true value and predicted value comparison if prediction window exceeds historical data
    if (no_data_last_year & no_recent_data) {
      # change it to a flag instead
      arima_output <- arima_output %>%
        mutate(model_status_code = 'NR') %>%
        mutate(model_status_desc = paste0('no recent data in the past year (most recent value date ', max(train$monitoring_period_end_date), ')'))
    } else if (nrow(target) != 0){
      arima_output <- arima_output %>%
        left_join(target, by = c("series_id", 
                                 "npdes_permit_id", 
                                 "permit_state",
                                 "parameter_code", 
                                 "parameter_desc", 
                                 "snc_type_group",
                                 "statistical_base_monthly_avg", 
                                 "limit_value_standard_units", 
                                 "limit_unit_desc",
                                 "limit_value_type_code",
                                 "limit_value_qualifier_code", 
                                 "prediction_timepoint")) %>%
        # impute true values
        mutate(true_value = dmr_value_standard_units,
               true_exceedence_pct = exceedence_pct,
               true_violation_flag = effluent_violation_flag) %>%
        # remove redundant columns
        select(-c(dmr_value_standard_units,
                  exceedence_pct, 
                  effluent_violation_flag, 
                  missing_dmr_value)) %>%
        mutate(model_status_code = 'FH') %>%
        mutate(model_status_desc = 'forecast for historical data')
    } else {
      arima_output <- arima_output %>%
        mutate(model_status_code = 'FF') %>%
        mutate(model_status_desc = 'forecast for future data')
    }
    
  }
  
  return(arima_output)
  
}

output_arima_forecast <- function(state, 
                                  treated_dmr,
                                  prediction_period_start_date,
                                  prediction_period_end_date,
                                  training_period_start_date,
                                  training_period_end_date){
  
  message(paste0('=========', state, '========'))
  # read in dmr
  state_dmr <- treated_dmr %>%
    filter(permit_state == state)
  
  series_ids <- unique(state_dmr$series_id)
  n_series <- length(series_ids)
  
  # ~40,000 for CA
  message(paste0('==> Running ARIMA for ', n_series, ' series with ', n_cores, ' CPUs:'))
  
  start_time <- Sys.time()
  results <- mclapply(series_ids, function(s){
    tryCatch({    
      df <- run_arima(state_dmr, 
                      s,
                      prediction_period_start_date = prediction_period_start_date,
                      prediction_period_end_date = prediction_period_end_date,
                      training_period_start_date = training_period_start_date,
                      training_period_end_date = training_period_end_date)},
      error = function(e){
        df <- NULL})
    if (class(df) != 'data.frame'){
      df <- NULL
    }
    df
  },
  mc.cores = n_cores) %>%
    bind_rows()
  
  # replace infinite values to NA (numeric columns in PostgreSQL do not allow infinite numbers)
  na_results <- do.call(data.frame, lapply(results, function(x) replace(x, is.infinite(x), NA)))
  na_results$model_run_start_timestamp <- total_start_time
  
  end_time <- Sys.time()
  na_results$model_run_end_timestamp <- end_time
  
  # took about 4 mins for CA
  message(paste0('==> Output generated: ARIMA took ', difftime(end_time, start_time, units = 'mins'), ' mins.'))
  
  start_time <- Sys.time()
  # 1) save in individual state folders in case writing to the big csv file break
  state_dir <- file.path(arima_dir, state)
  if (!dir.exists(state_dir)){
    dir.create(state_dir)
  }
  saveRDS(na_results, file.path(state_dir, paste0('snc_dist_', training_period_end_date, '_', runtime, '.rds')))
  
  # 2) save to big csv file
  output_file <- file.path(arima_dir, paste0(arima_file_name, '.csv'))
  write_csv(na_results, output_file, append = file.exists(output_file))
  
  end_time <- Sys.time()
  message(paste0('==> Saving arima output took ', difftime(end_time, start_time, units = 'mins'), ' mins.'))
}

run_states <- function(states, 
                       prediction_period_start_date,
                       prediction_period_end_date,
                       training_period_start_date,
                       training_period_end_date){
  
  n_states <- length(states)
  message(paste0('----------Start ARIMA analysis for ',  n_states, ' states: ', paste(states, collapse = ', '), '---------'))
  message(paste0('Prediction period: ', prediction_period_start_date, ' to ', prediction_period_end_date,
                 '; Training period: ', training_period_start_date, ' to ', training_period_end_date))
  
  treated_dmr <- treat_dmr(dmr)
  
  errors <- c()
  for (state in states){
    tryCatch({
      output_arima_forecast(state,
                            treated_dmr,
                            prediction_period_start_date,
                            prediction_period_end_date,
                            training_period_start_date,
                            training_period_end_date)
    }, error = function(e){
      message(e)
      errors <- c(errors, state)
    })
  }
  
  message('--------------Finished ARIMA analysis----------------')
  if (length(errors) != 0){
    message(paste0('Errors in states: ', paste(errors, collapse = ', ')))
  }
  
}

# output by states --------------------------------------------------------

if (state_list == 'all'){
  states <- unique(dmr$permit_state)
} else {
  states <- states
}

# 2019Q3-Q4
year <- "2019"
run_states(states, ymd(paste0(year, "-04-30")), ymd(paste0(year, "-06-30")), ymd("1000-01-01"), ymd(paste0(year, "-03-31")))
run_states(states, ymd(paste0(year, "-07-31")), ymd(paste0(year, "-09-30")), ymd("1000-01-01"), ymd(paste0(year, "-06-30")))
# 2020Q1
year <- "2020"
run_states(states, ymd(paste0(year, "-10-31")) - years(1), ymd(paste0(year, "-12-31")) - years(1), ymd("1000-01-01"), ymd(paste0(year, "-09-30")) - years(1))

# end script --------------------------------------------------------------

total_end_time <- Sys.time()

message(paste0('Script took ', difftime(total_end_time, total_start_time, units = 'mins'), ' mins.'))

sink() 
sink(type="output")
sink(type="message")


