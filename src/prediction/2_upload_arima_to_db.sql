/*
Must have already generated the output in LC-R with src/1_run_arima.R

Push arima output generated in LC-R to the database 

*/

drop table if exists model_outputs.arima_forecasts_snc_dist;
create table if not exists model_outputs.arima_forecasts_snc_dist
(
series_id text,
npdes_permit_id text,
permit_state text,
parameter_code text,
parameter_desc text,
snc_type_group integer,
statistical_base_monthly_avg text,
limit_value_standard_units numeric,
limit_unit_desc text,
limit_value_type_code text,
limit_value_qualifier_code text,
prediction_timepoint date,
training_period_start_date date,
training_period_end_date date,
model_status_code text,
model_status_desc text, 
fail_arima boolean,
prediction_value numeric,
prediction_violation_flag boolean, 
prediction_exceedence_pct numeric, 
prediction_sd numeric,
prediction_violation_probability numeric,
imputed_mean numeric,
true_value numeric,
true_exceedence_pct numeric, 
true_violation_flag boolean,
model_run_start_timestamp timestamp,
model_run_end_timestamp timestamp
);

-- copy the large file 
-- 2019q3-2020q1: file is about 7GB. 
-- takes about 8 mins
\copy model_outputs.arima_forecasts_snc_dist FROM '/home/hongjinl/EPA/Data/processed/arima_output/snc_dist_20193-20201.csv' DELIMITER ',' CSV HEADER NULL AS 'NA';


-- add indices
ALTER TABLE model_outputs.arima_forecasts_snc_dist ADD UNIQUE (series_id, prediction_timepoint, training_period_start_date, model_run_start_timestamp);


-- vacuum and analyze
VACUUM ANALYZE model_outputs.arima_forecasts_snc_dist;