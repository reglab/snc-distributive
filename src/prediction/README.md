## Note

1. **0_dependencies.R** contains the list of packages necessary to run the scripts in this folder. 
2. **1_run_arima.R** runs ARIMA models over all time series data on a High Performance Computing cluster based in Stanford Law School, and stores the outputs in Nicole Lin's personal node.
3. **2_upload_arima_to_db.R** uploads ARIMA outputs from Nicole's personal node to RegLab's centralized database for EPA data. 
4. **3_run_random_forest.R** generates prediction data and run random forest for both the classification and regression model. All outputs are stored in the `output/models` folder. 
5. **4_distributive_impact.R** produces distributive impact results for both the oracle test and the prediction results. Outputs are stored in the `output/figures` and  `output/tables` folders.
