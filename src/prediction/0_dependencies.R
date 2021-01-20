# import packages
if (!require("pacman")){
  install.packages("pacman", repos=" https://CRAN.R-project.org")
}

# pacman is a package that helps with loading libraries: https://cran.r-project.org/web/packages/pacman/index.html
library(pacman)
p_load(
  # overall
  'tidyverse', # for data frame manipulation
  'lubridate', # for date manipulation
  'stringr', # for string manipulation
  'Metrics', # for rmse function
  'RPostgres', # for database connection
  'DBI', # for database connection
  'parallel', # for parallel computing
  # run arima
  'tsoutliers', # for treating outliers in time series
  'forecast', # for arima model and forecast
  # run random forest
  'randomForest', # for classification and regression model
  'caret', # for hyperparameter tuning
  'doParallel', # for parallel computing
  'PRROC', # for plotting PR curve and AUC curve
  'gridExtra', # putting graphs together
  'ggplotify',
  # distributive impact
   'rio',
   'janitor',
   # 'infer',
   'readxl',
   'writexl',
   'summarytools',
   'ggpubr',
   'huxtable',
   'xtable'
)
