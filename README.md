## The Distributive Effects of Risk Prediction in Environmental Compliance: Algorithmic Design, Environmental Justice, and Public Policy

---

This repository contains the code necessary to reproduce the results, figures, and tables for the article "The Distributive Effects of Risk Prediction in Environmental Compliance: Algorithmic Design, Environmental Justice, and Public Policy" published in *the Proceedings of 2021 ACM FAccT Conference in Toronto, Canada* ([link](https://doi.org/10.1145/1122445.1122456)).

Please cite this article as follows, or use the BibTeX entry below.

> Elinor Benami, Reid Whitaker, Vincent La, Hongjin Lin, Brandon R. Anderson, and Daniel E. Ho. 2021. The Distributive Effects of Risk Prediction in Environmental Compliance: Algorithmic Design, Environmental Justice, and Public Policy. In *Toronto ’21: ACM FAccT, June 03–05, 2021, Toronto, Canada.* ACM, New York, NY, USA, 16 pages. https://doi.org/10.1145/1122445.1122456

```tex
@article{Benami2021Distributive,
    author = {Elinor Benami, Reid Whitaker, Vincent La, Hongjin Lin, Brandon R. Anderson, and Daniel E. Ho},
    doi = {10.1145/1122445.1122456},
    journal = {Toronto ’21: ACM FAccT},
    month = {6},
    title = {{The Distributive Effects of Risk Prediction in Environmental Compliance: Algorithmic Design, Environmental Justice, and Public Policy}},
    year = {2021}
}
```

As of Jan 20th, 2021, we have yet to include scripts that reproduce Figure 2, Figure 3, and Figure 4.  

## Hardware and Software Requirements

This code was tested on a system with the following specifications:

- operating system: macOS Mojave Version 10.14.6
- processor: 4 GHz Intel Core i7
- memory (RAM): 16 GB 1867 MHz DDR3

The main software requirements are Python 3.7.3, and R version 3.6.1 (2019-07-05) with Rstudio version 1.2.5019 (Elderflower). For scripts in R, it is recommended that the user opens the `snc-distributive.Rproj` R project file. The complete list of packages for R scripts are listed in `src/prediction/0_dependencies.R`, which is sourced in the beginning of each R scripts. The list of packages for Python scripts are listed within the scripts and should be installed prior to running the scripts. 

## Model Training Instructions
The `src/prediction` folder contains scripts necessary to generate prediction results and distributive impact results. Model outputs are saved in the `output/models` folder.
1. **Run ARIMA models**: `1_run_arima.R` 
2. **Upload ARIMA results to database**: `2_upload_arima_to_db.R`
3. **Run Random Forest models for both classification and regression**: `3_run_random_forest.R`
4. **Produce distributive impact results**: `4_distributive_impact.R`

## Figure and Table Replication Instructions
The `src/figures` and `src/tables` folders contain scripts necessary to generate figures and tables in the article. Outputs are saved in the `output/figures` and `output/tables` folders.

## Data Availability Statment
Most scripts in this repo depends on access to RegLab's AWS database. Most EPA data contained in the database, including DMRs, are sourced from EPA's [public data download page](https://echo.epa.gov/tools/data-downloads). We are not able to publish detailed prediction results due to sensitivity of the information. Interested persons should contact Elinor Benami (elinor@vt.edu) for data access issues.

