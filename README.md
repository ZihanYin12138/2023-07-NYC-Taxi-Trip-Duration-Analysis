# 中文版README文件请见[`README.zh.md`](README.zh.md)
# 2023-07 NYC Taxi Trip Duration Analysis

A data science project analyzing NYC taxi trip durations using PySpark, from January to June 2019, including data preprocessing, geospatial visualization, and predictive modeling.

---

## Project Overview

This project explores the factors that influence **NYC taxi trip durations** using open data from the NYC Taxi and Limousine Commission (TLC), combined with weather and zone data. The final goal is to build a regression model that can predict trip duration. PySpark was used to handle and process millions of NYC taxi trip records efficiently during the preprocessing stage.

- **Research Objective**: Understand and model `trip_duration`, defined as the time from passenger pickup to drop-off.
- **Timeline Covered**: January 2019 – June 2019
- **Tools Used**: PySpark, Python, Pandas, NumPy, Scikit-learn, Seaborn, GeoPandas, Jupyter Notebook
- A PDF version of the final project report is available [here](./report/ADS_Project_1_Report.pdf).

---

## Project Structure

```plaintext
├── data/
│   ├── landing/               # Raw data downloaded from external sources
│   ├── raw/                   # After initial preprocessing
│   ├── curated/               # Cleaned and standardized datasets
│   ├── merged_data/           # Final datasets used for modeling
│   └── taxi_zones/            # Geospatial reference data
├── notebooks/
│   ├── step2-1st_preprocessing.ipynb
│   ├── step3-2nd_preprocessing.ipynb
│   ├── step4-download_external_data_and_merge.ipynb
│   ├── step5-download_taxi_zone_data_and_geo_plot.ipynb
│   ├── step6-plotting_and_analysis.ipynb
│   ├── step8-model_and_evaluation.ipynb
├── scripts/
│   ├── step1-download_tlc_data.py
│   ├── step7-build_test_data.py
├── README.md
```

> ⚠️ **Note**: The `data/` subdirectories are intentionally left empty in the repository. Please run the provided scripts to populate them, or refer to the README for data source links.

---

## Pipeline & Workflow

### Data Acquisition
- Downloads TLC trip data (`step1`) and external data including weather and zone files (`step4`, `step5`, `step7`).

### Data Preprocessing
- Initial and secondary cleaning of trip data and external data (`step2`, `step3`, `step4`).
- Merge curated datasets for final modeling (`step4`, `step5`, `step7`).

### Exploratory Data Analysis
- Visual analysis of trip duration distributions, correlations, and geospatial patterns (`step6`).

### Modeling
- Builds regression models using curated merged datasets.
- Evaluates performance using July 2019 data as holdout (`step8`).

---

## Model & Evaluation

- Uses standard regression models (e.g., Linear Regression, Random Forest).
- Evaluation metrics: RMSE, R² Score.
- Model trained on Jan–Jun 2019 data; tested on July 2019.

---

## Data Sources

- [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [NYC Open Data Portal](https://opendata.cityofnewyork.us/)
- Weather and zone files obtained through public repositories.

---

## Author

**Zihan Yin**  
Bachelor of Science, Major in Data Science, Minor in Statistics & Stochastic Modelling, The University of Melbourne  
Student ID: 1149307

---

## License

This project is licensed under the [MIT License](LICENSE).

---

## Appendix: Original README

### MAST30034 Project 1 README.md

- Name: `Zihan Yin`  
- Student ID: `1149307`

---

### Research Goal

**trip_duration**: the time it takes a passenger to get in and out of a taxi  
**Timeline**: 2019.01 – 2019.6

---

### Running Order of Files

To run the pipeline, please visit the `scripts` and `notebooks` directory and run the files in order:

1. `step1-download_tlc_data.py`:  
   Downloads TLC_data to the directory `data/landing/TLC_data/`

2. `step2-1st_preprocessing.ipynb`:  
   Does the first part of the preprocessing and stores the data in the directory `data/raw/TLC_data/`

3. `step3-2nd_preprocessing.ipynb`:  
   Does the second part of the preprocessing and stores the data in the directory `data/curated/TLC_data/`

4. `step4-download_external_data_and_merge.ipynb`:  
   Downloads external_data to the directory `data/landing/external_data/`, then preprocesses and stores it in the directories `data/raw/external_data/` and `data/curated/external_data/`.  
   Finally, curated TLC_data is merged with curated external_data and stored in the directory `data/merged_data/`.

5. `step5-download_taxi_zone_data_and_geo_plot.ipynb`:  
   Downloads taxi_zones related data to the directory `data/taxi_zones/`.  
   Merges `merged_data` and taxi_zones data and plots geospatial visualisation.

6. `step6-plotting_and_analysis.ipynb`:  
   Uses sampled `merged_data` for plotting and analysis.

7. `step7-build_test_data.py`:  
   Downloads the 2019.07 TLC_data to the directory `data/landing/TLC_data_for_test` as test data for subsequent models.  
   A complete preprocessing of the data was performed and stored in the directories `data/raw/TLC_data_for_test/` and `data/curated/TLC_data_for_test/`.  
   Finally, the data was merged with the curated `external_data` and stored in the directory `data/merged_data/`.

8. `step8-model_and_evaluation.ipynb`:  
   Uses sampled `merged_data` and `merged_data_for_test` to build and evaluate models.
