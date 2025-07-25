# This is the ORIGINAL `README.md`.

# MAST30034 Project 1 README.md

- Name: `Zihan Yin`  
- Student ID: `1149307`

---

## Research Goal

**trip_duration**: the time it takes a passenger to get in and out of a taxi  
**Timeline**: 2019.01 – 2019.6

---

## Running Order of Files

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
