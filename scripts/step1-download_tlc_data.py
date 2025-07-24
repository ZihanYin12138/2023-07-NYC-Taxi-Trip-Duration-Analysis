# Import necessary modules
from pyspark.sql import SparkSession
from urllib.request import urlretrieve
import os

# Create a Spark session
spark = (
    SparkSession.builder.appName('ADS_project_1.py')
    .config('spark.sql.repl.eagerEval.enabled', True)
    .config('spark.sql.parquet.cacheMetadata', 'true')
    .config('spark.sql.session.timeZone', 'Etc/UTC')
    .config('spark.driver.memory', '4g')
    .config('spark.executer.memory', '8g')
    .getOrCreate()
)

# Define a list of data sources & usage
data_source_list = [
    'TLC_data', 'TLC_data_for_test', 'external_data'
]

# Loop through the lists of data sources & usage
for data_source in data_source_list:
    # Define the directory for data source in '../data/landing/'
    directory = (
        '../mast30034-project-1-ZihanYin12138/data/landing' + '/' + data_source
    )
    # Check if the directory exists; if not, create it
    if not os.path.exists(directory):
        os.makedirs(directory)

# Root URL for the TLC data
root_data_url = (
    'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-'
)

# Define a list of months which we want to download TLC_data for
mouth_list = range(1, 7)

# Loop through the list of months
for month in mouth_list:
    print(f'\nstarting 2019 {month}')
    # Convert the month number to a string and pad it with zeros if needed
    month = str(month).zfill(2)
    # Define the specific URL for each month's TLC_data
    specific_data_url = f'{root_data_url}{month}.parquet'
    # Define the output path for saving the TLC_data
    output_path = (
        '../mast30034-project-1-ZihanYin12138/data/landing/TLC_data/'
        f'yellow_tripdata_2019-{month}.parquet'
    )
    # Download the data from the specific URL and save it to the output path
    urlretrieve(specific_data_url, output_path)
    # Print a message indicating the downloading process
    print(f'\nfinishing 2019 {int(month)}')

# Stop the Spark session
spark.stop()
