# Import required modules
from pyspark.sql import SparkSession
from urllib.request import urlretrieve
from pyspark.sql import functions as F
from pyspark.sql.functions import date_format, unix_timestamp, dayofweek
import os
from pyspark.sql.types import IntegerType

# Start a Spark session
spark = (
    SparkSession.builder.appName('ADS_project_1.py')
    .config('spark.sql.repl.eagerEval.enabled', True)
    .config('spark.sql.parquet.cacheMetadata', 'true')
    .config('spark.sql.session.timeZone', 'Etc/UTC')
    .config('spark.driver.memory', '16g')
    .config('spark.executer.memory', '16g')
    .getOrCreate()
)

# List of data steps
data_step_list = ['landing', 'raw', 'curated']

# Loop to check/create directories for data storage
for data_step in data_step_list:
    dir_path = (
        '../mast30034-project-1-ZihanYin12138/data/' +
        data_step + '/TLC_data_for_test'
    )
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

# Root URL for data
root_data_url = (
    'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-'
)

# Use 2019.07 data as test data
month_list = [7]

# Loop to download data
for month in month_list:
    print(f'starting 2019 {month}')
    print('\n')
    month_str = str(month).zfill(2)
    data_url = f'{root_data_url}{month_str}.parquet'
    save_path = (
        '../mast30034-project-1-ZihanYin12138/data/landing/' +
        f'TLC_data_for_test/yellow_tripdata_2019-{month_str}.parquet'
    )
    urlretrieve(data_url, save_path)
    print(f'finishing 2019 {int(month_str)}')
    print('\n')


# Start 1st preprocessing
print('start 1st preprocessing')
print('\n')

# Define the directory for reading TLC data
TLC_data_dir = (
    '../mast30034-project-1-ZihanYin12138/data/landing/'
    'TLC_data_for_test/yellow_tripdata_2019-07.parquet'
)
TLC_data = spark.read.parquet(TLC_data_dir)

# Count the number of rows and columns in the original data
original_num_rows = TLC_data.count()
original_num_cols = len(TLC_data.columns)
print('number of rows: ', original_num_rows)
print('number of cols: ', original_num_cols)
print('\n')

# List of features that won't be used
useless_feature_list = [
    'VendorID', 'RatecodeID', 'store_and_fwd_flag', 'payment_type',
    'fare_amount', 'mta_tax', 'improvement_surcharge', 'tip_amount',
    'total_amount'
]

# Filter out invalid data instances for useless features
TLC_data = TLC_data.where(
    # remove 'VendorID' that are not 1 & 2
    (F.col('VendorID') == 1) | (F.col('VendorID') == 2)

).where(
    # remove 'RatecodeID' that are in 1 ~ 6
    F.col('RatecodeID').isin([1, 2, 3, 4, 5, 6])

).where(
    # remove 'store_and_fwd_flag' that are not 'Y' & 'N'
    (F.col('store_and_fwd_flag') == 'Y') | (F.col('store_and_fwd_flag') == 'N')

).where(
    # remove 'payment_type' that are not 1 & 2
    F.col('payment_type').isin([1, 2])

).where(
    # remove 'fare_amount' those are smaller than or equal to 0
    F.col('fare_amount') > 0

).where(
    # remove 'mta_tax' that is not 0.5
    F.col('mta_tax') == 0.5

).where(
    # remove 'improvement_surcharge' that is not 0.3
    F.col('improvement_surcharge') == 0.3

).where(
    # remove 'tip_amount' those are smaller than 0
    F.col('tip_amount') >= 0

).where(
    # remove 'total_amount' those are smaller than 0
    F.col('total_amount') >= 0
)


# Drop the useless features from the dataset
for useless_feature in useless_feature_list:
    TLC_data = TLC_data.drop(useless_feature)

# Rename columns for easier understanding
TLC_data = (
    TLC_data.withColumnRenamed('tpep_pickup_datetime', 'pickup_time')
            .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_time')
            .withColumnRenamed('PULocationID', 'up_location_id')
            .withColumnRenamed('DOLocationID', 'off_location_id')
            .withColumnRenamed('extra', 'extra_fee')
            .withColumnRenamed('tolls_amount', 'toll_fee')
            .withColumnRenamed('passenger_count', '#passenger')
            .withColumnRenamed('congestion_surcharge', 'congestion_fee')
)

# Order the saved features
TLC_data = TLC_data.select(
    'pickup_time',
    'dropoff_time',
    'up_location_id',
    'off_location_id',
    '#passenger',
    'trip_distance',
    'congestion_fee',
    'extra_fee',
    'toll_fee',
    'airport_fee'
)

# Count the number of rows and columns after 1st preprocessing
num_rows_after_1st_preprocessing = TLC_data.count()
num_cols_after_1st_preprocessing = len(TLC_data.columns)

# Calculate the changes in rows and columns after 1st preprocessing
num_removed_rows = original_num_rows - num_rows_after_1st_preprocessing
num_removed_cols = original_num_cols - num_cols_after_1st_preprocessing

print('number of rows: ', num_rows_after_1st_preprocessing)
print('number of cols: ', num_cols_after_1st_preprocessing)
print('\n')
print('number of removed rows: ', num_removed_rows)
print('number of removed cols: ', num_removed_cols)
print('\n')

# Save the processed data to the raw directory
print('start downloading TLC_data_for_test in "../data/raw/"')
print('\n')
TLC_data.write.mode('overwrite').parquet(
    '../mast30034-project-1-ZihanYin12138/data/raw/TLC_data_for_test/'
    'TLC_data_for_test.parquet'
)
print('finish downloading TLC_data_for_test in "../data/raw/"')
print('\n')

print('finish 1st preprocessing')
print('\n')


# Start 2nd preprocessing
print('start 2nd preprocessing')
print('\n')

original_rows_2nd = TLC_data.count()
print('#rows after 1st preprocessing: ', original_rows_2nd)
print('#cols after 1st preprocessing: ', len(TLC_data.columns))
print('\n')

# Clean the valid instances
TLC_data = TLC_data.where(F.col('#passenger') > 0) \
                   .where(F.col('trip_distance') > 0) \
                   .where(F.col('extra_fee') > 0)

# Drop not needed column
TLC_data = TLC_data.drop('airport_fee')
# Change the null value to 0
TLC_data = TLC_data.fillna({"congestion_fee": 0.0})

# Add columns to dataset
TLC_data = TLC_data.withColumn(
    # Create 'date' from 'pickup_time' by extracting month and day
    'date',
    date_format('pickup_time', 'MM-dd')

).withColumn(
    # Create 'trip_duration' from 'dropoff_time' & 'pickup_time', in unit (s)
    'trip_duration',
    unix_timestamp('dropoff_time') - unix_timestamp('pickup_time')

).withColumn(
    # Create 'average_speed' from 'trip_distance' & 'trip_duration'
    # in unit (miles/h)
    'average_speed',
    F.col('trip_distance') / (F.col('trip_duration') / 3600)
).withColumn(
    'average_speed', F.col('average_speed').cast('float')
).withColumn(
    'average_speed', F.round(F.col('average_speed'), 2)

).withColumn(
    # Create 'if_weekend' from 'pickup_time', values are 0 & 1
    'if_weekend',
    dayofweek('pickup_time').isin([1, 7]).cast(IntegerType())

).withColumn(
    # Create 'if_morning_peak' from 'pickup_time', values are 0 & 1
    'if_morning_peak', (F.hour('pickup_time').between(7, 10))
    .cast(IntegerType())

).withColumn(
    # Create 'if_evening_peak' from 'pickup_time', values are 0 & 1
    'if_evening_peak', (F.hour('pickup_time').between(16, 19))
    .cast(IntegerType())

).withColumn(
    # Create 'if_peak_hour' from 'if_morning_peak' & 'if_evening_peak'
    # values are 0 & 1
    'if_peak_hour', F.expr("if_morning_peak = 1 OR if_evening_peak = 1")
    .cast(IntegerType())

).withColumn(
    # Create 'if_overnight' from 'pickup_time', values are 0 & 1
    'if_overnight', F.hour('pickup_time').isin([23, 0, 1, 2, 3, 4, 5, 6])
    .cast(IntegerType())

).withColumn(
    # Create 'if_airport' from 'up_location_id' & 'off_location_id'
    # values are 0 & 1
    'if_airport',
    ((F.col('up_location_id')).isin([1, 132, 138]) | (F.col('off_location_id'))
     .isin([1, 132, 138]))
    .cast(IntegerType())
)

# remove invalid values
TLC_data = TLC_data.where(F.col('trip_duration') > 0) \
                   .where(F.col('average_speed') > 0)

# Drop used columns
features_to_drop = [
    'pickup_time', 'dropoff_time', 'extra_fee',
    'if_morning_peak', 'if_evening_peak'
]
for feature in features_to_drop:
    TLC_data = TLC_data.drop(feature)

# Order the saved features
TLC_data = TLC_data.select(
    'date', 'up_location_id', 'off_location_id', '#passenger',
    'trip_distance', 'trip_duration', 'average_speed', 'congestion_fee',
    'toll_fee', 'if_weekend', 'if_peak_hour', 'if_overnight', 'if_airport'
)

# Removing outliers
features_for_outliers = ['trip_distance', 'trip_duration', 'average_speed']
for feature in features_for_outliers:
    # Compute IQR for feature
    Q1, Q3 = TLC_data.approxQuantile(feature, [0.25, 0.75], 0.05)
    IQR = Q3 - Q1
    lower, upper = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR
    TLC_data = TLC_data.filter((F.col(feature) >= lower) &
                               (F.col(feature) <= upper))

rows_after_2nd = TLC_data.count()
print('#rows after 2nd preprocessing: ', rows_after_2nd)
print('#cols after 2nd preprocessing: ', len(TLC_data.columns))
print('\n')
print('number of removed rows: ', original_rows_2nd - rows_after_2nd)
print('\n')

# Save the processed data to the curated directory
print('start downloading TLC_data_for_test in "../data/curated/"')
print('\n')
data_path = ('../mast30034-project-1-ZihanYin12138/data/curated/'
             'TLC_data_for_test/TLC_data_for_test.parquet')
TLC_data.write.mode('overwrite').parquet(data_path)
print('finish downloading TLC_data_for_test in "../data/curated/"')
print('\n')
print('finish 2nd preprocessing')
print('\n')


# Start merge data
print('start merge')
print('\n')

# Read external data from the specified path
external_data_dir = (
    '../mast30034-project-1-ZihanYin12138/data/curated/'
    'external_data/external_data.parquet'
)
external_data = spark.read.parquet(external_data_dir, header=True)

# Merge the TLC and external data on the 'date' column
merged_data_for_test = TLC_data.join(external_data, on="date", how="left")

# Get and print the number of rows and columns
merged_num_rows = merged_data_for_test.count()
merged_num_cols = len(merged_data_for_test.columns)
print('#rows of merged_data_for_test: ', merged_num_rows)
print('#cols of merged_data_for_test: ', merged_num_cols)
print('\n')

# Save the merged data to a new directory
save_path = ('../mast30034-project-1-ZihanYin12138/data/merged_data/'
             'merged_data_for_test.parquet')
merged_data_for_test.write.mode('overwrite').parquet(save_path)

print('finish merge')
print('\n')
print('all done')
print('\n')

# Stop the Spark session
spark.stop()
