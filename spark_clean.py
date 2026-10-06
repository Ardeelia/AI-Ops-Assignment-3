from pyspark import SparkConf, SparkContext
import pandas as pd
import time
import uuid
import os

conf = (
    SparkConf()
    .setAppName("GenericSparkApp")
    .setMaster("spark://localhost:7077")
    .set("spark.executor.cores", "1")
)
sc = SparkContext(conf=conf)

start_time = time.perf_counter()

def data_preprocessing(month_ids):
    os.chdir('/home/raghav-iyengar/DA24B047_3')
    location_data = pd.read_csv('data/taxi_zone_lookup.csv')
    saved_files = []
    for month in month_ids:
        if(month < 10):
            month_num = '0' + str(month)
        else:
            month_num = str(month)
        curr_month_data = pd.read_csv(f'data/yellow_tripdata_2024-{month_num}.csv', parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime'], low_memory = False)
        final_taxi_data = pd.merge(curr_month_data, location_data, left_on = 'PULocationID', right_on = 'LocationID', how = 'inner')
        final_taxi_data = final_taxi_data.dropna()
        final_taxi_data = final_taxi_data.drop_duplicates()
        time_udf_start = time.perf_counter()
        final_taxi_data['average_speed'] = final_taxi_data['trip_distance']/((final_taxi_data['tpep_dropoff_datetime'] - final_taxi_data['tpep_pickup_datetime']).dt.total_seconds() / 3600)
        time_udf_end = time.perf_counter()
        time_udf_taken = time_udf_end - time_udf_start
        print(time_udf_taken)
        output_filename = f'cleaned_data_2025_{month_num}_{uuid.uuid4().hex[:6]}.parquet'
        final_taxi_data.to_parquet(output_filename)
        saved_files.append(output_filename)

    for f in saved_files:
        yield f

months = [j for j in range(1,7)]
rdd = sc.parallelize(months, numSlices=2)

output_rdd = rdd.mapPartitions(data_preprocessing)
results = output_rdd.collect()
print("Results from Spark workers (Saved Files):", results)

end_time = time.perf_counter()

time_spent = end_time - start_time
print(f'time spent in the process was {time_spent}')
input("Spark UI at http://localhost:4040 - press Enter to exit")
