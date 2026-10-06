import ray
import pandas as pd
import time
import uuid
import os

start_time = time.perf_counter()
ray.init(address='auto')

def data_preprocessing(batch):
    os.chdir('/home/raghav-iyengar/DA24B047_3')
    month_ids = batch['month']
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

    return pd.DataFrame({'file': saved_files})

month_ids_1 = pd.DataFrame({'month': [j for j in range(1,4)]})
month_ids_2 = pd.DataFrame({'month': [j for j in range(4,7)]})

ds = ray.data.from_pandas([month_ids_1, month_ids_2])

results = ds.map_batches(data_preprocessing, batch_format = 'pandas', batch_size = 3).take_all()
print("Results from Ray workers (Saved Files):", results)

end_time = time.perf_counter()

time_spent = end_time - start_time
print(f'time spent in the process was {time_spent}')
ray.shutdown()
