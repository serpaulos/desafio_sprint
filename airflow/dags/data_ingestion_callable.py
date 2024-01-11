import requests
import os
import pandas as pd

bronze_bucket = 'bronze'
folder_location = 'air_data'


def download(year) -> str:
    print("Download started")
    url = f"https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-{year}-json_corrigido.json"

    # Chunk size for downloading
    chunk_size = 1024 * 1024  # 1 MB chunks

    # downloading the file sending the request to URL
    req = requests.get(url, stream=True)

    # Determine the total file size from the Content-Length header
    total_size = int(req.headers.get('content-length', 0))

    # split url to get file name
    dest_folder = folder_location
    file_name = url.split('/')[-1]
    file_path = os.path.join(dest_folder, file_name)

    with open(file_path, 'wb') as file:
        for chunk in req.iter_content(chunk_size):
            if chunk:
                file.write(chunk)
                file_size = file.tell()
                print(f'Downloading... {file_size}/{total_size} bytes', end='\r')

    return file_name  # data-sample_data-nyctaxi-trips-2010-json_corrigido.json


def clean_task(ti):
    filename = ti.xcom_pull(task_ids="upload_bucket_bronze")

    # Read JSON into DataFrame
    df = pd.read_json(filename, lines=True)
    print(f"pre: Missing passenger count {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: Missing passenger count {df['passenger_count'].isin([0]).sum()}")

    print(f"pre: Missing passenger count {df.isna().sum()}")
    df = df.fillna(0)
    print(f"post: Missing passenger count {df.isna().sum()}")

    # df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], format='mixed').dt.tz_convert(None)
    # df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'], format='mixed').dt.tz_convert(None)
    # change this line in order to be accoring to the pattern
    # df.at[498551, 'pickup_datetime'] = '2009-04-02 23:37:30.297216+00:00'
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], format='mixed').dt.tz_convert(None)
    df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'], format='mixed').dt.tz_convert(None)

    print("Data Types of The Columns in Data Frame")
    print(df.dtypes)

    return df


def convert_parquet(ti):
    dataframe = ti.xcom_pull(task_ids="clean_task")
    dest_folder = folder_location
    filename = ti.xcom_pull(task_ids="download_data")

    directory = f'{dest_folder}/{filename}'
    file_name = os.path.basename(directory).split('.')[0]

    # Write Arrow Table to Parquet file
    path = f'{dest_folder}/{file_name}.parquet'
    dataframe.to_parquet(path, compression="gzip")
    return path


