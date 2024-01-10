#!/usr/bin/env python
# coding: utf-8
import requests
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
import pymysql


def ingest_data(user, password, host, port, db, table_name, year):
    url = f"https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-{year}-json_corrigido.json"

    folder_location = 'data'

    # split url to get file name
    file_name = url.split('/')[-1]
    dest_folder = folder_location
    file_path = os.path.join(dest_folder, file_name)

    # Chunk size for downloading
    chunk_size = 1024 * 1024  # 1 MB chunks

    # downloading the file sending the request to URL
    req = requests.get(url, stream=True)

    # Determine the total file size from the Content-Length header
    total_size = int(req.headers.get('content-length', 0))

    # split url to get file name
    file_name = url.split('/')[-1]
    dest_folder = folder_location
    file_path = os.path.join(dest_folder, file_name)

    with open(file_path, 'wb') as file:
        for chunk in req.iter_content(chunk_size):
            if chunk:
                file.write(chunk)
                file_size = file.tell()
                print(f'Downloading... {file_size}/{total_size} bytes', end='\r')

    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")

    data = f'data/data-sample_data-nyctaxi-trips-{year}-json_corrigido.json'
    df = pd.read_json(data, lines=True)

    df.to_csv(f'data/data-sample_data-nyctaxi-trips-{year}.csv', index=False)

    df_csv = pd.read_csv(f'data/data-sample_data-nyctaxi-trips-{year}.csv')

    df_iter = pd.read_csv(f'data/data-sample_data-nyctaxi-trips-{year}.csv', iterator=True, chunksize=100000)

    df_csv = next(df_iter)

    df_csv.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df_csv.to_sql(name=table_name, con=engine, if_exists='append')

    while True:

        try:
            t_start = time()

            df = next(df_iter)

            df_csv['pickup_datetime'] = pd.to_datetime(df_csv['pickup_datetime'], format='mixed')
            df_csv['dropoff_datetime'] = pd.to_datetime(df_csv['dropoff_datetime'], format='mixed')

            df_csv.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == '__main__':
    user = "sprint_prefect"
    password = "sprint_prefect"
    host = "localhost"
    port = "3306"
    db = "sprint_ny_taxi_db"
    table_name = "nyctaxi-trips"
    year = 2009

    ingest_data(user, password, host, port, db, table_name, year)
