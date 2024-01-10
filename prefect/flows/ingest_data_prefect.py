#!/usr/bin/env python
# coding: utf-8
from pathlib import Path

import requests
import os
import argparse
from datetime import timedelta
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

data_folder = "data"


@task(name="download_data", log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_data(year):
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


@task(name="extract_data", log_prints=True, retries=3)
def extract_data(year):
    path = Path(f'{data_folder}/data-sample_data-nyctaxi-trips-{year}-json_corrigido.json')
    df = pd.read_json(path, lines=True)

    df.to_csv(f'data/data-sample_data-nyctaxi-trips-{year}.csv', index=False)

    df_iter = pd.read_csv(f'data/data-sample_data-nyctaxi-trips-{year}.csv', iterator=True, chunksize=100000)

    df_csv = next(df_iter)

    df_csv['pickup_datetime'] = pd.to_datetime(df_csv['pickup_datetime'], format='mixed')
    df_csv['dropoff_datetime'] = pd.to_datetime(df_csv['dropoff_datetime'], format='mixed')

    return df_csv


@task(name="transform_data", log_prints=True, retries=3)
def transform_data(df_csv):
    print(f"pre: Missing passenger count {df_csv['passenger_count'].isin([0]).sum()}")
    df_csv = df_csv[df_csv['passenger_count'] != 0]
    print(f"post: Missing passenger count {df_csv['passenger_count'].isin([0]).sum()}")

    print(f"pre: Missing or null values count {df_csv.isna().sum()}")
    df_csv = df_csv.fillna(0)
    print(f"post: Missing or null values count {df_csv.isna().sum()}")

    return df_csv


@task(name="ingest_data", log_prints=True, retries=3)
def ingest_data(table_name, df_csv):
    database_block = SqlAlchemyConnector.load("mysql-conn-sprint-ny-taxi")
    with database_block.get_connection(begin=False) as engine:
        df_csv.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df_csv.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Ingest flow")
def main_flow(table_name: str):
    year = 2009
    raw_data = extract_data(year)
    data = transform_data(raw_data)
    ingest_data(table_name, data)


if __name__ == '__main__':
    main_flow("nyctaxi-trips")
