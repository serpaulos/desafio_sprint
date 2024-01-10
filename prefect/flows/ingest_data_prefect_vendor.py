#!/usr/bin/env python
# coding: utf-8
import os
from datetime import timedelta
from pathlib import Path
import pandas as pd
import requests
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

data_folder = "data"


# @task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=2))
@task(log_prints=True, retries=3)
def download_vendor():
    url = "https://s3.amazonaws.com/data-sprints-eng-test/data-vendor_lookup-csv.csv"

    # split url to get file name
    file_name = url.split('/')[-1]
    dest_folder = data_folder
    file_path = os.path.join(dest_folder, file_name)

    # Chunk size for downloading
    chunk_size = 1024 * 1024  # 1 MB chunks

    # downloading the file sending the request to URL
    req = requests.get(url, stream=True)

    # Determine the total file size from the Content-Length header
    total_size = int(req.headers.get('content-length', 0))

    with open(file_path, 'wb') as file:
        for chunk in req.iter_content(chunk_size):
            if chunk:
                file.write(chunk)
                file_size = file.tell()
                print(f'Downloading... {file_size}/{total_size} bytes', end='\r')


@task(name="extract_vendor_data", log_prints=True, retries=3)
def extract_vendor_data():
    path = Path(f'{data_folder}/data-vendor_lookup-csv.csv')

    df = pd.read_csv(path)

    return df


@task(name="ingest_data", log_prints=True, retries=3)
def ingest_data(table_name, df):
    database_block = SqlAlchemyConnector.load("mysql-conn-sprint-ny-taxi")
    with database_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Ingest flow")
def main_flow(table_name: str):
    download_vendor()
    raw_data = extract_vendor_data()
    ingest_data(table_name, raw_data)


if __name__ == '__main__':
    main_flow("nyctaxi-vendors")
