#!/usr/bin/env python
# coding: utf-8
import os
from datetime import timedelta
from pathlib import Path
from time import time
import pandas as pd
import requests
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from prefect_sqlalchemy import SqlAlchemyConnector

data_folder = "data"


@task(name="download-data", log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_data(year):
    url = f"https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-{year}-json_corrigido.json"

    # Chunk size for downloading
    chunk_size = 1024 * 1024  # 1 MB chunks

    # downloading the file sending the request to URL
    req = requests.get(url, stream=True)

    # Determine the total file size from the Content-Length header
    total_size = int(req.headers.get('content-length', 0))

    # split url to get file name
    file_name = url.split('/')[-1]
    dest_folder = data_folder
    file_path = os.path.join(dest_folder, file_name)

    with open(file_path, 'wb') as file:
        for chunk in req.iter_content(chunk_size):
            if chunk:
                file.write(chunk)
                file_size = file.tell()
                print(f'Downloading... {file_size}/{total_size} bytes', end='\r')


@task(name="extract-data", log_prints=True, retries=3)
def extract_data(year):
    path = Path(f'{data_folder}/data-sample_data-nyctaxi-trips-{year}-json_corrigido.json')
    df = pd.read_json(path, lines=True)

    df.to_csv(f'{data_folder}/data-sample_data-nyctaxi-trips-{year}-json_corrigido.csv', index=False)

    df_csv = pd.read_csv(f'{data_folder}/data-sample_data-nyctaxi-trips-{year}-json_corrigido.csv')

    return df_csv


@task(name="transform-data", log_prints=True, retries=3)
def transform_data(df_csv):
    print(f"pre: Missing passenger count {df_csv['passenger_count'].isin([0]).sum()}")
    df_csv = df_csv[df_csv['passenger_count'] != 0]
    print(f"post: Missing passenger count {df_csv['passenger_count'].isin([0]).sum()}")

    print(f"pre: Missing or null values count {df_csv.isna().sum()}")
    df_csv = df_csv.fillna(0)
    print(f"post: Missing or null values count {df_csv.isna().sum()}")

    return df_csv


@task(name="write_local", log_prints=True, retries=3)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{data_folder}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(name="write-data-gcs", log_prints=True, retries=3)
def write_gcs(path: Path) -> None:
    """Upload file to GCS"""
    gcs_block = GcsBucket.load("gcp-zoomcamp-airflow-zoomcamp")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)
    return


@task(name="ingest-data", log_prints=True, retries=3)
def ingest_data(table_name, year, df_csv):
    database_block = SqlAlchemyConnector.load("mysql-conn-sprint-ny-taxi")
    with database_block.get_connection(begin=False) as engine:
        df_iter = pd.read_csv(f'{data_folder}/data-sample_data-nyctaxi-trips-{year}-json_corrigido.csv', iterator=True, chunksize=100000)

        while True:

            try:
                t_start = time()

                df_csv = next(df_iter)

                df_csv['pickup_datetime'] = pd.to_datetime(df_csv['pickup_datetime'], format='mixed')
                df_csv['dropoff_datetime'] = pd.to_datetime(df_csv['dropoff_datetime'], format='mixed')

                df_csv.to_sql(name=table_name, con=engine, if_exists='append')

                t_end = time()

                print('inserted another chunk, took %.3f second' % (t_end - t_start))

            except StopIteration:
                print("Finished ingesting data into the postgres database")
                break


@flow(name="Ingest flow")
def main_flow(table_name: str, year: int):
    dataset_file = f"data-sample_data-nyctaxi-trips-{year}-json_corrigido"

    download_data(year)
    raw_data = extract_data(year)
    dataf = transform_data(raw_data)
    path = write_local(dataf, dataset_file)
    write_gcs(path)
    ingest_data(table_name, year, dataf)


@flow(name="Parent flow")
def etl_parent_flow(
        table_name: str = "nyctaxi-trips", years: list[int] = []
):
    for year in years:
        main_flow(table_name, year)


if __name__ == '__main__':
    table_name = "nyctaxi-trips"
    year = [2009]
    etl_parent_flow(table_name, year)
