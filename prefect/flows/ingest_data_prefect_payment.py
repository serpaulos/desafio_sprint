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


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_payment():
    url = "https://s3.amazonaws.com/data-sprints-eng-test/data-payment_lookup-csv.csv"

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


@task(log_prints=True, retries=3)
def extract_payment_data():
    path = Path(f'{data_folder}/data-payment_lookup-csv.csv')

    df = pd.read_csv(path)

    return df


@task(log_prints=True, retries=3)
def transform_data(df):
    # payment csv

    # delete unnecessary lines since they have values like this
    """
    3284          3267             Foo
    3285          3268             Foo
    3286          3269             Foo
    """
    df = df[1:18]

    # convert values to lower to delete duplicates
    df['A'] = df['A'].astype(str).str.lower()
    df['B'] = df['B'].astype(str).str.lower()

    # renaming column payment_type   payment_lookup
    df.rename(columns={'A': 'payment_type', 'B': 'payment_lookup'}, inplace=True)

    # delete duplicate values
    df = df.drop_duplicates(subset=['payment_type'], keep='first')

    """
           payment_type payment_lookup
    1           cas           cash
    3           cre         credit
    5           no       no charge
    6           dis        dispute
    7          cash           cash
    9        credit         credit
    11    no charge      no charge
    12      dispute        dispute
    13          crd         credit
    14          csh           cash
    15          noc      no charge
    17          unk        unknown
    """

    # clean unnecessary values
    df = df.drop(labels=[5, 7, 9, 11, 12, 13, 14], axis=0)

    df = df.reset_index()

    return df


@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    database_block = SqlAlchemyConnector.load("mysql-conn-sprint-ny-taxi")
    with database_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Ingest flow")
def main_flow(table_name: str):
    download_payment()
    raw_data = extract_payment_data()
    data = transform_data(raw_data)
    ingest_data(table_name, data)


if __name__ == '__main__':
    main_flow("nyctaxi-payments")
