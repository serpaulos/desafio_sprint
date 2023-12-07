from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
import os
from airflow.operators.python import PythonOperator
from data_ingestion_callable import download, clean_task, convert_parquet
from data_upload_callable import upload_bucket_bronze_s3, upload_bucket_silver_s3

load_dotenv()

# MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
# MYSQL_USER = os.getenv('MYSQL_USER')
# MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')


DEFAULT_ARGS = {
    'owner': 'PSergios',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 13),
}

with DAG(
        'etl_carrega_bucket',
        default_args=DEFAULT_ARGS,
        description='DAG para teste de execução de dag',
        schedule_interval=timedelta(days=1),
        catchup=False
) as dag:
    ingest_task = PythonOperator(
        task_id="download_data",
        python_callable=download,
        do_xcom_push=True,
        op_kwargs=dict(
            year=2009
        ),
    )
    upload_task_bronze = PythonOperator(
        task_id="upload_bucket_bronze",
        python_callable=upload_bucket_bronze_s3,
    )

    clean_task = PythonOperator(
        task_id="clean_task",
        python_callable=clean_task,
    )

    convert_parquet = PythonOperator(
        task_id="convert_parquet",
        python_callable=convert_parquet,
    )

    upload_task_silver = PythonOperator(
        task_id="upload_bucket_silver",
        python_callable=upload_bucket_silver_s3,
    )


ingest_task >> upload_task_bronze >> clean_task >> convert_parquet >> upload_task_silver