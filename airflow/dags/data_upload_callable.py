from dotenv import load_dotenv
from airflow.hooks.S3_hook import S3Hook

load_dotenv()

raw_bucket = 'bronze'
cleaned_bucket = 'silver'
folder_location = 'air_data'


def upload_bucket_bronze_s3(ti):
    filename = ti.xcom_pull(task_ids="download_data")
    if not filename:
        raise ValueError("No value set on xcom store")
    hook = S3Hook('my_s3_conn')
    dest_folder = folder_location
    hook.load_file(
        filename=f"{dest_folder}/{filename}",
        key=f"{dest_folder}/{filename}",
        bucket_name=raw_bucket,
        replace=True
    )
    return f"{dest_folder}/{filename}"


def upload_bucket_silver_s3(ti):
    filename = ti.xcom_pull(task_ids="convert_parquet")
    if not filename:
        raise ValueError("No value set on xcom store")
    hook = S3Hook('my_s3_conn')
    hook.load_file(
        filename=f"{filename}",
        key=f"{filename}",
        bucket_name=cleaned_bucket,
        replace=True
    )
    #return f"{dest_folder}/{filename}"
