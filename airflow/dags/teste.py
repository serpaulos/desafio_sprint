import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator

TEST_BUCKET = 'bronze'


def task1():
    print("Logic to extract data")
    print(f"the valuue of xcom pull object is {TEST_BUCKET}")
    rtn_val = "value passed"
    return rtn_val


def task2(ti):
    xcom_pull_obj = ti.xcom_pull(task_ids=["create_file"])
    print(f"type of xom object is {type(xcom_pull_obj)}")
    extract_rtn_obj = xcom_pull_obj[0]
    print(f"the valuue of xcom pull object is {extract_rtn_obj}")
    print("Logic to transform data")
    return 10


with DAG(
        dag_id='s3_hook_overview',
        schedule_interval='@daily',
        start_date=datetime(2023, 9, 1),
        catchup=False
) as dag:
    task_create_file = PythonOperator(
        task_id='create_file',
        python_callable=task1
    )

    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=task2
    )

    task_create_file >> task_upload_to_s3