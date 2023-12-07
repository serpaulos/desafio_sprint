from datetime import datetime
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator


with DAG(dag_id='mysql_operator_example', start_date=datetime(2023, 1, 1)) as dag:
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='airflow_mysql_sprint',
        database="mysql_sprint",
        sql="CREATE TABLE IF NOT EXISTS my_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255));",
    )

    insert_data = MySqlOperator(
        task_id='insert_data',
        mysql_conn_id='airflow_mysql_sprint',
        database="mysql_sprint",
        sql="INSERT INTO my_table (name) VALUES ('John Doe');",
    )

    create_table >> insert_data
