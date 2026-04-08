from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def print_hello():
    print("Hello from Airflow!")

with DAG(
    dag_id='hello_world',
    default_args=default_args,
    description='Mon premier DAG',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    task_hello = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    task_bash = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    task_hello >> task_bash
