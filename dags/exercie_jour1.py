from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def retourner_date():
    from datetime import datetime
    print(f"Date du jour : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

with DAG(
    dag_id='exercice_jour1',
    default_args=default_args,
    description='Exercice Jour 1 - 3 tâches',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    debut = BashOperator(
        task_id='debut_workflow',
        bash_command='echo "Début du workflow"',
    )

    date_jour = PythonOperator(
        task_id='retourner_date',
        python_callable=retourner_date,
    )
    fin = BashOperator(
        task_id='fin_workflow',
        bash_command='echo "Fin du workflow"',
    )

    debut >> date_jour >> fin