from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import random
import logging

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def generer_nombre(**context):
    nombre = random.randint(1, 100)
    logging.info(f"Nombre généré : {nombre}")
    return nombre

def choisir_branche(**context):
    ti = context["ti"]
    nombre = ti.xcom_pull(task_ids="generer_nombre")
    if nombre % 2 == 0:
        logging.info(f"{nombre} est pair → branche pair")
        return "tache_pair"
    else:
        logging.info(f"{nombre} est impair → branche impair")
        return "tache_impair"

def tache_pair(**context):
    ti = context["ti"]
    nombre = ti.xcom_pull(task_ids="generer_nombre")
    logging.info(f"Le nombre {nombre} est PAIR !")

def tache_impair(**context):
    ti = context["ti"]
    nombre = ti.xcom_pull(task_ids="generer_nombre")
    logging.info(f"Le nombre {nombre} est IMPAIR !")

def tache_finale(**context):
    ti = context["ti"]
    nombre = ti.xcom_pull(task_ids="generer_nombre")
    logging.info("=" * 40)
    logging.info(f"Tâche finale — nombre traité : {nombre}")
    logging.info("Pipeline terminé !")
    logging.info("=" * 40)

with DAG(
    dag_id="dag_branchement",
    default_args=default_args,
    description="Exercice 1 — Branchement pair/impair",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["branchement", "exercice"],
) as dag:

    t1 = PythonOperator(
        task_id="generer_nombre",
        python_callable=generer_nombre,
    )

    t2 = BranchPythonOperator(
        task_id="choisir_branche",
        python_callable=choisir_branche,
    )

    t3_pair = PythonOperator(
        task_id="tache_pair",
        python_callable=tache_pair,
    )

    t3_impair = PythonOperator(
        task_id="tache_impair",
        python_callable=tache_impair,
    )

    t4 = PythonOperator(
        task_id="tache_finale",
        python_callable=tache_finale,
        trigger_rule="none_failed_min_one_success",
    )

    t1 >> t2 >> [t3_pair, t3_impair] >> t4