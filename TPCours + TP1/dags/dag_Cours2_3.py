from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def fichier_detecte(**context):
    logging.info("=" * 40)
    logging.info("Fichier /tmp/go.txt détecté !")
    logging.info("Pipeline débloqué !")
    logging.info("=" * 40)

def traitement_final(**context):
    logging.info("Traitement lancé après détection.")
    logging.info("Pipeline terminé avec succès !")

with DAG(
    dag_id="sensor_fichier",
    default_args=default_args,
    description="Exercice 3 — Sensor fichier /tmp/go.txt",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sensor", "exercice"],
) as dag:

    attendre_fichier = FileSensor(
        task_id="attendre_fichier",
        filepath="/tmp/go.txt",
        poke_interval=10,
        timeout=300,
        mode="reschedule", 
    )

    t2 = PythonOperator(
        task_id="fichier_detecte",
        python_callable=fichier_detecte,
    )

    t3 = PythonOperator(
        task_id="traitement_final",
        python_callable=traitement_final,
    )

    attendre_fichier >> t2 >> t3