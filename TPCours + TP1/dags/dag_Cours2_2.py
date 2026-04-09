from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import logging

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Tâche 1 — Génère une liste de 5 nombres et la retourne
def generer_liste(**context):
    liste = [random.randint(1, 100) for _ in range(5)]
    logging.info(f"Liste générée : {liste}")
    return liste 

# Tâche 2 — Récupère la liste via XCom et calcule la somme
def calculer_somme(**context):
    ti = context["ti"]
    liste = ti.xcom_pull(task_ids="generer_liste")  
    somme = sum(liste)
    logging.info(f"Liste reçue : {liste}")
    logging.info(f"Somme calculée : {somme}")
    return somme  

# Tâche 3 — Affiche le résultat final
def afficher_resultat(**context):
    ti = context["ti"]
    liste = ti.xcom_pull(task_ids="generer_liste")
    somme = ti.xcom_pull(task_ids="calculer_somme")
    logging.info("=" * 40)
    logging.info(f"Liste : {liste}")
    logging.info(f"Somme : {somme}")
    logging.info("=" * 40)

with DAG(
    dag_id="xcom_communication",
    default_args=default_args,
    description="Exercice XCom — liste de nombres",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["xcom", "exercice"],
) as dag:

    t1 = PythonOperator(
        task_id="generer_liste",
        python_callable=generer_liste,
    )

    t2 = PythonOperator(
        task_id="calculer_somme",
        python_callable=calculer_somme,
    )

    t3 = PythonOperator(
        task_id="afficher_resultat",
        python_callable=afficher_resultat,
    )

    t1 >> t2 >> t3