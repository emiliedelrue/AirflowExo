from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import logging
import os
import sys

sys.path.insert(0, '/opt/airflow/plugins')
from hdfs_sensor import HdfsFileSensor

SEUIL_ERREUR_PCT = 5.0
NAMENODE_URL = "http://namenode:9870"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def generer_logs_journaliers(**context):
    execution_date = context["ds"]
    fichier_sortie = f"/tmp/access_{execution_date}.log"
    script_path = "/opt/airflow/scripts/generer_logs.py"
    subprocess.run(
        ["python3", script_path, execution_date, "1000", fichier_sortie],
        check=True
    )
    taille = os.path.getsize(fichier_sortie)
    logging.info(f"Fichier généré : {fichier_sortie} ({taille} octets)")
    return fichier_sortie

def brancher_selon_taux_erreur(**context):
    execution_date = context["ds"]
    fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"
    with open(fichier_taux, "r") as f:
        contenu = f.read().strip()
    erreurs, total = contenu.split()
    erreurs = int(erreurs)
    total = int(total)
    taux_pct = (erreurs / total) * 100 if total > 0 else 0
    logging.info(f"Taux d'erreur : {taux_pct:.2f}% ({erreurs}/{total})")
    if taux_pct > SEUIL_ERREUR_PCT:
        logging.warning(f"Taux > {SEUIL_ERREUR_PCT}% → alerte OPS")
        return "alerter_equipe_ops"
    else:
        logging.info("Taux OK → archivage rapport")
        return "archiver_rapport_ok"

def alerter_equipe_ops(**context):
    execution_date = context["ds"]
    logging.warning(f"[ALERTE] Taux d'erreur anormal pour {execution_date} !")

def archiver_rapport_ok(**context):
    execution_date = context["ds"]
    logging.info(f"[OK] Taux d'erreur normal pour {execution_date}")

with DAG(
    dag_id="logs_ecommerce_hdfs_sensor",
    default_args=default_args,
    description="Pipeline avec HdfsSensor personnalisé",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "hdfs", "sensor", "exercice"],
) as dag:

    t_generer = PythonOperator(
        task_id="generer_logs_journaliers",
        python_callable=generer_logs_journaliers,
    )

    t_upload = BashOperator(
        task_id="uploader_vers_hdfs",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            FICHIER_LOCAL="/tmp/access_${EXECUTION_DATE}.log"
            CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
            docker cp ${FICHIER_LOCAL} namenode:/tmp/access_${EXECUTION_DATE}.log
            docker exec namenode hdfs dfs -put -f /tmp/access_${EXECUTION_DATE}.log ${CHEMIN_HDFS}
            echo "[OK] Upload terminé"
        """,
    )

    # ← Notre sensor personnalisé !
    t_sensor = HdfsFileSensor(
        task_id="attendre_fichier_hdfs",
        hdfs_path="/data/ecommerce/logs/raw/access_{{ ds }}.log",
        namenode_url=NAMENODE_URL,
        poke_interval=30,   # vérifie toutes les 30 secondes
        timeout=600,        # abandonne après 10 minutes
        mode="reschedule",  # libère le worker entre chaque vérification
    )

    t_analyser = BashOperator(
        task_id="analyser_logs_hdfs",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
            docker exec namenode hdfs dfs -cat "${CHEMIN_HDFS}" > /tmp/logs_analyse_${EXECUTION_DATE}.txt

            echo "=== STATUS CODES ==="
            grep -oP '"[A-Z]+ [^ ]+ HTTP/[0-9.]+" [0-9]+' /tmp/logs_analyse_${EXECUTION_DATE}.txt \
                | grep -oP '[0-9]+$' | sort | uniq -c | sort -rn

            echo "=== TOP 5 URLS ==="
            grep -oP '"(GET|POST) [^ ]+' /tmp/logs_analyse_${EXECUTION_DATE}.txt \
                | cut -d' ' -f2 | sort | uniq -c | sort -rn | head -5

            TOTAL=$(wc -l < /tmp/logs_analyse_${EXECUTION_DATE}.txt)
            ERREURS=$(grep -cP '"[A-Z]+ [^ ]+ HTTP/[0-9.]+" (4|5)[0-9]{2}' \
                /tmp/logs_analyse_${EXECUTION_DATE}.txt || echo 0)

            echo "=== TAUX ERREUR ==="
            echo "Total: ${TOTAL}, Erreurs: ${ERREURS}"
            echo "${ERREURS} ${TOTAL}" > /tmp/taux_erreur_${EXECUTION_DATE}.txt
        """,
    )

    t_branch = BranchPythonOperator(
        task_id="brancher_selon_taux_erreur",
        python_callable=brancher_selon_taux_erreur,
    )

    t_alerte = PythonOperator(
        task_id="alerter_equipe_ops",
        python_callable=alerter_equipe_ops,
    )

    t_archive_ok = PythonOperator(
        task_id="archiver_rapport_ok",
        python_callable=archiver_rapport_ok,
    )

    t_archiver = BashOperator(
        task_id="archiver_logs_hdfs",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            SOURCE="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
            DESTINATION="/data/ecommerce/logs/processed/access_${EXECUTION_DATE}.log"
            docker exec namenode hdfs dfs -mv ${SOURCE} ${DESTINATION}
            echo "[OK] Fichier archivé"
        """,
        trigger_rule="none_failed_min_one_success",
    )

    (
        t_generer
        >> t_upload
        >> t_sensor
        >> t_analyser
        >> t_branch
        >> [t_alerte, t_archive_ok]
        >> t_archiver
    )
