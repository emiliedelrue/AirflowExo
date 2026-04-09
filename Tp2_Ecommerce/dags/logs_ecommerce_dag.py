from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import logging
import os

SEUIL_ERREUR_PCT = 5.0

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# --- Étape 3 ---
def generer_logs_journaliers(**context):
    execution_date = context["ds"]
    fichier_sortie = f"/tmp/access_{execution_date}.log"
    script_path = "/opt/airflow/scripts/generer_logs.py"

    result = subprocess.run(
        ["python3", script_path, execution_date, "1000", fichier_sortie],
        check=True
    )

    taille = os.path.getsize(fichier_sortie)
    logging.info(f"Fichier généré : {fichier_sortie} ({taille} octets)")
    return fichier_sortie

# --- Étape 7 ---
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
        logging.info(f"Taux OK → archivage rapport")
        return "archiver_rapport_ok"

def alerter_equipe_ops(**context):
    execution_date = context["ds"]
    logging.warning(f"[ALERTE] Taux d'erreur HTTP anormal pour les logs du {execution_date} !")
    logging.warning("Vérifiez les serveurs web.")

def archiver_rapport_ok(**context):
    execution_date = context["ds"]
    logging.info(f"[OK] Taux d'erreur dans les seuils normaux pour les logs du {execution_date}")

with DAG(
    dag_id="logs_ecommerce_dag",
    default_args=default_args,
    description="Pipeline logs e-commerce vers HDFS",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "hdfs", "logs"],
) as dag:

    # Étape 3 — Générer les logs
    t_generer = PythonOperator(
        task_id="generer_logs_journaliers",
        python_callable=generer_logs_journaliers,
    )

    # Étape 4 — Upload vers HDFS
    t_upload = BashOperator(
        task_id="uploader_vers_hdfs",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            FICHIER_LOCAL="/tmp/access_${EXECUTION_DATE}.log"
            CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"

            echo "[INFO] Copie vers namenode..."
            docker cp ${FICHIER_LOCAL} namenode:/tmp/access_${EXECUTION_DATE}.log

            echo "[INFO] Upload vers HDFS..."
            docker exec namenode hdfs dfs -put -f /tmp/access_${EXECUTION_DATE}.log ${CHEMIN_HDFS}

            echo "[OK] Upload terminé : ${CHEMIN_HDFS}"
        """,
    )

    # Étape 5 — Sensor HDFS
    t_sensor = BashOperator(
        task_id="hdfs_file_sensor",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"

            echo "[INFO] Vérification existence fichier HDFS..."
            docker exec namenode hdfs dfs -test -e ${CHEMIN_HDFS} && echo "[OK] Fichier présent" || exit 1
        """,
    )

    # Étape 6 — Analyser les logs
    t_analyser = BashOperator(
        task_id="analyser_logs_hdfs",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"

            echo "[INFO] Lecture du fichier HDFS : ${CHEMIN_HDFS}"
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

    # Étape 7 — Branchement
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

    # Étape 8 — Archiver dans HDFS
    t_archiver = BashOperator(
        task_id="archiver_logs_hdfs",
        bash_command="""
            EXECUTION_DATE="{{ ds }}"
            SOURCE="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
            DESTINATION="/data/ecommerce/logs/processed/access_${EXECUTION_DATE}.log"

            echo "[INFO] Déplacement HDFS : ${SOURCE} → ${DESTINATION}"
            docker exec namenode hdfs dfs -mv ${SOURCE} ${DESTINATION}
            echo "[OK] Fichier archivé dans la zone processed"
        """,
        trigger_rule="none_failed_min_one_success",
    )

    # Dépendances
    (
        t_generer
        >> t_upload
        >> t_sensor
        >> t_analyser
        >> t_branch
        >> [t_alerte, t_archive_ok]
        >> t_archiver
    )
