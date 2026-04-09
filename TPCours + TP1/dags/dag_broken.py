from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
import logging 

logger = logging.getLogger(__name__)

@dag(
    dag_id="dag_broken",
    #mettre une date fixe sinon il change a chaque fois que le dag se lance
    start_date=datetime(2024, 1, 1),          
    schedule="@daily",
    # =False sinon il rattrape tous les date manquante
    catchup=False,                             
    default_args={
        # = 2 sinon il n'y a pas de retries 
        "retries": 2,                         
        "owner": "stagiaire",
    }
)
def dag_broken_fixed() -> None:

    @task()
    def extraire() -> dict:                  
        import requests
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=48.8566&longitude=2.3522&current_weather=true"
        )
        return resp.json()["current_weather"]

    @task()
    #Ajout du dict sinon on sait pas ce que data est
    def transformer(data: dict) -> dict:      
        return {
            "temperature": data["temperature"],
            "vent": data["windspeed"],
            "statut": "OK" if data["windspeed"] < 50 else "ALERTE",
        }

    @task()
    def charger(resultat: dict) -> None:
        #logger comme ça on a la log 
        logger.info(f"Résultat : {resultat}")  

    data = extraire()
    resultat = transformer(data)
    charger(resultat)

    chain(data, resultat)                      
dag_broken_fixed()