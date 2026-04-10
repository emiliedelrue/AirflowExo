from airflow.sensors.base import BaseSensorOperator
import requests
import logging

class HdfsFileSensor(BaseSensorOperator):
    """
    Sensor personnalisé qui vérifie qu'un fichier existe dans HDFS
    via l'API WebHDFS REST.
    """

    def __init__(self, hdfs_path: str, namenode_url: str, **kwargs):
        super().__init__(**kwargs)
        self.hdfs_path = hdfs_path
        self.namenode_url = namenode_url

    def poke(self, context) -> bool:
        """
        Appelée à chaque intervalle de vérification.
        Retourne True si le fichier existe → sensor terminé.
        Retourne False pour continuer à attendre.
        """
        url = f"{self.namenode_url}/webhdfs/v1{self.hdfs_path}"
        params = {
            "op": "GETFILESTATUS",
            "user.name": "root"
        }

        try:
            resp = requests.get(url, params=params, timeout=5)

            if resp.status_code == 200:
                size = resp.json()["FileStatus"]["length"]
                self.log.info(
                    f"Fichier trouvé : {self.hdfs_path} ({size} bytes)"
                )
                return True
            else:
                self.log.warning(
                    f"Fichier absent — status HTTP : {resp.status_code}"
                )
                return False

        except Exception as exc:
            self.log.warning(f"Fichier absent ou HDFS KO : {exc}")
            return False
