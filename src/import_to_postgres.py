# src/import_to_postgres.py

"""
Classe PostgresImporter
-----------------------
- Se connecte à PostgreSQL via SQLAlchemy
- Utilise pandas.to_sql() pour insérer les données
- Crée les tables yellow_taxi_trips et import_log si elles n’existent pas
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

class PostgresImporter:
    def __init__(self):
        # Configuration de connexion PostgreSQL
        self.user = os.getenv("POSTGRES_USER", "taxi_user")
        self.password = os.getenv("POSTGRES_PASSWORD", "taxi-2025")
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.db = os.getenv("POSTGRES_DB", "taxi_db")

        # Construire l’URL de connexion SQLAlchemy
        self.db_url = f"postgresql://{self.user}:{self.password}@{self.host}/{self.db}"
        self.engine = create_engine(self.db_url)

        print(f"[INFO] Connexion à PostgreSQL établie sur {self.db}")

    def import_yellow_taxi_data(self, csv_path: str):
        """
        Importe les données du fichier CSV dans la table yellow_taxi_trips.
        """
        print(f"[INFO] Import du fichier : {csv_path}")
        df = pd.read_csv(csv_path)

        # Insérer dans la base via pandas.to_sql()
        df.to_sql(
            "yellow_taxi_trips",
            self.engine,
            if_exists="append",  # append → ajoute les données sans supprimer
            index=False,
            chunksize=10_000
        )
        print(f"[OK] {len(df)} lignes insérées dans yellow_taxi_trips")

        # Ajouter une entrée dans le journal d'import
        self.log_import(csv_path, len(df))

    def log_import(self, file_name: str, row_count: int):
        """
        Insère une ligne dans la table import_log.
        """
        log_df = pd.DataFrame([{
            "file_name": file_name,
            "row_count": row_count
        }])
        log_df.to_sql(
            "import_log",
            self.engine,
            if_exists="append",
            index=False
        )
        print(f"[LOG] Import de {file_name} enregistré dans import_log")

if __name__ == "__main__":
    importer = PostgresImporter()
    importer.import_yellow_taxi_data("data/yellow_tripdata_2024-01.csv")
