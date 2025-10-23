

import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient
from typing import Dict, Iterator, Tuple
import datetime

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_running_in_docker() -> bool:
    return os.getenv("RUNNING_IN_DOCKER", "false").lower() == "true"

class DataCleaner:
    """
    Classe pour charger, nettoyer, analyser et sauvegarder les données des taxis de NYC.
    """

    def __init__(self):
        logging.info("Initialisation de DataCleaner...")
        self.postgres_engine = self._get_postgres_engine()
        self.mongo_client = self._get_mongo_client()
        
        if self.mongo_client:
            self.mongo_db = self.mongo_client[os.getenv("MONGO_DATABASE", "nyc_taxi")]
            self.collection = self.mongo_db["cleaned_trips"]
            logging.info(f"Connecté à la base de données MongoDB: {self.mongo_db.name}")
            logging.info(f"Collection initialisée: {self.collection.name}")
        else:
            self.mongo_db = None
            self.collection = None

    def _get_postgres_engine(self):
        try:
            user = os.getenv("POSTGRES_USER", "taxi_user")
            password = os.getenv("POSTGRES_PASSWORD", "taxi_2025")
            host = "postgres" if is_running_in_docker() else "localhost"
            port = os.getenv("POSTGRES_PORT", "5432")
            db = os.getenv("POSTGRES_DB", "taxi_db")
            url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
            return create_engine(url)
        except Exception as e:
            logging.error(f"Erreur lors de la création du moteur PostgreSQL: {e}")
            return None

    def _get_mongo_client(self):
        try:
            user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin")
            password = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "admin123")
            host = "mongodb" if is_running_in_docker() else "localhost"
            port = os.getenv("MONGO_PORT", "27017")
            url = f"mongodb://{user}:{password}@{host}:{port}/"
            client = MongoClient(url)
            client.admin.command('ping')
            return client
        except Exception as e:
            logging.error(f"Erreur de connexion à MongoDB: {e}")
            return None

    def load_data_from_postgres(self, chunksize: int = 100000) -> Iterator[pd.DataFrame]:
        if not self.postgres_engine:
            return iter([])
        try:
            logging.info(f"Chargement des données depuis PostgreSQL par chunks de {chunksize}...")
            query = "SELECT * FROM yellow_taxi_trips"
            return pd.read_sql(query, self.postgres_engine, chunksize=chunksize)
        except Exception as e:
            logging.error(f"Erreur lors du chargement des données: {e}")
            return iter([])

    def clean_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Nettoie un DataFrame et retourne le df nettoyé ainsi qu'un dictionnaire de stats.
        """
        rows_before = len(df)
        stats = {
            "nulls_removed": 0,
            "negatives_removed": 0,
            "outliers_removed": 0
        }

        # 1. Valeurs nulles
        df_cleaned = df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
        stats["nulls_removed"] = rows_before - len(df_cleaned)
        
        # 2. Valeurs négatives
        rows_after_nulls = len(df_cleaned)
        negative_cols = ['passenger_count', 'trip_distance', 'fare_amount', 'tip_amount', 'tolls_amount', 'total_amount']
        for col in negative_cols:
            df_cleaned = df_cleaned[df_cleaned[col] >= 0]
        stats["negatives_removed"] = rows_after_nulls - len(df_cleaned)

        # 3. Outliers
        rows_after_negatives = len(df_cleaned)
        df_cleaned = df_cleaned[
            (df_cleaned['passenger_count'].between(1, 8)) &
            (df_cleaned['trip_distance'] <= 100) &
            (df_cleaned['fare_amount'] <= 500)
        ]
        stats["outliers_removed"] = rows_after_negatives - len(df_cleaned)
        
        return df_cleaned, stats

    def clear_collection(self):
        if self.collection is not None:
            logging.info(f"Suppression des documents de '{self.collection.name}'...")
            count = self.collection.delete_many({}).deleted_count
            logging.info(f"{count} documents supprimés.")

    def save_to_mongodb(self, df: pd.DataFrame) -> int:
        if self.collection is None or df.empty:
            return 0
        try:
            records = df.to_dict('records')
            for rec in records:
                rec.pop('id', None)
                for key, value in rec.items():
                    if isinstance(value, pd.Timestamp):
                        rec[key] = value.to_pydatetime()
            
            result = self.collection.insert_many(records)
            return len(result.inserted_ids)
        except Exception as e:
            logging.error(f"Erreur lors de la sauvegarde dans MongoDB: {e}")
            return 0

    def close(self):
        if self.mongo_client:
            self.mongo_client.close()
            logging.info("Connexion MongoDB fermée.")

if __name__ == "__main__":
    logging.info("Démarrage du processus de nettoyage des données.")
    cleaner = DataCleaner()
    
    # Initialisation des statistiques globales
    global_stats = {
        "total_rows_read": 0,
        "total_rows_inserted": 0,
        "total_nulls_removed": 0,
        "total_negatives_removed": 0,
        "total_outliers_removed": 0
    }

    try:
        cleaner.clear_collection()
        chunk_iterator = cleaner.load_data_from_postgres(chunksize=100000)
        
        for i, chunk_df in enumerate(chunk_iterator):
            logging.info(f"--- Traitement du Chunk #{i+1} ---")
            global_stats["total_rows_read"] += len(chunk_df)
            
            # Nettoyer le chunk et récupérer ses stats
            cleaned_chunk, chunk_stats = cleaner.clean_data(chunk_df)
            
            # Mettre à jour les stats globales
            global_stats["total_nulls_removed"] += chunk_stats["nulls_removed"]
            global_stats["total_negatives_removed"] += chunk_stats["negatives_removed"]
            global_stats["total_outliers_removed"] += chunk_stats["outliers_removed"]
            
            # Sauvegarder le chunk nettoyé
            inserted_count = cleaner.save_to_mongodb(cleaned_chunk)
            global_stats["total_rows_inserted"] += inserted_count
            logging.info(f"{len(chunk_df) - inserted_count} lignes supprimées dans ce chunk. {inserted_count} insérées.")

        # Afficher le résumé final
        logging.info("\n--- PROCESSUS TERMINÉ : RÉSUMÉ GLOBAL ---")
        logging.info(f"Lignes totales lues depuis PostgreSQL: {global_stats['total_rows_read']}")
        logging.info(f"Lignes avec dates nulles supprimées: {global_stats['total_nulls_removed']}")
        logging.info(f"Lignes avec valeurs négatives supprimées: {global_stats['total_negatives_removed']}")
        logging.info(f"Lignes avec outliers supprimées: {global_stats['total_outliers_removed']}")
        logging.info("--------------------------------------------------")
        logging.info(f"Lignes totales insérées dans MongoDB: {global_stats['total_rows_inserted']}")
        total_removed = global_stats['total_rows_read'] - global_stats['total_rows_inserted']
        logging.info(f"Nombre total de lignes supprimées: {total_removed}")

    finally:
        cleaner.close()

