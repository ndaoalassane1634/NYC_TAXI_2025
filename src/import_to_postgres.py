# src/import_to_postgres.py

"""
Classe PostgresImporter
-----------------------
- Se connecte à PostgreSQL via SQLAlchemy
- Utilise pandas.to_sql() pour insérer les données
- Crée les tables yellow_taxi_trips et import_log si elles n’existent pas
"""

# src/import_to_postgres.py
import os
import pandas as pd
from sqlalchemy import text
from src.api.database import engine
from dotenv import load_dotenv

load_dotenv()

class PostgresImporter:
    def __init__(self):
        self.engine = engine
        print("[INFO] Connexion PostgreSQL OK ")

    # --- Crée une table en fonction d’un DataFrame ---
    def create_table_from_df(self, table_name: str, df: pd.DataFrame):
        """Crée automatiquement une table PostgreSQL à partir du schéma d'un DataFrame"""
        dtype_mapping = {
            "int64": "BIGINT",
            "float64": "DOUBLE PRECISION",
            "object": "TEXT",
            "datetime64[ns]": "TIMESTAMP",
            "bool": "BOOLEAN"
        }

        # Conversion dynamique des colonnes
        columns = []
        for col, dtype in df.dtypes.items():
            pg_type = dtype_mapping.get(str(dtype), "TEXT")
            columns.append(f'"{col}" {pg_type}')
        
        create_sql = ",\n    ".join(columns)

        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {create_sql}
        );
        """

        with self.engine.begin() as conn:
            conn.execute(text(query))

        print(f"[INFO] Table '{table_name}' créée ou déjà existante.")

    # --- Importe un fichier Parquet ---
    def import_parquet(self, parquet_path: str, table_name: str):
        print(f"[INFO] Import du fichier : {parquet_path}")
        df = pd.read_parquet(parquet_path)

        # Normaliser les noms de colonnes
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        # Créer la table si elle n’existe pas encore
        self.create_table_from_df(table_name, df)

        # Insérer dans PostgreSQL
        df.to_sql(
            table_name,
            self.engine,
            if_exists="append",
            index=False,
            chunksize=10_000
        )
        print(f"[OK] {len(df)} lignes importées dans {table_name}")

        # Journaliser
        self.log_import(parquet_path, len(df))

    # --- Journal des imports ---
    def log_import(self, file_name: str, row_count: int):
        log_df = pd.DataFrame([{
            "file_name": file_name,
            "row_count": row_count,
            "status": "imported"
        }])

        # Crée la table de log si elle n'existe pas
        self.create_table_from_df("import_logs", log_df)

        log_df.to_sql(
            "import_logs",
            self.engine,
            if_exists="append",
            index=False
        )
        print(f"[LOG] Import de {file_name} enregistré ({row_count} lignes).")


if __name__ == "__main__":
    importer = PostgresImporter()


