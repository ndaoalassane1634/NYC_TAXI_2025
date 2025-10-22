import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv


class PostgresImporter:
    def __init__(self):
        """ 
            Chargement des variables d'environnement avec dotenv
        """
        load_dotenv()
        self.db_user = os.getenv("POSTGRES_USER", "postgres")
        self.db_pass = os.getenv("POSTGRES_PASSWORD", "postgres")
        self.db_name = os.getenv("POSTGRES_DB", "nyc_taxi")
        self.db_host = os.getenv("POSTGRES_HOST", "localhost")
        self.db_port = os.getenv("POSTGRES_PORT", "5432")

        # Connexion PostgreSQL via SQLAlchemy
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    def create_tables(self):
        """
            Créer les tables si elles n'existent pas
        """
        create_yellow_taxi_table = """
        CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
            vendor_id INTEGER,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DOUBLE PRECISION,
            ratecode_id INTEGER,
            store_and_fwd_flag TEXT,
            pu_location_id INTEGER,
            do_location_id INTEGER,
            payment_type INTEGER,
            fare_amount DOUBLE PRECISION,
            extra DOUBLE PRECISION,
            mta_tax DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            tolls_amount DOUBLE PRECISION,
            improvement_surcharge DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,
            congestion_surcharge DOUBLE PRECISION,
            airport_fee DOUBLE PRECISION
        );
        """

        create_log_table = """
        CREATE TABLE IF NOT EXISTS import_log (
            id SERIAL PRIMARY KEY,
            file_name TEXT NOT NULL,
            import_date TIMESTAMP NOT NULL,
            rows_imported INTEGER NOT NULL
        );
        """

        with self.engine.connect() as conn:
            conn.execute(text(create_yellow_taxi_table))
            conn.execute(text(create_log_table))
            conn.commit()

        print("Tables créées ou déjà existantes.")

    def clean_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
            Renommer les noms de colonnes du fichier Parquet avec ceux de PostgreSQL pour eviter les confusions de nom de table
        """
        rename_map = {
            "VendorID": "vendor_id",
            "tpep_pickup_datetime": "tpep_pickup_datetime",
            "tpep_dropoff_datetime": "tpep_dropoff_datetime",
            "passenger_count": "passenger_count",
            "trip_distance": "trip_distance",
            "RatecodeID": "ratecode_id",
            "store_and_fwd_flag": "store_and_fwd_flag",
            "PULocationID": "pu_location_id",
            "DOLocationID": "do_location_id",
            "payment_type": "payment_type",
            "fare_amount": "fare_amount",
            "extra": "extra",
            "mta_tax": "mta_tax",
            "tip_amount": "tip_amount",
            "tolls_amount": "tolls_amount",
            "improvement_surcharge": "improvement_surcharge",
            "total_amount": "total_amount",
            "congestion_surcharge": "congestion_surcharge",
            "airport_fee": "airport_fee"
        }

        df = df.rename(columns=rename_map)
        df.columns = [col.lower() for col in df.columns]
        return df

    def import_parquet_files(self, parquet_folder="data/raw"):
        """
            Importe tous les fichiers Parquet
        """
        parquet_files = glob.glob(os.path.join(parquet_folder, "*.parquet"))

        if not parquet_files:
            print("Aucun fichier .parquet trouvé dans le dossier.")
            return

        for file_path in parquet_files:
            file_name = os.path.basename(file_path)
            print(f"📦 Import du fichier : {file_name}")

            # Charger les données
            df = pd.read_parquet(file_path)
            df = self.clean_columns(df)

            # Garder seulement les colonnes valides pour la table
            valid_columns = [
                "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                "passenger_count", "trip_distance", "ratecode_id", "store_and_fwd_flag",
                "pu_location_id", "do_location_id", "payment_type", "fare_amount", "extra",
                "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
                "total_amount", "congestion_surcharge", "airport_fee"
            ]
            df = df[[col for col in df.columns if col in valid_columns]]

            # Import des deux tables vers PostgreSQL
            df.to_sql("yellow_taxi_trips", self.engine, if_exists="append", index=False)

            import_log = pd.DataFrame({
                "file_name": [file_name],
                "import_date": [datetime.now()],
                "rows_imported": [len(df)]
            })
            import_log.to_sql("import_log", self.engine, if_exists="append", index=False)

            print(f" {file_name} importé ({len(df)} lignes).")

        print(" Tous les fichiers ont été importés avec succès !")


if __name__ == "__main__":
    importer = PostgresImporter()
    importer.create_tables()
    importer.import_parquet_files()
