import duckdb 
from pathlib import Path
from datetime import datetime

class DuckDBImport :
    def __init__(self, db_path : str):
        """
        declaration des variables avec la methodes self
        """
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._initialize_database()


    def _initialize_database(self):
        """
        Crée les tables si elles n'existent pas
        """
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
            VendorID BIGINT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            RatecodeID DOUBLE,
            store_and_fwd_flag VARCHAR,
            PULocationID BIGINT,
            DOLocationID BIGINT,
            payment_type BIGINT,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            congestion_surcharge DOUBLE,
            Airport_fee DOUBLE, 
        );
        """)
#         self.conn.execute(f"""
#     INSERT INTO yellow_taxi_trips
#     SELECT
#         VendorID,
#         tpep_pickup_datetime,
#         tpep_dropoff_datetime,
#         passenger_count,
#         trip_distance,
#         RatecodeID,
#         store_and_fwd_flag,
#         PULocationID,
#         DOLocationID,
#         payment_type,
#         fare_amount,
#         extra,
#         mta_tax,
#         tip_amount,
#         tolls_amount,
#         improvement_surcharge,
#         total_amount,
#         congestion_surcharge,
#         Airport_fee
#     FROM read_parquet('{file_path}')
# """)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS import_log (
            file_name VARCHAR PRIMARY KEY,
            import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            rows_imported BIGINT
        );
        """)


    def is_file_imported(self, filename: str) -> bool:
        """
        Vérifie si le fichier a déjà été importé
        """
        result = self.conn.execute(
            "SELECT COUNT(*) FROM import_log WHERE file_name = ?", (filename,)
        ).fetchone()[0]
        return result > 0
    

    def import_parquet(self, file_path: Path) -> bool:
        """
        Importe un fichier Parquet dans DuckDB
        """
        filename = file_path.name
        if self.is_file_imported(filename):
            print(f"[INFO] Fichier déjà importé : {filename}")
            return True

        try:
            print(f"[INFO] Import de {filename} ...")
            rows_before = self.conn.execute("SELECT COUNT(*) FROM yellow_taxi_trips").fetchone()[0]

            self.conn.execute(f"""
                INSERT INTO yellow_taxi_trips
                SELECT
                    VendorID,
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    RatecodeID,
                    store_and_fwd_flag,
                    PULocationID,
                    DOLocationID,
                    payment_type,
                    fare_amount,
                    extra,
                    mta_tax,
                    tip_amount,
                    tolls_amount,
                    improvement_surcharge,
                    total_amount,
                    congestion_surcharge,
                    Airport_fee
                FROM read_parquet('{file_path}')

            """)

            rows_after = self.conn.execute("SELECT COUNT(*) FROM yellow_taxi_trips").fetchone()[0]
            rows_imported = rows_after - rows_before

            self.conn.execute(
                "INSERT INTO import_log (file_name, rows_imported) VALUES (?, ?)",
                (filename, rows_imported)
            )

            print(f"[SUCCESS] {filename} importé ({rows_imported} lignes).")
            return True
        except Exception as e:
            print(f"[ERROR] Erreur lors de l'import de {filename} : {e}")
            return False
        

    def import_all_parquet_files(self, data_dir: Path) -> int:
        """
        Importe tous les fichiers Parquet du répertoire
        """
        parquet_files = sorted(data_dir.glob("*.parquet"))
        count = 0
        for file_path in parquet_files:
            if self.import_parquet(file_path):
                count += 1
        print(f"\n[SUMMARY] Total fichiers importés : {count}")
        return count
    

    def get_statistics(self):
        """
        Affiche des statistiques sur les trajets importés
        """
        total_trips = self.conn.execute("SELECT COUNT(*) FROM yellow_taxi_trips").fetchone()[0]
        total_files = self.conn.execute("SELECT COUNT(*) FROM import_log").fetchone()[0]
        min_date = self.conn.execute("SELECT MIN(tpep_pickup_datetime) FROM yellow_taxi_trips").fetchone()[0]
        max_date = self.conn.execute("SELECT MAX(tpep_dropoff_datetime) FROM yellow_taxi_trips").fetchone()[0]
        db_size = Path(self.db_path).stat().st_size / 1024 / 1024  

        print("\n[STATISTICS]")
        print(f"Nombre total de trajets : {total_trips}")
        print(f"Nombre de fichiers importés : {total_files}")
        print(f"Plage de dates : {min_date} → {max_date}")
        print(f"Taille de la base : {db_size:.2f} Mo")


    def close(self):
        self.conn.close()


if __name__ == "__main__":
    db_file = Path(__file__).parent.parent / "data" / "duckdb" / "nyc_taxi_2025.duckdb"
    db_file.parent.mkdir(parents=True, exist_ok=True)

    data_dir = Path(__file__).parent.parent / "data" / "raw"
    importer = DuckDBImport(str(db_file))
    importer.import_all_parquet_files(data_dir)
    importer.get_statistics()
    importer.close()