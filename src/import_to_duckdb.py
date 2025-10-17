# src/import_to_duckdb.py

from pathlib import Path
import duckdb
from tqdm import tqdm

class DuckDBImporter:


    def __init__(self, db_path):
        """Initialise la connexion à DuckDB et crée les tables si nécessaire."""
        from pathlib import Path
        import duckdb

        self.con = duckdb.connect(database=str(db_path), read_only=False)
        self._initialize_database()

    def _initialize_database(self):
        """Crée les tables si elles n'existent pas déjà."""
        # Table principale avec schéma imposé
        self.con.execute("""
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
                cbd_congestion_fee DOUBLE
            )
        """)
        # Table de log
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS import_log (
                file_name VARCHAR PRIMARY KEY,
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rows_imported BIGINT
            )
        """)


    def is_file_imported(self, file_path: Path) -> bool:
        """Vérifie si un fichier a déjà été importé."""
        result = self.con.execute("""
            SELECT COUNT(*) FROM import_log WHERE file_name = ?
        """, (str(file_path),)).fetchone()
        return result[0] > 0

    def import_parquet(self, file_path: Path) -> bool:
        file_path = Path(file_path)
        if self.is_file_imported(file_path):
            print(f" {file_path.name} déjà importé.")
            return True

        try:
            # Nombre de lignes avant import
            before_rows = self.con.execute("SELECT COUNT(*) FROM yellow_taxi_trips").fetchone()[0]

            # Import du fichier Parquet
            self.con.execute(f"""
                INSERT INTO yellow_taxi_trips
                SELECT * FROM read_parquet('{file_path}')
            """)

            # Nombre de lignes après import
            after_rows = self.con.execute("SELECT COUNT(*) FROM yellow_taxi_trips").fetchone()[0]
            rows_added = after_rows - before_rows

            # Ajout dans la table de log
            self.con.execute("""
                INSERT INTO import_log (file_name, rows_imported)
                VALUES (?, ?)
            """, (str(file_path), rows_added))

            print(f" {file_path.name} importé avec succès ({rows_added} lignes ajoutées).")
            return True

        except Exception as e:
            print(f" Erreur lors de l'import de {file_path.name}: {e}")
            return False


    def import_all_parquet_files(self, data_dir: Path) -> int:
        """Importe tous les fichiers Parquet du répertoire et retourne le nombre de fichiers importés."""
        data_dir = Path(data_dir)
        parquet_files = sorted(data_dir.glob("*.parquet"))
        imported_count = 0

        for file_path in tqdm(parquet_files, desc="Import des fichiers Parquet", unit="file", ascii=True):
            if self.import_parquet(file_path):
                imported_count += 1

        print(f"\n Total fichiers importés : {imported_count}")
        return imported_count

    def get_statistics(self):
        """Affiche le nombre de trajets, fichiers importés, plage de dates, taille de la base de données."""
        total_trips = self.con.execute("SELECT COUNT(*) FROM yellow_taxi_trips").fetchone()[0]
        total_files = self.con.execute("SELECT COUNT(DISTINCT file_name) FROM import_log").fetchone()[0]
        date_range = self.con.execute("""
            SELECT MIN(tpep_pickup_datetime), MAX(tpep_dropoff_datetime)
            FROM yellow_taxi_trips
        """).fetchone()
        db_size = self.con.execute("PRAGMA database_size").fetchone()[0]

        print(f"Total de trajets : {total_trips}")
        print(f"Total de fichiers importés : {total_files}")
        print(f"Plage de dates : {date_range[0]} à {date_range[1]}")
        print(f"Taille de la base de données : {db_size} bytes")

    def close(self):
        """Ferme la connexion à la base de données."""
        self.con.close()


if __name__ == "__main__":
    from pathlib import Path

    # Chemin vers la base DuckDB
    db_path = Path("data/taxi.duckdb")
    # Chemin vers les fichiers Parquet
    data_dir = Path("data/raw")

    # Crée l'importer
    db = DuckDBImporter(db_path=db_path)

    # Import de tous les fichiers
    db.import_all_parquet_files(data_dir=data_dir)

    # Affiche les stats
    db.get_statistics()

    # Ferme la connexion
    db.close()
