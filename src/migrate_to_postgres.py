# src/migrate_duckdb_to_postgres.py

import duckdb
import psycopg2
import os
import pandas as pd

# --- Connexion à DuckDB ---
duckdb_path = 'data/taxi.duckdb'
duck_conn = duckdb.connect(database=duckdb_path, read_only=True)

# --- Connexion à PostgreSQL ---
pg_conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    database=os.getenv("POSTGRES_DB", "taxi_db"),
    user=os.getenv("POSTGRES_USER", "taxi_user"),
    password=os.getenv("POSTGRES_PASSWORD", "taxi-2025")
)
pg_cur = pg_conn.cursor()

# --- Étape 1 : lister les tables DuckDB ---
tables = [row[0] for row in duck_conn.execute("SHOW TABLES").fetchall()]
print(f"Tables détectées dans DuckDB : {tables}")


# --- Étape 2 : migrer chaque table ---
for table in tables:
    print(f"Migration de la table : {table}")

    # Récupérer les colonnes et types
    schema = duck_conn.execute(f"DESCRIBE {table}").fetchdf()
    columns = schema['column_name'].tolist()
    types = schema['column_type'].tolist()

    # Conversion des types DuckDB → PostgreSQL
    type_mapping = {
        'BIGINT': 'BIGINT',
        'DOUBLE': 'DOUBLE PRECISION',
        'INTEGER': 'INT',
        'VARCHAR': 'TEXT',
        'TIMESTAMP': 'TIMESTAMP',
        'BOOLEAN': 'BOOLEAN'
    }
    pg_columns = ", ".join(f'"{col}" {type_mapping.get(dtype, "TEXT")}' for col, dtype in zip(columns, types))

    # Création de la table dans PostgreSQL
    print(f"  Création de la table {table} si elle n'existe pas...")
    if table == 'yellow_taxi_trips':
        pg_cur.execute(f"CREATE TABLE IF NOT EXISTS {table} (id SERIAL PRIMARY KEY, {pg_columns});")
    else:
        pg_cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({pg_columns});")
    pg_conn.commit()

    # Vider la table avant insertion pour éviter les doublons
    print(f"  Vidage de la table {table}...")
    pg_cur.execute(f"TRUNCATE TABLE {table};")
    pg_conn.commit()

    # Récupération et insertion des données par lots
    print(f"  Insertion des données dans {table}...")
    duck_cursor = duck_conn.cursor()
    duck_cursor.execute(f"SELECT * FROM {table} LIMIT 100000")
    
    total_rows_inserted = 0
    chunk_size = 10000  # Vous pouvez ajuster cette taille

    while True:
        chunk = duck_cursor.fetchmany(chunk_size)
        if not chunk:
            break
        
        # Insertion du lot dans PostgreSQL
        column_names = ', '.join(f'"{c}"' for c in columns)
        placeholders = ','.join(['%s'] * len(columns))
        query = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
        
        pg_cur.executemany(query, chunk)
        pg_conn.commit()
        
        total_rows_inserted += len(chunk)
        print(f"    {total_rows_inserted} lignes insérées...")

    if total_rows_inserted == 0:
        print(f"  Table {table} vide, rien à insérer.")
    else:
        print(f"  Migration de la table {table} terminée. Total de {total_rows_inserted} lignes insérées.")

# --- Finalisation ---
pg_cur.close()
pg_conn.close()
duck_conn.close()

print("Migration complète terminée avec succès !")

