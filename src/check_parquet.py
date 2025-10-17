import pandas as pd

file_path = "data/raw/yellow_tripdata_2025-01.parquet"

# Charger uniquement les colonnes (pas tout le fichier)
df = pd.read_parquet(file_path, engine="pyarrow")

# Afficher les colonnes et leur nombre
print("Nombre de colonnes :", len(df.columns))
print("Colonnes :", list(df.columns))
