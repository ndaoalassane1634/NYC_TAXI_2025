import os
import requests
from datetime import datetime
from tqdm import tqdm  # ✅ Barre de progression

# Dossier local pour les fichiers téléchargés
DATA_DIR = "data/raw"
os.makedirs(DATA_DIR, exist_ok=True)

# Base URL de téléchargement
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


# Fonction pour télécharger un seul mois
def download_month(year: int, month: int) -> str | None:
    """Télécharge un fichier parquet pour une année et un mois donnés."""
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    file_url = BASE_URL + file_name
    file_path = os.path.join(DATA_DIR, file_name)

    # Si le fichier existe déjà localement
    if os.path.exists(file_path):
        print(f"{file_name} déjà téléchargé.")
        return None

    # Vérifie si le fichier existe sur le serveur
    head = requests.head(file_url)
    if head.status_code == 200:
        print(f" Téléchargement de {file_name} en cours...")

        # Téléchargement avec barre de progression
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("Content-Length", 0))
            chunk_size = 8192

            with open(file_path, "wb") as f, tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc=file_name,
                ascii=True,
            ) as progress_bar:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        progress_bar.update(len(chunk))

        print(f"{file_name} téléchargé dans {DATA_DIR}/")
        return file_path

    elif head.status_code == 404:
        print(f"{file_name} non encore disponible.")
        return None
    else:
        print(f"Erreur {head.status_code} pour {file_name}.")
        return None


# Fonction pour télécharger tous les fichiers disponibles jusqu'au mois actuel
def download_all_available(year: int = 2025) -> list:
    """Télécharge tous les fichiers disponibles jusqu'au mois actuel."""
    now = datetime.now()
    current_year = now.year
    current_month = now.month

    downloaded_files = []

    if year == current_year:
        for month in range(1, current_month + 1):
            file_path = download_month(year, month)
            if file_path:
                downloaded_files.append(file_path)
    else:
        print(f"Année {year} différente de l'année actuelle ({current_year}).")

    # Résumé final
    print("\nRésumé du téléchargement :")
    for f in downloaded_files:
        print(f"{os.path.basename(f)}")
    print(f"\nTotal fichiers téléchargés : {len(downloaded_files)}")

    return downloaded_files


# Exécution directe du script
if __name__ == "__main__":
    download_all_available(2025)
