import requests
from pathlib import Path
from datetime import datetime

class NYCTaxiDataDownloader : 
    def __init__(self):
        """
        Fonction d'initialiation des variables de téléchargement
        """
        # self.BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
        self.BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        self.YEAR = 2025
        self.DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        pass


    def get_file_path (self, month : int) -> Path :
        """ 
        Fonxtion qui permet de récuperer le chemin complet du fichier pour une mois donnée
        """
        month_str = f"{month:02d}"
        filename = f"yellow_tripdata_{self.YEAR}-{month_str}.parquet"
        # return self.get_file_path(month).exists()
        return self.DATA_DIR / filename
    

    def file_exists(self, month: int) -> bool:
        """
        Vérifie si le fichier existe déjà
        """
        return self.get_file_path(month).exists()

    # def download_month(self, month: int) -> bool:
    #     """
    #     Télécharge le fichier du mois si nécessaire
    #     """
    #     file_path = self.get_file_path(month)
    #     if self.file_exists(month):
    #         print(f"[INFO] Fichier déjà présent : {file_path.name}")
    #         return True
        
    #     month_str = f"{month:02d}"
    #     url = f"{self.BASE_URL}yellow_tripdata_{self.YEAR}-{month_str}.parquet"

    #     print(f"[INFO] Téléchargement de {url} ...")
    #     try:
    #         response = requests.get(url, stream=True, timeout=30)
    #         response.raise_for_status()
    #         with open(file_path, "wb") as f:
    #             for chunk in response.iter_content(chunk_size=8192):
    #                 f.write(chunk)
    #         print(f"[SUCCESS] Fichier téléchargé : {file_path.name}")
    #         return True
    #     except requests.exceptions.RequestException as e:
    #         print(f"[ERROR] Impossible de télécharger {url}: {e}")
    #         if file_path.exists():
    #             file_path.unlink()  
    #         return False


    def download_month(self, month: int) -> bool:
        file_path = self.get_file_path(month)
        if self.file_exists(month):
            print(f"[INFO] Fichier déjà présent : {file_path.name}")
            return True

        month_str = f"{month:02d}"
        url = f"{self.BASE_URL}yellow_tripdata_{self.YEAR}-{month_str}.parquet"

        print(f"[INFO] Téléchargement de {url} ...")
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"[SUCCESS] Fichier téléchargé : {file_path.name}")
            return True
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Impossible de télécharger {url}: {e}")
            if file_path.exists():
                file_path.unlink()  
            return False


    def download_all_available(self) -> list:
        """
        Télécharge tous les fichiers disponibles pour l'année 2025
        """
        downloaded_files = []
        current_month = datetime.now().month
        for month in range(1, current_month + 1):
            if self.download_month(month):
                downloaded_files.append(self.get_file_path(month))
        print(f"\n[SUMMARY] Total fichiers téléchargés : {len(downloaded_files)}")
        return downloaded_files    



if __name__ == "__main__":
    downloader = NYCTaxiDataDownloader()
    files = downloader.download_all_available()