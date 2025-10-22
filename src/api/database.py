"""
Fichier servant de pont entre PostgreSQL et FastAPI :
1️⃣ Lit les variables d'environnement pour la connexion à PostgreSQL
2️⃣ Crée un moteur SQLAlchemy (create_engine)
3️⃣ Fournit une session get_db() pour interagir avec la base
4️⃣ Initialise les tables si elles n'existent pas déjà
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from pathlib import Path

# --- Charger les variables d'environnement depuis le fichier .env ---
env_path = Path(__file__).resolve().parent.parent.parent / '.env'
if env_path.exists():
    print(f"--- Fichier .env trouvé à {env_path} ---")
    load_dotenv(dotenv_path=env_path)
else:
    print("--- Fichier .env non trouvé, utilisation des valeurs par défaut ---")

# --- Récupérer les variables d'environnement ---
POSTGRES_USER = os.getenv("POSTGRES_USER", "taxi_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "taxi_2025")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "taxi_db")

# --- DEBUG: Afficher les variables utilisées ---
print("--- VALEURS DE CONNEXION UTILISÉES ---")
print(f"USER: {POSTGRES_USER}")
print(f"PASSWORD: {'*' * len(POSTGRES_PASSWORD) if POSTGRES_PASSWORD else 'None'}")
print(f"HOST: {POSTGRES_HOST}")
print(f"DB: {POSTGRES_DB}")
print("------------------------------------")

# --- Construire l'URL de connexion ---
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}"

# --- Créer le moteur SQLAlchemy : objet qui sait parler à postgresql, c'est le pont entre python et la base 
engine = create_engine(DATABASE_URL)

# --- Créer une session locale pour faire des requêtes ---
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- Base déclarative pour les modèles, la base sert comme classe parente et c'est elle qui va créer les tables ---
Base = declarative_base()

# --- Dépendance pour obtenir une session de base de données: la session est ouverte au début de la requête et fermée à la fin ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Fonction pour initialiser les tables ---
def init_db():
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from api.models import YellowTaxiTrip, ImportLog
    
    Base.metadata.create_all(bind=engine)
    print("Schéma initialisé avec succès.")

if __name__ == "__main__":
    print("Ce script est exécuté directement. Lancement de l'initialisation de la base de données.")
    init_db()
