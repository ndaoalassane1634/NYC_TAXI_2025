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

# --- Charger les variables d'environnement depuis le fichier .env ---
load_dotenv()

# --- Récupérer les variables d'environnement ---
POSTGRES_USER = os.getenv("POSTGRES_USER", "taxi_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "taxi-2025")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "taxi_db")

# --- Construire l'URL de connexion ---
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}"

# --- Créer le moteur SQLAlchemy ---
engine = create_engine(DATABASE_URL)

# --- Créer une session locale ---
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- Base déclarative pour les modèles ---
Base = declarative_base()

# --- Dépendance pour obtenir une session de base de données ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Fonction pour initialiser les tables ---
def init_db():
    import src.models  # Importer les modèles pour que SQLAlchemy les connaisse
    Base.metadata.create_all(bind=engine)
