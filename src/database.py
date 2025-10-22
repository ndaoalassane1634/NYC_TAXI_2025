import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "nyc_taxi")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(DATABASE_URL)

# Création d’une session locale
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base à partir de laquelle on définit les modèles
Base = declarative_base()


def get_db():
    """
     Fonction pour obtenir une session DB (utile pour FastAPI)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """
    Fonction pour initialiser la base de données
    Crée les tables à partir des modèles définis avec SQLAlchemy (si elles n’existent pas déjà).
    """
    from src import models  # si tu crées des modèles plus tard
    Base.metadata.create_all(bind=engine)
