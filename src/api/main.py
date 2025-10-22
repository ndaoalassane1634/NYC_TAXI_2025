# src/main.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import OperationalError
from .database import engine, Base, get_db
from .routes import router

# ---------------- Créer les tables ---------------- #
try:
    Base.metadata.create_all(bind=engine)
except OperationalError as e:
    print("Erreur lors de la création des tables :", e)

# ---------------- Initialiser FastAPI ---------------- #
app = FastAPI(
    title="NYC Taxi Data Pipeline API",
    description="API pour gérer les trajets de taxis jaunes de NYC, exécuter des pipelines d'import et fournir des statistiques.",
    version="1.0.0"
)

# ---------------- Configurer CORS ---------------- #
origins = [
    "*",  # Autoriser toutes les origines pour dev; restreindre en prod
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- Inclure le router ---------------- #
app.include_router(router, prefix="/api/v1")


# ---------------- Routes racine et health ---------------- #
@app.get("/", tags=["Root"])
def root():
    return {"message": "Bienvenue sur NYC Taxi Data Pipeline API!"}


@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok"}
