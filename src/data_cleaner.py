#on crée classe datacleaner qu'on initialise et connecte à mongo

from pymongo import MongoClient
from pymongo.errors import PyMongoError
import logging, os
from typing import Optional
from pymongo import ASCENDING
import pandas as pd
logging.basicConfig(level=logging.INFO)
from sqlalchemy import create_engine

def is_running_in_docker() -> bool:
    return os.getenv("RUNNING_IN_DOCKER", "false").lower() == "true"

def connect_postgresql():

    from sqlalchemy import text
        # Lire les variables d'environnement
    USER=os.getenv("POSTGRES_USER", "taxi_user")
    PASSWORD=os.getenv("POSTGRES_PASSWORD", "taxi_2025")
    HOST=os.getenv("POSTGRES_HOST", "localhost")
    if is_running_in_docker():
        HOST = "postgres"
    PORT=os.getenv("POSTGRES_PORT", "5432")
    DATABASE=os.getenv("POSTGRES_DB", "taxi_db")

    url = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    engine = create_engine(url)

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logging.info("Connected to PostgreSQL, test query result: %s", result.scalar())
        return engine
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        return None


def connect_mongo():
    USER = os.getenv("MONGO_USER", "admin")
    PASSWORD = os.getenv("MONGO_PASSWORD", "admin123")
    HOST = os.getenv("MONGO_HOST", "mongodb")
    PORT = os.getenv("MONGO_PORT", "27017")

    if not is_running_in_docker():
        HOST = "localhost"

    try:
        mongo_client = MongoClient("mongodb://%s:%s@%s:%s/" % (USER, PASSWORD, HOST, PORT))
        mongo_client.admin.command('ping')
        logging.info(f"Tentative de connexion à MongoDB sur {HOST}:{PORT}")
        return mongo_client
    except PyMongoError as e:
        logging.error(f"Error connecting to MongoDB: {e}")
        return None
    
def create_db_mongo(mongo_client: MongoClient, db_name: str):
    try:
        db = mongo_client[db_name]
        logging.info(f"Database '{db_name}' created or accessed successfully")
        return db
    except PyMongoError as e:
        logging.error(f"Error creating/accessing database '{db_name}': {e}")
        return None

def clean_data(data):
    data = data.dropna()  
    return data

if __name__ == "__main__":
    # Test PostgreSQL
    engine = connect_postgresql()

    # Test MongoDB
    mongo_client = connect_mongo()
    db = create_db_mongo(mongo_client, "nyc_taxi")
    print("Collections existantes :", db.list_collection_names())

    # Test nettoyage
    df = pd.DataFrame({"col1": [1, None], "col2": ["a", "b"]})
    df_cleaned = clean_data(df)
    print(df_cleaned)
