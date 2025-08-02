"""
load_dim_users.py — Módulo para poblar la tabla de dimensión de usuarios (dim_users).
Incluye lógica para:
- Leer los IDs únicos de usuario desde un archivo CSV de transacciones.
- Insertar los registros únicos en la tabla dim_users de PostgreSQL.
"""

import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    """
    Crea y retorna un engine de SQLAlchemy para conexión a PostgreSQL
    utilizando variables de entorno definidas en el archivo .env.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

def load_dim_users(csv_path: str):
    """
    Carga IDs únicos de usuario desde un archivo CSV a la tabla dim_users.

    Pasos:
    - Lee la columna 'user_id' desde el archivo CSV.
    - Elimina duplicados y construye un DataFrame limpio.
    - Inserta los datos en la tabla dim_users de PostgreSQL.

    Parámetros:
    - csv_path: Ruta al archivo CSV de origen que contiene la columna 'user_id'.
    """
    print("📥 Cargando usuarios únicos...")
    df = pd.read_csv(csv_path, usecols=["user_id"])
    df_users = df["user_id"].drop_duplicates().to_frame()
    df_users.to_sql("dim_users", con=get_engine(), if_exists="append", index=False)
    print("✅ Usuarios cargados a dim_users.")

if __name__ == "__main__":
    load_dim_users("data/sample_transactions.csv")
