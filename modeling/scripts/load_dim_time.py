"""
load_dim_time.py — Módulo para poblar la tabla de dimensión de fechas (dim_date).
Incluye lógica para:
- Leer timestamps desde un archivo CSV de transacciones.
- Generar una tabla de fechas únicas con día, mes, año y nombre del día.
- Insertar los registros únicos en la tabla dim_date de PostgreSQL.
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

def load_dim_time(csv_path: str):
    """
    Carga fechas únicas desde un CSV con timestamp a la tabla dim_date.

    Pasos:
    - Lee la columna 'ts' desde el archivo CSV.
    - Extrae componentes de fecha: día, mes, año, nombre del día.
    - Elimina duplicados y genera un DataFrame limpio.
    - Inserta los datos en la tabla dim_date de PostgreSQL.

    Parámetros:
    - csv_path: Ruta al archivo CSV de origen que contiene la columna 'ts'.
    """
    print("📅 Procesando fechas únicas...")
    df = pd.read_csv(csv_path, usecols=["ts"])
    df['full_date'] = pd.to_datetime(df['ts']).dt.date
    df = df.drop_duplicates(subset=['full_date']).copy()
    df['day'] = pd.to_datetime(df['full_date']).dt.day
    df['month'] = pd.to_datetime(df['full_date']).dt.month
    df['year'] = pd.to_datetime(df['full_date']).dt.year
    df['weekday'] = pd.to_datetime(df['full_date']).dt.day_name()

    df_final = df[['full_date', 'day', 'month', 'year', 'weekday']]
    df_final.to_sql("dim_date", con=get_engine(), if_exists="append", index=False)
    print("✅ Fechas cargadas en dim_date.")
