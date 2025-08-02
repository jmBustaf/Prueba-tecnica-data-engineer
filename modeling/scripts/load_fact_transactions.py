"""
load_fact_transactions.py — Módulo para poblar la tabla de hechos fact_transactions.
Incluye lógica para:
- Leer datos de transacciones desde un archivo CSV.
- Mapear fechas a su ID correspondiente desde la dimensión dim_date.
- Insertar registros procesados en la tabla fact_transactions de PostgreSQL.
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

def get_date_id_map(engine):
    """
    Consulta la tabla dim_date y construye un diccionario para mapear fechas (YYYY-MM-DD)
    al campo date_id correspondiente.

    Parámetros:
    - engine: Conexión activa de SQLAlchemy.

    Retorna:
    - Un diccionario con clave = fecha (str), valor = date_id (int).
    """
    query = "SELECT date_id, full_date FROM dim_date"
    df = pd.read_sql(query, con=engine)
    return {str(row['full_date']): row['date_id'] for _, row in df.iterrows()}

def load_fact_transactions(csv_path: str):
    """
    Carga transacciones desde un archivo CSV y las inserta en fact_transactions.

    Pasos:
    - Lee las columnas relevantes desde el CSV (incluye ts para mapear fecha).
    - Convierte la fecha 'ts' a 'full_date' y la asocia con un 'date_id'.
    - Renombra 'order_id' a 'transaction_id' para cumplir con la tabla destino.
    - Inserta las columnas seleccionadas en la tabla fact_transactions.

    Parámetros:
    - csv_path: Ruta del archivo CSV de transacciones.
    """
    print("💾 Cargando transacciones...")
    engine = get_engine()
    df = pd.read_csv(csv_path)

    # Mapear fecha
    df['full_date'] = pd.to_datetime(df['ts']).dt.date.astype(str)
    date_id_map = get_date_id_map(engine)
    df['date_id'] = df['full_date'].map(date_id_map)

    # Renombrar columnas
    df.rename(columns={ 'order_id': 'transaction_id' }, inplace=True)

    # Selección final
    df_final = df[['transaction_id', 'user_id', 'date_id', 'amount', 'status', 'ts']]
    df_final.to_sql("fact_transactions", con=engine, if_exists="append", index=False)
    print("✅ Transacciones cargadas en fact_transactions.")
