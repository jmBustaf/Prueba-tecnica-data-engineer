"""
etl_star_model.py — Carga incremental al modelo en estrella en PostgreSQL
Incluye lógica para:
- Leer un CSV en chunks.
- Insertar datos únicos en dimensiones: usuarios, productos, fechas.
- Cargar hechos en la tabla fact_transactions.
- Evitar duplicados mediante ON CONFLICT DO NOTHING.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from etl.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

CHUNK_SIZE = 10000
CSV_PATH = "data/sample_transactions.csv"

def get_engine():
    """
    Crea y retorna un engine de SQLAlchemy para conexión a PostgreSQL
    usando variables de entorno desde .env.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

def insert_if_not_exists(df, table_name, engine, unique_cols):
    """
    Inserta registros en una tabla evitando duplicados según columnas únicas.

    Parámetros:
    - df: DataFrame con los datos a insertar.
    - table_name: Nombre de la tabla destino.
    - engine: Conexión SQLAlchemy.
    - unique_cols: Lista de columnas que definen unicidad.
    """
    with engine.begin() as conn:
        for _, row in df.iterrows():
            placeholders = ', '.join([f":{col}" for col in df.columns])
            conflict_cols = ', '.join(unique_cols)
            insert_stmt = text(f"""
                INSERT INTO {table_name} ({', '.join(df.columns)})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_cols}) DO NOTHING
            """)
            conn.execute(insert_stmt, row.to_dict())

def main():
    """
    Ejecuta la carga por chunks desde un archivo CSV hacia el modelo en estrella.
    Inserta en las dimensiones dim_users, dim_products, dim_date y luego en fact_transactions.
    """
    logger.info("🚀 Iniciando carga al modelo estrella...")

    engine = get_engine()
    chunk_iter = pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE, parse_dates=["timestamp"])

    for i, chunk in enumerate(chunk_iter):
        logger.info(f"📦 Procesando chunk {i+1}")

        # dim_users
        dim_users = chunk[["user_id", "user_name", "user_email", "user_signup"]].drop_duplicates()
        dim_users.columns = ["user_id", "name", "email", "signup_date"]
        insert_if_not_exists(dim_users, "dim_users", engine, ["user_id"])

        # dim_products
        dim_products = chunk[["product_id", "product_name", "product_category"]].drop_duplicates()
        dim_products.columns = ["product_id", "product_name", "category"]
        insert_if_not_exists(dim_products, "dim_products", engine, ["product_id"])

        # dim_date
        dim_date = pd.DataFrame()
        dim_date["full_date"] = chunk["timestamp"].dt.date
        dim_date["day"] = chunk["timestamp"].dt.day
        dim_date["month"] = chunk["timestamp"].dt.month
        dim_date["year"] = chunk["timestamp"].dt.year
        dim_date["weekday"] = chunk["timestamp"].dt.day_name()
        dim_date = dim_date.drop_duplicates()
        insert_if_not_exists(dim_date, "dim_date", engine, ["full_date"])

        # Obtener date_id para mapear fact
        with engine.connect() as conn:
            dim_date_ids = pd.read_sql("SELECT date_id, full_date FROM dim_date", conn)

        chunk = chunk.merge(dim_date_ids, left_on=chunk["timestamp"].dt.date, right_on="full_date", how="left")

        fact = chunk[[
            "transaction_id",
            "user_id",
            "product_id",
            "date_id",
            "amount",
            "status",
            "timestamp"
        ]]
        insert_if_not_exists(fact, "fact_transactions", engine, ["transaction_id"])

    logger.info("✅ Carga al modelo estrella finalizada exitosamente.")

if __name__ == "__main__":
    main()
