"""
db.py — Módulo de conexión a base de datos
Responsable de crear una conexión a PostgreSQL usando SQLAlchemy
y variables de entorno cargadas desde un archivo `.env`.

Se espera que el archivo .env contenga:
- DB_USER
- DB_PASSWORD
- DB_HOST
- DB_PORT
- DB_NAME
"""

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from etl.logger import get_logger

load_dotenv()  # Carga las variables de entorno desde .env

logger = get_logger(__name__)

def get_engine():
    """
    Retorna un SQLAlchemy Engine conectado a la base de datos definida en las variables de entorno.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")

    connection_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(connection_url)


def validate_table_not_empty(table_name: str) -> bool:
    """
    Verifica si una tabla en la base de datos contiene al menos un registro.

    Parámetros:
    - table_name: nombre de la tabla a validar

    Retorna:
    - True si la tabla tiene datos
    - False si está vacía o hubo error de conexión
    """
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
            logger.info(f"✔️ La tabla {table_name} tiene {count} registros.")
            return count > 0
    except Exception as e:
        logger.error(f"❌ Error al validar si la tabla {table_name} está vacía: {str(e)}")
        return False