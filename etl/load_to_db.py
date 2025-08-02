"""
 load_to_db.py — Módulo para cargar datos a PostgreSQL
Define función que inserta un DataFrame en una tabla usando SQLAlchemy.
"""

from etl.db import get_engine
from etl.logger import get_logger

logger = get_logger(__name__)

def load_to_postgres(df, table_name):
    """
    Inserta un DataFrame en la tabla especificada de PostgreSQL.

    Parámetros:
    - df: pandas.DataFrame con los datos limpios
    - table_name: nombre de la tabla destino

    Usa `if_exists="append"` para agregar los datos sin truncar.
    """
    engine = get_engine()
    try:
        df.to_sql(table_name, con=engine, if_exists="append", index=False, method="multi")
        logger.info(f"Cargado chunk en tabla {table_name} ({len(df)} filas)")
    except Exception as e:
        logger.error(f"Error cargando a DB: {str(e)}")
        raise
