"""
 transform_csv.py — Módulo de transformación de datos
Contiene la lógica para limpiar y normalizar los datos:
- Nombres de columnas en minúscula
- Conversión de fechas
"""

import pandas as pd
from etl.logger import get_logger

logger = get_logger(__name__)

def transform_chunk(df):
    """
    Aplica transformaciones básicas a un DataFrame:
    - Limpia nombres de columnas
    - Convierte `transaction_date` a datetime

    Devuelve:
    - DataFrame limpio
    """
    try:
        df.columns = [col.strip().lower() for col in df.columns]
        if "transaction_date" in df.columns:
            df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
        return df
    except Exception as e:
        logger.error(f"Error transformando chunk: {str(e)}")
        raise
