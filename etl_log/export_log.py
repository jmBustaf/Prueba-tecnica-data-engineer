"""
export_log.py — Módulo de exportación de métricas a formato Parquet
Incluye lógica para:
- Exportar un DataFrame procesado a un archivo Parquet.
- Crear las carpetas necesarias si no existen.
- Validar que el DataFrame no esté vacío antes de exportar.
"""

import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def export_to_parquet(df: pd.DataFrame, output_path: str) -> None:
    """
    Exporta un DataFrame a Parquet con compresión Snappy.

    - Valida si el DataFrame está vacío y evita exportar en ese caso.
    - Crea la estructura de carpetas necesaria si no existe.
    - Lanza excepciones en caso de error durante la escritura.

    Parámetros:
    - df: DataFrame a exportar.
    - output_path: Ruta de salida del archivo .parquet.
    """
    if df.empty:
        logger.warning("DataFrame vacío. No se exportará ningún archivo.")
        return

    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_parquet(output_path, index=False, compression="snappy")
        logger.info(f"Archivo exportado exitosamente a: {output_path}")

    except Exception as e:
        logger.error(f"Error exportando a Parquet: {str(e)}")
        raise
