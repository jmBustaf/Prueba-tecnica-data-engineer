"""
 extract_csv.py — Módulo de extracción de datos desde archivo CSV
Incluye lógica para:
- Esperar a que el archivo esté disponible con un tamaño mínimo.
- Leer el archivo por chunks (ideal para archivos grandes).
"""

import time
import os
import pandas as pd
from etl.logger import get_logger

logger = get_logger(__name__)

def wait_for_file(filepath: str, min_size_kb=10, timeout=30):
    """
    Espera hasta que el archivo exista y tenga un tamaño mínimo.
    Lanza TimeoutError si no está disponible después del tiempo dado.
    """
    logger.info(f"Esperando archivo: {filepath}")
    start_time = time.time()

    while not os.path.exists(filepath) or os.path.getsize(filepath) < min_size_kb * 1024:
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Archivo {filepath} no disponible tras {timeout} segundos")
        time.sleep(2)
    
    logger.info(f"Archivo disponible: {filepath}")

def read_csv_chunks(filepath, chunksize=100000):
    """
    Generador que retorna chunks del archivo CSV usando pandas.read_csv.

    Parámetros:
    - filepath: Ruta del archivo CSV
    - chunksize: Cantidad de filas por chunk

    Yields:
    - DataFrame parcial con cada chunk
    """
    wait_for_file(filepath)
    try:
        for chunk in pd.read_csv(filepath, chunksize=chunksize):
            yield chunk
    except Exception as e:
        logger.error(f"Error leyendo CSV: {str(e)}")
        raise
