"""
🚀 pipeline_runner.py — Orquestador del pipeline ETL

Este script ejecuta el flujo completo:
1. Lee el archivo CSV por chunks.
2. Transforma cada chunk.
3. Carga los datos transformados en PostgreSQL.
4. Valida si la tabla contiene registros tras la carga.
"""

from etl.extract_csv import read_csv_chunks
from etl.transform_csv import transform_chunk
from etl.load_to_db import load_to_postgres
from etl.logger import get_logger
from etl.db import validate_table_not_empty

logger = get_logger(__name__)

def run_pipeline(filepath, table_name):
    """
    Ejecuta el pipeline completo: extracción → transformación → carga → validación.

    Parámetros:
    - filepath: Ruta del archivo CSV
    - table_name: Nombre de la tabla destino en PostgreSQL
    """
    try:
        for chunk in read_csv_chunks(filepath):
            df_clean = transform_chunk(chunk)
            load_to_postgres(df_clean, table_name)

        logger.info("✅ Pipeline finalizado correctamente.")

        # Validación post-carga
        if validate_table_not_empty(table_name):
            logger.info(f"📊 La tabla '{table_name}' fue cargada exitosamente.")
        else:
            logger.warning(f"⚠️ La tabla '{table_name}' está vacía tras la carga.")

    except Exception as e:
        logger.error(f"❌ Error en el pipeline: {str(e)}")
