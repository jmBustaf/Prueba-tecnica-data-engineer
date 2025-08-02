"""
main_log_etl.py — Script principal del pipeline ETL para logs comprimidos
Incluye la orquestación completa del flujo:
- Extracción de registros desde archivo .gz en formato JSONL.
- Transformación con filtrado y agregación de métricas.
- Exportación del resultado a formato Parquet comprimido.
- Medición del rendimiento (tiempo y memoria).
"""

import os
from etl_log.extract_log import read_jsonl_gzip_stream
from etl_log.transform_log import transform_records
from etl_log.export_log import export_to_parquet
from etl_log.profiler import profile_function
import logging

# Configurar logging global del pipeline
logger = logging.getLogger("log_etl_pipeline")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def main():    
    """
    Función principal del pipeline ETL para logs.    
    """
    input_path = "data/sample.log.gz"
    output_path = "outputs/metrics.parquet"

    logger.info("Iniciando pipeline ETL para archivo de logs...")

    try:
        # 1. EXTRAER (stream JSONL desde gzip)
        records = list(read_jsonl_gzip_stream(input_path))  # Para permitir reuse

        # 2. TRANSFORMAR con profiling
        df_metrics, time_taken, mem_peak = profile_function(transform_records, records)

        # 3. EXPORTAR
        export_to_parquet(df_metrics, output_path)

        logger.info("Pipeline finalizado exitosamente.")

    except Exception as e:
        logger.error(f"Error en el pipeline: {str(e)}")

if __name__ == "__main__":
    main()
