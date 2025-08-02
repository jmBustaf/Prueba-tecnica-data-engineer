"""
extract_log.py — Módulo de extracción de logs desde archivo comprimido .gz en formato JSONL
Incluye lógica para:
- Leer el archivo en streaming línea por línea.
- Validar y filtrar registros JSON válidos.
- Ignorar líneas mal formateadas y registrar advertencias.
"""

import gzip
import json
import logging
from typing import Generator, Dict, Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def read_jsonl_gzip_stream(filepath: str) -> Generator[Dict, None, None]:
    """
    Lee un archivo .gz en formato JSONL línea por línea y rinde solo los objetos válidos.
    
    - Loggea cada intento de lectura y omite líneas mal formateadas.
    - Maneja errores como archivo no encontrado o parseo fallido.
    - Cuenta el número total de líneas y errores de parseo.
    """
    logger.info(f"📂 Iniciando lectura de archivo comprimido: {filepath}")
    line_count = 0
    error_count = 0

    try:
        with gzip.open(filepath, "rt", encoding="utf-8") as file:
            for line in file:
                line_count += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    if validate_log_record(record):
                        yield record
                except json.JSONDecodeError as e:
                    error_count += 1
                    logger.warning(f"Línea malformada #{line_count}: {str(e)}")
    except FileNotFoundError:
        logger.error(f"Archivo no encontrado: {filepath}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado al leer {filepath}: {str(e)}")
        raise
    finally:
        logger.info(f"Líneas leídas: {line_count}, errores de parseo: {error_count}")


def validate_log_record(record: Dict) -> bool:
    """
    Valida que el registro tenga los campos mínimos requeridos para procesamiento.

    Requiere los campos: timestamp, endpoint y status_code.
    Si falta alguno, el registro es descartado y logueado a nivel debug.
    """
    required_fields = {"timestamp", "endpoint", "status_code"}
    if not all(field in record for field in required_fields):
        logger.debug(f"Registro incompleto descartado: {record}")
        return False
    return True
