"""
run_analysis.py — Script para ejecutar todos los archivos SQL del ejercicio 2

Este script automatiza la ejecución de:
- Consultas analíticas (detección de errores y anomalías)
- Vistas agregadas
- Índices y triggers
- Estrategia de partición por fechas
"""


import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# Lista de archivos SQL en orden lógico de ejecución
SQL_FILES = [
    "sql/views/daily_summary.sql",
    "sql/queries/failed_users_last7.sql",
    "sql/queries/detect_anomalies.sql",
    "sql/indices_and_triggers.sql",
    "sql/partition_strategy.sql",
]

def run_sql_file(cursor, path):
    """
    Ejecuta el contenido de un archivo SQL dado.
    """

    print(f"Ejecutando {path}...")
    with open(path, "r", encoding="utf-8") as file:
        sql = file.read()
        cursor.execute(sql)
    print("Completado.\n")

def main():
    # Conexión a PostgreSQL usando variables de entorno
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Ejecutar cada script SQL
    for sql_path in SQL_FILES:
        run_sql_file(cursor, sql_path)

    # Cerrar conexión
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
