"""
run_pipeline_entry.py — Punto de entrada principal para ejecutar el pipeline ETL

Este script permite lanzar la ejecución del pipeline ETL completo, indicando el archivo de entrada y el nombre lógico del proceso.
"""

from etl.pipeline_runner import run_pipeline

if __name__ == "__main__":
    # Ejecuta el pipeline ETL para el archivo de transacciones
    run_pipeline("data/sample_transactions.csv", "transactions")
