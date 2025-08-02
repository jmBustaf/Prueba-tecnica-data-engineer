"""
run_star_pipeline.py — Script principal para ejecutar el pipeline de carga al modelo estrella.

Incluye llamadas secuenciales a:
- load_dim_users: carga usuarios únicos a la dimensión dim_users.
- load_dim_time: carga fechas únicas a la dimensión dim_date.
- load_fact_transactions: carga hechos en fact_transactions usando claves foráneas.

Este archivo sirve como punto de entrada principal al pipeline.
"""

from modeling.scripts.load_dim_user import load_dim_users
from modeling.scripts.load_dim_time import load_dim_time
from modeling.scripts.load_fact_transactions import load_fact_transactions

CSV_PATH = "data/sample_transactions.csv"

def main():
    """
    Ejecuta el pipeline de carga al modelo en estrella.
    Llama en orden a los módulos de carga para dimensiones y hechos.
    """
    print("Iniciando carga al modelo estrella...")
    load_dim_users(CSV_PATH)
    load_dim_time(CSV_PATH)
    load_fact_transactions(CSV_PATH)
    print("Carga finalizada exitosamente.")

if __name__ == "__main__":
    main()
