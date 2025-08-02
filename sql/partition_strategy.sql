-- Estrategia de partición lógica por rango de fechas (mes a mes)
DROP TABLE IF EXISTS transactions CASCADE;

CREATE TABLE transactions (
  transaction_id TEXT,
  user_id TEXT,
  status TEXT,
  transaction_date TIMESTAMP,
  amount NUMERIC
) PARTITION BY RANGE (transaction_date);

-- Particiones manuales por mes
CREATE TABLE transactions_2025_07 PARTITION OF transactions
  FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');

CREATE TABLE transactions_2025_08 PARTITION OF transactions
  FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
