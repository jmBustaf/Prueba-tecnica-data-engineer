-- Crea una vista agregada de transacciones por día y estado
CREATE OR REPLACE VIEW daily_summary AS
SELECT
  DATE(ts) AS day,
  status,
  COUNT(*) AS total_transactions
FROM transactions
GROUP BY day, status
ORDER BY day, status;
