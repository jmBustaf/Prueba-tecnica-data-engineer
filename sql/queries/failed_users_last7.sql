-- Usuarios con más de 3 transacciones fallidas en los últimos 7 días
SELECT user_id, COUNT(*) AS failed_attempts
FROM transactions
WHERE status = 'failed'
  AND ts::timestamp >= NOW() - INTERVAL '7 days'
GROUP BY user_id
HAVING COUNT(*) > 3
ORDER BY failed_attempts DESC;
