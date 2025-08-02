-- Índices para optimizar consultas
CREATE INDEX IF NOT EXISTS idx_user_ts ON transactions (user_id, ts);
CREATE INDEX IF NOT EXISTS idx_order_id ON transactions (order_id);

-- Trigger para validar rango de montos en nuevas transacciones
CREATE OR REPLACE FUNCTION validate_amount_range()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.amount < 0 THEN
    RAISE EXCEPTION '🚫 El monto no puede ser negativo: %', NEW.amount;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validate_amount ON transactions;

CREATE TRIGGER trg_validate_amount
BEFORE INSERT OR UPDATE ON transactions
FOR EACH ROW
EXECUTE FUNCTION validate_amount_range();
