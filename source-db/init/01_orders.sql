-- =============================================================================
-- Source database schema + seed data.
--
-- The official postgres image runs every *.sql file in
-- /docker-entrypoint-initdb.d exactly once, when the data volume is empty.
-- To re-seed from scratch:  docker compose down -v  (deletes ALL volumes).
-- =============================================================================

CREATE TABLE IF NOT EXISTS orders (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT         NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL,
    status      TEXT           NOT NULL
                CHECK (status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded')),
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- The incremental sync asks "which rows changed between T1 and T2?", i.e.
--   WHERE updated_at >= :start AND updated_at < :end
-- Without this index every 10-minute sync would scan the whole table.
CREATE INDEX IF NOT EXISTS orders_updated_at_idx ON orders (updated_at);

-- Incremental sync is only correct if updated_at changes on EVERY update.
-- Relying on each application to remember `SET updated_at = now()` is fragile,
-- so the database enforces it with a trigger.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS orders_set_updated_at ON orders;
CREATE TRIGGER orders_set_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ~100k historical orders spread over the last 30 days. Their updated_at is in
-- the past, so only a FULL load (not a 10-minute incremental window) picks
-- them up. That's exactly the "initial load" problem the pipeline solves.
INSERT INTO orders (customer_id, amount, status, created_at, updated_at)
SELECT
    customer_id,
    amount,
    status,
    created_at,
    -- updated some time after creation, but never in the future
    LEAST(created_at + random() * interval '3 days', now()) AS updated_at
FROM (
    SELECT
        (1 + floor(random() * 5000))::BIGINT                       AS customer_id,
        round((5 + random() * 495)::NUMERIC, 2)                    AS amount,
        (ARRAY['pending', 'paid', 'shipped', 'delivered', 'delivered',
               'delivered', 'cancelled', 'refunded'])[1 + floor(random() * 8)::INT] AS status,
        now() - random() * interval '30 days'                      AS created_at
    FROM generate_series(1, 100000)
) AS seed
ORDER BY created_at;  -- ids roughly follow creation time, like a real system

ANALYZE orders;
