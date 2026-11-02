#!/usr/bin/env bash
# =============================================================================
# Change generator: simulates an application writing to the source database.
#
# Every CHANGE_INTERVAL_SECONDS it, in a single transaction:
#   * inserts CHANGE_INSERTS new 'pending' orders, and
#   * moves CHANGE_UPDATES random orders one step along their lifecycle
#     (pending -> paid -> shipped -> delivered, sometimes cancelled/refunded).
#
# The BEFORE UPDATE trigger in the source DB bumps updated_at, which is what
# the incremental sync keys on. It runs in a stock postgres image and uses
# psql, so there is nothing to install.
#
# Note: it never DELETEs rows. Hard deletes are invisible to an updated_at
# based sync (see "Deletes" in the README).
# =============================================================================
set -euo pipefail

: "${PGHOST:?}" "${PGUSER:?}" "${PGDATABASE:?}"
INTERVAL="${CHANGE_INTERVAL_SECONDS:-5}"
INSERTS="${CHANGE_INSERTS:-10}"
UPDATES="${CHANGE_UPDATES:-25}"

echo "change-generator: waiting for ${PGHOST}..."
until pg_isready -q; do sleep 1; done
until psql -tAc "SELECT 1 FROM orders LIMIT 1" >/dev/null 2>&1; do sleep 1; done
echo "change-generator: every ${INTERVAL}s -> ${INSERTS} inserts, ${UPDATES} updates"

while true; do
  psql -v ON_ERROR_STOP=1 -q \
       -v inserts="${INSERTS}" -v updates="${UPDATES}" <<'SQL'
BEGIN;

INSERT INTO orders (customer_id, amount, status)
SELECT (1 + floor(random() * 5000))::BIGINT,
       round((5 + random() * 495)::NUMERIC, 2),
       'pending'
FROM generate_series(1, :inserts);

-- Pick random rows cheaply: sample random ids in [min, max] instead of
-- ORDER BY random(), which would sort the whole table every few seconds.
WITH bounds AS (SELECT min(id) AS lo, max(id) AS hi FROM orders),
     picked AS (
       SELECT DISTINCT (lo + floor(random() * (hi - lo + 1)))::BIGINT AS id
       FROM bounds, generate_series(1, :updates)
     )
UPDATE orders o
SET status = CASE o.status
               WHEN 'pending'   THEN (ARRAY['paid', 'paid', 'paid', 'cancelled'])[1 + floor(random() * 4)::INT]
               WHEN 'paid'      THEN 'shipped'
               WHEN 'shipped'   THEN 'delivered'
               WHEN 'delivered' THEN (ARRAY['delivered', 'refunded'])[1 + floor(random() * 2)::INT]
               ELSE o.status    -- cancelled/refunded are terminal; still bumps updated_at
             END,
    amount = CASE WHEN random() < 0.1 THEN round(o.amount * 0.9, 2) ELSE o.amount END
FROM picked
WHERE o.id = picked.id;

COMMIT;
SQL
  sleep "${INTERVAL}"
done
