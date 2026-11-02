---
title: Hard deletes
description: Why an updated_at sync never sees deleted rows, and the three ways to capture them.
sidebar:
  order: 3
---

The sync asks one question: *which rows have `updated_at` in this window?* A row that's **hard-deleted**
(`DELETE FROM orders WHERE id = 42`) simply stops existing in the source. No window will ever contain it,
so it stays in Iceberg forever, looking like a live order.

The change generator never deletes, so the sandbox doesn't hit this. A real source would. There are three
ways to handle it.

## 1. Soft deletes

The application stops deleting. It sets `deleted_at = now()` instead, and because that's an UPDATE, the
trigger bumps `updated_at` too. The row then flows through the normal sync, and the MERGE gets one more
clause:

```sql
MERGE INTO lake.sales.orders AS t
USING source_orders AS s
ON t.id = s.id
WHEN MATCHED AND s.deleted_at IS NOT NULL THEN DELETE
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED AND s.deleted_at IS NULL THEN INSERT *
```

Alternatively, keep the tombstones in Iceberg and have readers filter `WHERE deleted_at IS NULL`.

- **Pros:** no new infrastructure, and the pipeline stays exactly as it is.
- **Cons:** every writer has to cooperate. One script that runs a real `DELETE` breaks the guarantee
  silently. The source also keeps the dead rows, so its tables grow.

## 2. Change data capture with Debezium

Instead of querying the table, read the database's own change log. Postgres records every change in its
write-ahead log (WAL). **Debezium**, running in Kafka Connect, reads it through a logical replication slot
and publishes each INSERT, UPDATE and DELETE as an event, usually to a Kafka topic:

```json
{ "op": "d", "before": { "id": 42, "status": "cancelled", ... }, "after": null,
  "source": { "lsn": 24023128, "ts_ms": 1793605625000 } }
```

Spark, either batch or Structured Streaming, then applies the events in order:

```sql
MERGE INTO lake.sales.orders AS t
USING latest_change_per_id AS c
ON t.id = c.id
WHEN MATCHED AND c.op = 'd' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED AND c.op <> 'd' THEN INSERT *
```

Note `latest_change_per_id`: several events for the same id in one batch must be collapsed to the last
one, for example by log sequence number (LSN), because a MERGE fails if two source rows match the same
target row.

- **Pros:** captures deletes and *every* intermediate version, not just the latest state. It puts no
  query load on the source tables, needs no `updated_at` discipline, and the latency can be seconds rather
  than minutes.
- **Cons:** a lot more infrastructure (Kafka, Kafka Connect, schema handling). A replication slot that
  nobody consumes makes Postgres retain WAL until the disk fills. Initial snapshots and schema changes
  need care.

## 3. Periodic reconciliation

Every so often, compare the set of ids in the source with the set in Iceberg and delete the ones that are
gone. It's an anti-join, the same idea as the difference checks in `jobs/verify_sync.py`:

```sql
DELETE FROM lake.sales.orders
WHERE id NOT IN (SELECT id FROM source_ids_snapshot)
```

- **Pros:** works with any source, catches deletes made by anyone, and fits into a nightly DAG.
- **Cons:** it reads every id on both sides, which is expensive on big tables. Deletes show up late, and
  rows inserted after the id snapshot was taken must not be deleted by mistake.

## Which one?

| If… | Use |
| --- | --- |
| You control every writer and deletes are rare | Soft deletes |
| You need deletes promptly, or need every version | CDC (Debezium) |
| You can't change the source and a day's delay is fine | Reconciliation |

Many production pipelines combine them: CDC for freshness, plus an occasional reconciliation as a safety
net.
