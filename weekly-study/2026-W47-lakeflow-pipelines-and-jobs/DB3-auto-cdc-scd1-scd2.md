# AUTO CDC for SCD Type 1 and Type 2 inside pipelines

Date: 2026-11-18 (Wed, W47)

## Exam pill

The guide’s retired sample picks a high-concurrency autoscaling cluster for many simultaneous ad hoc SQL users (question 4, answer C). Current product docs more often point that workload at a SQL warehouse, so treat the sample as an exam answer, not as today’s only architecture.

## Deep dive

**AUTO CDC (formerly APPLY CHANGES): SCD Type 1 and Type 2 in pipelines**

AUTO CDC is the Lakeflow Spark Declarative Pipelines API that turns a stream of change events (inserts, updates, deletes) into a target table that reflects the current state, or the full history. It replaces hand-written MERGE logic and, importantly, handles out-of-order events for you.

**How it works.** You declare a target streaming table and a flow that reads a change feed (a streaming table or source, typically from Auto Loader or Kafka/CDC output). You tell it the **keys** (which identify a row), a **sequence_by** column (the ordering of events, e.g. a timestamp or log sequence number), and what to do with deletes. For each micro-batch the pipeline orders events by the sequence column, so a late older event does not overwrite a newer one. For SCD Type 1 it upserts the latest version per key. For Type 2 it closes the old row and inserts a new one.

**Main options**
- **keys**: the columns that identify a record. Without a stable key there is no CDC.
- **sequence_by**: the ordering column. It must increase with change order, not arrival order.
- **apply_as_deletes**: an expression (e.g. `op = 'DELETE'`) marking events that remove the row.
- **stored_as_scd_type**: 1 (overwrite, no history) or 2 (history kept).
- **except_column_list**: columns not copied to the target, such as the operation flag.
- **track_history_column_list**: for Type 2, only changes to these columns create a new version.

Type 2 adds `__START_AT` and `__END_AT` columns; the current row has a null `__END_AT`.

**Example (SQL)**
```sql
CREATE OR REFRESH STREAMING TABLE customers_silver;

CREATE FLOW customers_cdc AS AUTO CDC INTO customers_silver
FROM STREAM(customers_cdc_bronze)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY event_ts
COLUMNS * EXCEPT (op)
STORED AS SCD TYPE 2;
```
If customer 7 changes city twice, Type 1 shows one row with the latest city; Type 2 shows three versions with non-overlapping start/end timestamps, and only the latest is open. If the second event arrives before the first, the result is the same.

**Tradeoffs and gotchas**
- Type 2 grows with every change; filter on `__END_AT IS NULL` for current state.
- A wrong or non-monotonic sequence column silently gives wrong results.
- The target is managed by the pipeline; don't write to it from elsewhere.
- The source must be a stream. If the source itself is updated or deleted in place, a plain streaming read fails, so feed it from an append-only change feed.
- Older code uses the `APPLY CHANGES INTO` / `dlt.apply_changes()` names; they are the same idea.

For the exam: know that AUTO CDC / APPLY CHANGES is the declarative way to process CDC feeds, what keys and sequence_by are for, and the Type 1 (overwrite) vs Type 2 (history) difference.
