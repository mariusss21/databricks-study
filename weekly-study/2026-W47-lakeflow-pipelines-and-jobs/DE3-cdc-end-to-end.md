# DE3: CDC end to end (capture, ordering, deletes, idempotent apply)

- **Date:** Wed 2026-11-18
- **Week:** 2026-W47
- **Theme:** Lakeflow pipelines and Jobs in depth
- **Paired Databricks pill (Gerald):** DB3: AUTO CDC for SCD Type 1 and Type 2 inside pipelines

---

Good morning, Mario. Gerald covers **how** AUTO CDC writes SCD1/SCD2 today. This pill covers the **whole road** a change takes, from the source database to your Delta table, and the four things that break on that road. In US interviews, "design CDC from Postgres into the lakehouse" is one of the most common senior prompts.

## 1. The road: capture, transport, apply

| Stage | Options | What can go wrong |
|---|---|---|
| **Capture** | **Log-based** (Debezium reads the Postgres WAL / MySQL binlog) or **query-based** (`WHERE updated_at > :hwm`) | Query-based misses hard deletes and intermediate updates, and needs a reliable `updated_at` |
| **Transport** | Kafka (Debezium on Kafka Connect), files in S3, or a managed connector (Lakeflow Connect, Fivetran) | Duplicates (at-least-once), events out of order across partitions |
| **Apply** | `MERGE INTO` in a job, or AUTO CDC in a pipeline | Wrong order, missed deletes, double-applied events |

**Rule of thumb:** log-based when you need deletes, every intermediate state, or low latency, and you can get access to the DB log. Query-based when the source team won't give log access and "latest state per day" is enough.

## 2. The four hard problems

1. **Ordering.** Kafka only orders within a partition (Debezium keys by primary key, so per-row order is kept), but files and retries reorder things. Never trust arrival order. Use a **sequence column** from the source: the log position (LSN) or a commit timestamp.
2. **Duplicates.** Transport is at-least-once, so the same change can arrive twice. The apply must be **idempotent**: replaying a batch leaves the same result.
3. **Deletes.** Debezium emits a delete event (`op = 'd'`) and then a Kafka **tombstone** (null value) for compaction. You decide: hard delete in Silver, or soft delete (`is_deleted = true`) to keep history.
4. **Late events.** An older change arrives after a newer one. The apply must **ignore anything older than what's already in the target**.

## 3. Databricks connection

- **Hand-written:** dedupe the batch to the latest change per key, then `MERGE INTO` with a sequence guard (below). This works in any job, inside `foreachBatch`.
- **Declarative:** `AUTO CDC INTO ... KEYS (...) SEQUENCE BY ... APPLY AS DELETE WHEN ...` (formerly APPLY CHANGES) handles ordering, late events, and deletes for you, as SCD1 or SCD2. That's Gerald's lane today.
- **Downstream:** turn on **Delta Change Data Feed** on Silver so Gold reads only row-level changes (`table_changes` / `readChangeFeed`) instead of rescanning. That's CDC *inside* the lakehouse.
- **Snapshot sources** (no log at all): `create_auto_cdc_from_snapshot_flow` diffs successive snapshots for you.

## 4. Hands-on: idempotent, order-safe MERGE

```sql
-- bronze_customers_cdc: Debezium events flattened: id, name, city, op ('c','u','d'), lsn
MERGE INTO silver.customers AS t
USING (
  SELECT * FROM bronze_customers_cdc_batch
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY lsn DESC) = 1   -- 1 change per key
) AS s
ON t.id = s.id
WHEN MATCHED AND s.lsn > t.lsn AND s.op = 'd' THEN DELETE              -- late-safe delete
WHEN MATCHED AND s.lsn > t.lsn THEN UPDATE SET *                        -- late-safe update
WHEN NOT MATCHED AND s.op != 'd' THEN INSERT *;
```

The `s.lsn > t.lsn` guard is what makes replays and late events harmless. One catch for interviews: once a row is hard-deleted, a late update for it would be re-inserted. That's why AUTO CDC keeps delete tombstones for a while, and why many teams soft-delete in Silver.

## 5. Tech gap sidebar: Snowflake Streams and Tasks

- **Stream:** an offset on a table that shows rows changed since you last consumed it (`METADATA$ACTION`, `METADATA$ISUPDATE`). Reading it in a DML transaction advances the offset. Closest Databricks equivalent: **Change Data Feed** plus a streaming checkpoint.
- **Task:** a scheduled or triggered SQL statement, often `WHEN SYSTEM$STREAM_HAS_DATA('s')` running a `MERGE`. Closest Databricks equivalent: a **Lakeflow Job** with a table-update trigger, or a pipeline.
- **Why US teams use it:** CDC inside the warehouse with pure SQL, no Spark. **Trade-off:** you hand-write every MERGE and its ordering logic, where AUTO CDC declares it. Snowflake's **Dynamic Tables** are the closer match to a declarative pipeline.

## 6. Interview question

**"Design CDC from a Postgres OLTP database into the lakehouse. How do you guarantee correctness?"**

> "I'd start with the requirements: latency, whether we need deletes and full history, and whether we can read the WAL. If yes, I'd use log-based capture with Debezium into Kafka, or Lakeflow Connect if it supports the source, because query-based capture misses hard deletes. I'd land raw events in Bronze unchanged, append-only, so I can always replay. Then I'd apply them to Silver with AUTO CDC, keyed on the primary key and sequenced by the LSN, so out-of-order and duplicate events are safe, with SCD2 where the business needs history. Correctness comes from three things: a source-provided sequence instead of arrival time, an idempotent apply so replays are harmless, and explicit delete handling. I'd also run a daily reconciliation, row counts and checksums per key range against the source, because CDC pipelines drift silently. On my team I'd own the runbook for the classic failure: the replication slot gets dropped or the WAL is recycled, and we need a fresh snapshot plus a resume from the new offset."

**English tip:** say "**replay-safe**" or "**idempotent apply**" instead of "it doesn't duplicate." And "the source of truth for ordering is the LSN, **not arrival time**" is a strong, quotable line.

## Go deeper

- [Databricks: The AUTO CDC APIs](https://docs.databricks.com/aws/en/ldp/cdc)
- [Databricks: Use change data feed](https://docs.databricks.com/aws/en/delta/delta-change-data-feed)
- [Snowflake: Introduction to streams](https://docs.snowflake.com/en/user-guide/streams-intro)
- [Debezium connector for PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
