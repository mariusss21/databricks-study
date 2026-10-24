# Dedup and replay: business keys, late and duplicate data, append-only Bronze vs MERGE on ingest

**Date:** Sat 2026-10-24 · **Week:** 2026-W43 · Ingestion into Bronze · **Track:** Data Engineering (NJ in Dollar)
**Pairs with Gerald's Databricks pill:** Bronze table design: append-only, `_metadata` file columns, ingest timestamps, rescued data

## Why duplicates are normal

On Tuesday we said most pipelines are at-least-once. Today is the bill for that. Duplicates come from three places:

- **Transport retries.** A Kafka producer retries, a Debezium connector restarts and re-emits changes since its last committed offset, an API call times out after the server already answered.
- **Source re-sends.** A vendor drops the same file twice, or re-exports "the full day" with yesterday's rows in it.
- **Your own replays.** You reprocess a window on purpose (a bug fix, a backfill), and the overlap window from Thursday's watermark trick re-reads rows by design.

So the question in an interview is never "how do you avoid duplicates?" It's "**where** do you remove them, and with **which key**?"

## Three kinds of "duplicate"

1. **Exact duplicate:** the same event delivered twice. Same payload, same source offset or LSN. Safe to drop.
2. **Same business key, new version:** order 42 updated twice. Not a duplicate. That's history, and you keep it in Bronze.
3. **Late or out-of-order:** version 2 arrives before version 1. You need an ordering column from the *source* (LSN, `updated_at`, a sequence number), never your ingest time.

The senior move is naming the **dedup key** explicitly: for CDC events it's `(primary key, LSN)`; for files it's `(source file, row offset)`; for API events it's the event id the producer gives you. A business key alone (like `order_id`) is the dedup key only for "latest state" tables, which belong in Silver.

## Append-only Bronze vs MERGE on ingest

| | Append-only Bronze (recommended default) | MERGE on ingest |
|---|---|---|
| What lands | Every event, duplicates included, plus `_metadata.file_path`, `_ingested_at` | Only new keys, deduped at write time |
| Replay | Easy: rebuild Silver from Bronze anytime | Harder: the raw history is gone |
| Cost | Cheap appends, more storage | Each write scans and joins the target |
| Where dedup happens | Bronze to Silver, with `QUALIFY ROW_NUMBER()` or AUTO CDC | At ingest |

**Rule of thumb:** keep Bronze append-only and replayable, and dedup on the way to Silver. Use an **insert-only MERGE** in Bronze only when duplicate volume is huge (a chatty vendor re-sending everything) and the key is a true event id.

## How Databricks helps

- **Streaming exact-dupe removal:** `dropDuplicatesWithinWatermark(["event_id"])` drops repeats that arrive within the watermark delay, with bounded state. Plain `dropDuplicates` on a stream keeps state forever unless you also set a watermark.
- **Insert-only MERGE:** `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` writes only unseen keys. Delta's docs call this out as the dedup pattern. Add a date predicate to the `ON` clause so the merge doesn't scan the whole table.
- **Batch dedup in SQL:** `QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY lsn DESC) = 1` gets the latest version per key in one line.
- **Replay tools:** Delta **time travel** (`VERSION AS OF`) and `RESTORE TABLE` let you roll a bad load back; a full refresh of a streaming table in Lakeflow Declarative Pipelines reprocesses from source.
- **Next week's teaser:** `AUTO CDC` (formerly `APPLY CHANGES`) in Lakeflow Declarative Pipelines does key-plus-sequence dedup and out-of-order handling for you with `KEYS` and `SEQUENCE BY`. We'll cover it with MERGE and SCD2 in the Bronze-to-Silver week.

## Hands-on

Streaming, drop exact duplicates in Bronze with bounded state:

```python
bronze = (spark.readStream.table("main.bronze.orders_cdc_raw")
  .withWatermark("source_ts", "2 hours")
  .dropDuplicatesWithinWatermark(["order_id", "lsn"]))
```

Batch, latest state per key on the way to Silver:

```sql
CREATE OR REPLACE TABLE main.silver.orders AS
SELECT *
FROM main.bronze.orders_cdc_raw
WHERE op <> 'd'
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY lsn DESC) = 1;
```

Insert-only MERGE, when you must dedup at ingest:

```sql
MERGE INTO main.bronze.vendor_events t
USING new_batch s
ON  t.event_id = s.event_id
AND t.event_date >= current_date() - INTERVAL 7 DAYS   -- prune the scan
WHEN NOT MATCHED THEN INSERT *;
```

Note: the delete filter in the batch example is simplified. A deleted key should drop out only if its *latest* event is a delete, which is exactly what AUTO CDC handles properly.

## Interview question

**"Your upstream is at-least-once and a partner re-sends files. How do you design Bronze so the data is correct and you can still replay?"**

**Strong answer:** "I keep Bronze append-only and immutable: every event lands with lineage columns like the source file path from `_metadata`, the ingest timestamp, and the source ordering field, such as the LSN. I don't dedup by business key there, because two updates to the same order are history, not duplicates. I remove exact duplicates with a real event identity, either the producer's event id or `(primary key, LSN)`, using `dropDuplicatesWithinWatermark` in streaming so state stays bounded. Then Silver gets the latest state per key, ordered by the source sequence and never by ingest time, with `AUTO CDC` or a `QUALIFY ROW_NUMBER()` step. Because Bronze keeps everything, a bug fix is a replay: I rebuild Silver from Bronze, and if a load itself was bad I can `RESTORE` the Delta table to an earlier version. I'd only switch to an insert-only MERGE at ingest when duplicate volume makes storage or downstream cost a real problem, and I'd prune the merge with a date predicate. The trade-off I'd call out is storage and some compute in exchange for replayability and auditability, and in my experience that trade is almost always worth it."

## English phrasing tip

Say "**dedupe**" (verb, sounds like *dee-DOOP*) and "**deduplication**" (noun). "Replay" and "reprocess" are both fine, and "**backfill**" means filling in a historical window. A natural sentence: *"Bronze is our source of truth for replays, so we dedupe on the way to Silver, not on the way in."*

## Go deeper

- [Upsert into a Delta table using merge (see "Data deduplication when writing into Delta tables")](https://docs.databricks.com/aws/en/delta/merge)
- [Apply watermarks to control data processing thresholds (dropDuplicatesWithinWatermark)](https://docs.databricks.com/aws/en/structured-streaming/watermarks)
- [QUALIFY clause in Databricks SQL](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-qualify)
- [Delta table history, time travel, and RESTORE](https://docs.databricks.com/aws/en/delta/history)
