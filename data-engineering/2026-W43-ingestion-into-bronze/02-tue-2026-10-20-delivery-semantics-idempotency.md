# Delivery semantics and idempotency

**Date:** Tue 2026-10-20 · **Week:** 2026-W43 · Ingestion into Bronze · **Track:** Data Engineering (NJ in Dollar)
**Pairs with Gerald's Databricks pill:** Auto Loader checkpoints, exactly-once, and trigger modes

## The three guarantees

- **At-most-once:** each event lands zero or one times. It's fast, but you can lose data (fire and forget).
- **At-least-once:** nothing gets lost, but retries can create duplicates. This is the default for Kafka consumers, Debezium, and most APIs.
- **Exactly-once:** every event affects the result one time. In practice you never get this from transport alone. It's an **end-to-end property** built from three pieces: a *replayable source* (Kafka offsets, files in cloud storage), *tracked progress* (a checkpoint), and a *transactional or idempotent sink*.

**Idempotency** is the key word. An operation is idempotent if running it twice gives the same result as running it once. If your sink is idempotent, at-least-once delivery becomes "effectively exactly-once."

## How Databricks does it

- **Structured Streaming checkpoints** record source offsets and committed batches. After a crash, the query restarts from the last commit.
- A **Delta Lake sink** (`writeStream.toTable(...)`) is transactional, so with a checkpoint you get exactly-once into Delta right out of the box.
- **Auto Loader** (`cloudFiles`) records every file it has ingested in its checkpoint, so each file is processed once even if the job restarts.
- **`foreachBatch` is the trap.** Inside it you write arbitrary code, so it's at-least-once unless you make the write idempotent, with Delta's `txnAppId` / `txnVersion` options or a `MERGE` on a key.
- **Batch vs streaming is a trigger choice, not a rewrite.** `trigger(availableNow=True)` runs streaming code as an incremental batch: it processes everything new, then stops. Streaming tables in Lakeflow Declarative Pipelines handle checkpoints for you.

## Hands-on (PySpark)

```python
def write_bronze(df, batch_id):
    (df.write.format("delta")
       .option("txnAppId", "orders_bronze_ingest")  # stable id per stream
       .option("txnVersion", batch_id)              # Delta skips a replayed batch
       .mode("append")
       .saveAsTable("main.bronze.orders_raw"))

(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", "/Volumes/main/bronze/_schemas/orders")
   .load("/Volumes/main/landing/orders/")
   .writeStream
   .foreachBatch(write_bronze)
   .option("checkpointLocation", "/Volumes/main/bronze/_checkpoints/orders")
   .trigger(availableNow=True)   # streaming semantics, batch cost
   .start())
```

Try this: run it, kill it in the middle, and rerun it. The row count shouldn't double. Then delete the checkpoint folder and run it again. Now everything replays, which shows that **the checkpoint is part of your data's state, not a temp folder.**

## Likely interview question

*"Your pipeline claims exactly-once. What does that really mean, and where can it break?"*

**Strong senior answer:** "Exactly-once is end to end, not a transport feature. Our source is replayable, Kafka or files. Spark checkpoints track offsets and committed batches, and the Delta sink is transactional, so a retried micro-batch doesn't double-write. It breaks in a few known places. First, side effects inside `foreachBatch`, like calling an API or writing to a second table, so we make those idempotent with `txnAppId`/`txnVersion` or a `MERGE` on a business key. Second, a lost or reset checkpoint, which replays history, so we treat checkpoints as production state with their own retention and change process. Third, duplicates that already exist at the source. CDC tools like Debezium are at-least-once, so Bronze stays append-only and we dedupe downstream on key plus log sequence number. So I'd describe it as at-least-once transport with idempotent sinks, which makes it effectively exactly-once."

## English tip

"Idempotent" is pronounced *eye-DEM-puh-tent* or *eye-dem-POH-tent*, and both are fine. One phrase that makes you sound senior is "**effectively exactly-once**."

## Go deeper

- [Idempotent writes in foreachBatch (Databricks)](https://docs.databricks.com/aws/en/structured-streaming/foreach)
- [Structured Streaming checkpoints (Databricks)](https://docs.databricks.com/aws/en/structured-streaming/checkpoints)
- [Trigger intervals and availableNow (Databricks)](https://docs.databricks.com/aws/en/structured-streaming/triggers)
