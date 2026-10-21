# CDC (Change Data Capture) fundamentals

**Date:** Mon 2026-10-19 · **Week:** 2026-W43 · Ingestion into Bronze · **Track:** Data Engineering (NJ in Dollar)
**Pairs with Gerald's Databricks pill:** Lakeflow Connect, managed vs standard connectors

**What it is.** CDC means capturing every insert, update, and delete in a source system and shipping those changes downstream, so you don't have to re-copy the whole table each time. There are three main ways to do it:

1. **Log-based CDC** reads the database's own transaction log (the Postgres WAL, the MySQL binlog, SQL Server CDC tables). It catches deletes and every intermediate change, puts almost no load on the source, and is the gold standard. Debezium is the best-known open-source tool for it.
2. **Query-based CDC** polls with something like `WHERE updated_at > last_watermark`. It's simple, but it misses hard deletes and squashes several updates between polls into one.
3. **Trigger-based CDC** uses database triggers that write to an audit table. It's accurate, but it adds write overhead on the source, so most DBAs push back on it.

**What a change event looks like.** A Debezium event carries `op` (`c` for create, `u` for update, `d` for delete, `r` for snapshot read), a `before` and an `after` image of the row, a source position like an LSN (the log sequence number), and a timestamp `ts_ms`. That position is your ordering key.

## The Databricks link

- **Lakeflow Connect** has managed connectors (SQL Server, Salesforce, Workday, and more) that handle the snapshot plus incremental CDC for you.
- **Bring your own CDC**: Debezium writes to Kafka or S3, and then Auto Loader or Structured Streaming lands the raw events in an append-only Bronze Delta table.

The key design rule is that Bronze keeps every change event exactly as it arrived, deletes included. Applying those changes into a current-state table (MERGE INTO, AUTO CDC, SCD Type 2) is the Silver step.

## Hands-on: landing Debezium JSON in Bronze with Auto Loader

```python
from pyspark.sql import functions as F

bronze = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/main/raw/_schemas/orders")
    .load("s3://my-bucket/debezium/orders/")
    .select(
        F.col("payload.op").alias("op"),
        F.col("payload.before").alias("before"),
        F.col("payload.after").alias("after"),
        F.col("payload.source.lsn").alias("source_lsn"),
        F.col("payload.ts_ms").alias("source_ts_ms"),
        F.current_timestamp().alias("_ingested_at"),
        F.col("_metadata.file_path").alias("_source_file")))

(bronze.writeStream
    .option("checkpointLocation", "/Volumes/main/raw/_chk/orders_cdc")
    .trigger(availableNow=True)
    .toTable("main.bronze.orders_cdc"))
```

Notice there's no MERGE here. The table is append-only, it keeps the ordering columns, and the file lineage is attached to every row.

## Likely interview question

*"A source team says 'just give me a nightly full dump.' When would you push for log-based CDC instead, and how would you land it?"*

**Strong senior answer:** "It depends on table size, freshness needs, and whether deletes matter. For a small reference table, a full snapshot is honestly fine and simpler. But for a large, high-churn OLTP table, full dumps get slow and expensive, they put load on production, and they can't tell you what changed between dumps. In that case I'd go log-based: Debezium or a managed connector reads the log, and I land the raw events append-only in Bronze, keeping the op type, the before and after images, and the LSN. Bronze then becomes a replayable audit trail. I apply the changes downstream in Silver, ordered by the LSN, so late or duplicate events can't corrupt the current state. I'd also plan for the initial snapshot, for schema changes, and for monitoring replication lag and log retention on the source, because if the connector falls behind the retention window you have to re-snapshot."

## English tip

Say "We capture changes from the transaction log" rather than "we take the changes of the log." Another handy phrase: "Bronze is our replayable source of truth."

## Go deeper

- [Lakeflow Connect overview](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/)
- [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [Debezium architecture](https://debezium.io/documentation/reference/stable/architecture.html)
