# Recap: an end-to-end Bronze pipeline that's safe to re-run

Date: Sun 2026-10-25

## Exam pill

A broadcast join sends a small table to every executor so the large table is not shuffled. spark.sql.autoBroadcastJoinThreshold is the cutoff Spark uses for that choice.

## Deep dive

**Recap: an end-to-end Bronze pipeline that's safe to re-run**

This week's pieces fit into one job: land raw files in Bronze, incrementally, and be able to run it again (after a failure, a retry, or a manual click) without duplicating or losing rows. "Safe to re-run" means the pipeline remembers what it already ingested, so re-running is a no-op for old data and only picks up new files.

**How it works, in order**

1. Files land in a Unity Catalog volume or external location.
2. Auto Loader (`cloudFiles`) discovers new files and records each one in its checkpoint (a RocksDB state store under `checkpointLocation`).
3. Each micro-batch is written to the Bronze Delta table, and the checkpoint is committed together with the write. That pairing is what gives exactly-once into Delta: a crashed batch is replayed from the last committed offset, and Delta's transaction log rejects the duplicate commit.
4. A Lakeflow Job runs it on a schedule with `trigger(availableNow=True)`: process everything new, then stop.

**The building blocks**

- **Checkpoint:** one per stream, never shared, never deleted casually. It's the pipeline's memory.
- **`cloudFiles.schemaLocation`:** stores the inferred schema so it stays stable across runs.
- **`_rescued_data`:** catches fields that don't match the schema instead of failing or dropping them.
- **`_metadata`:** gives you `file_path` and `file_modification_time` for lineage and replay.
- **Ingest timestamp:** `current_timestamp()` as `_ingested_at`, so you can tell load time from event time.
- **Append-only:** Bronze keeps raw history; dedup and MERGE happen in Silver.

**Example**

```python
from pyspark.sql import functions as F
(spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", "/Volumes/main/raw/_chk/orders_schema")
  .load("/Volumes/main/raw/landing/orders/")
  .select("*", "_metadata.file_path",
          F.current_timestamp().alias("_ingested_at"))
  .writeStream
  .option("checkpointLocation", "/Volumes/main/raw/_chk/orders")
  .trigger(availableNow=True)
  .toTable("main.bronze.orders"))
```

Run it twice with no new files: the second run finds nothing and writes nothing. Drop three new files in and run again: only those three get appended.

The SQL equivalents keep the same property. `COPY INTO` tracks loaded files in the target table and skips them on re-run. A streaming table in Lakeflow Declarative Pipelines (`CREATE OR REFRESH STREAMING TABLE ... FROM STREAM read_files(...)`) manages the checkpoint for you.

**Gotchas**

- Deleting or moving the checkpoint makes Auto Loader reprocess every file, which duplicates Bronze.
- A file overwritten in place under the same name isn't re-ingested by default (`cloudFiles.allowOverwrites` is off).
- A plain `spark.read` + `append` job has no file memory, so every re-run duplicates data.
- A full refresh of a streaming table truncates and reloads it. That's fine only if the source files are still there.
- Retries at the job level are safe only because of the checkpoint; without one, retries cause duplicates.

**For the exam:** expect scenarios asking which option ingests only new files idempotently (Auto Loader with a checkpoint, or `COPY INTO`), what `availableNow` does, where unexpected columns go (`_rescued_data`), and why Bronze stays append-only while dedup lives in Silver.
