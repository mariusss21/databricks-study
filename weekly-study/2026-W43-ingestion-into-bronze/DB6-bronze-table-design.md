# Bronze table design: append-only, _metadata file columns, ingest timestamps, rescued data

Date: Saturday, 2026-10-24

## Exam pill

An inner join keeps only matching keys. A left join keeps every row from the left side and nulls where the right side does not match.

## Deep dive

**Bronze table design: append-only, `_metadata` file columns, ingest timestamps, rescued data**

Bronze is the raw, replayable record of what arrived from a source. A good Bronze table answers three questions for any row later on: where did it come from, when did we receive it, and did anything fail to fit the schema? Design it so you never lose data and can always rebuild Silver from it.

**How it works under the hood**

Auto Loader (or a Lakeflow Spark Declarative Pipelines streaming table) discovers new files, tracks them in its checkpoint, and appends each batch to a Delta table as a new commit in `_delta_log`. Nothing gets updated in place. While reading, Spark can expose a hidden `_metadata` struct for every file-based source, so you can copy file details into real columns in the same write. Any field that doesn't match the schema (unexpected column, wrong type, case mismatch) goes into a rescued data column instead of failing the stream or quietly turning into null.

**The building blocks**

- **Append-only writes:** use `outputMode("append")` or a streaming table. Bronze keeps duplicates and late rows on purpose; dedup and MERGE happen in Silver. You can enforce it with the table property `delta.appendOnly = true`, which blocks UPDATE and DELETE.
- **`_metadata` columns:** `_metadata.file_path`, `file_name`, `file_size`, `file_modification_time`, plus block offset and length fields. It's hidden, so you only get it if you select it explicitly.
- **Ingest timestamp:** add `current_timestamp()` as something like `_ingested_at`. It records when *you* received the row, which is different from the source's event time and the file's modification time.
- **Rescued data:** when Auto Loader infers the schema, it adds `_rescued_data` by default. With an explicit schema, turn it on with the `rescuedDataColumn` option. The column holds the mismatched fields as JSON plus the source file path.

**Example**

```python
from pyspark.sql import functions as F

(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", "/Volumes/main/raw/_chk/orders_schema")
   .load("/Volumes/main/raw/landing/orders/")
   .select("*",
           F.col("_metadata.file_path").alias("_source_file"),
           F.col("_metadata.file_modification_time").alias("_file_mtime"),
           F.current_timestamp().alias("_ingested_at"))
   .writeStream
   .option("checkpointLocation", "/Volumes/main/raw/_chk/orders")
   .trigger(availableNow=True)
   .toTable("main.bronze.orders"))
```

Say a file arrives where `amount` is `"12,50"` instead of a number. The row still lands, `amount` is null, and `_rescued_data` holds `{"amount":"12,50","_file_path":"..."}`. A quick `WHERE _rescued_data IS NOT NULL` shows you exactly which files and fields drifted.

**Tradeoffs and gotchas**

- Append-only means duplicates are expected. If the source resends a file under a new name, it lands twice, so plan for business-key dedup downstream.
- `SELECT *` does not include `_metadata`. Pick the fields you want, or they're gone once the batch is written.
- Use `current_timestamp()`, not the file's modification time, as your ingest time. Re-copied files can keep old mtimes.
- If nobody checks `_rescued_data`, rescued fields pile up unnoticed. Alert on it.
- Keep Bronze columns loose (strings or `VARIANT`) when the source is messy. Strict types belong in Silver.

**For the exam:** Know that Bronze is raw and append-only, that `_metadata` gives file lineage in Auto Loader and COPY INTO, and that a rescued data column catches schema mismatches without failing ingestion.
