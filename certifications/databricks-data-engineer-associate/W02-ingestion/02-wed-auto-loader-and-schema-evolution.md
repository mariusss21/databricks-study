# W2 · P2 — Auto Loader, schema inference and schema evolution

**Wed Nov 25, 2026 · ~15–20 min · Exam Section 2 (Data Ingestion and Loading)**
⚙ Exam topic: Auto Loader (`cloudFiles` / `read_files`) with schema enforcement and evolution, in directory listing or file notification mode
🧱 Fundamental: schema-on-read vs schema-on-write, schema drift, and pull vs push file discovery

---

## 0) Warm-up (answer before reading; answers in §5)

**W1.** A Python Auto Loader stream reads JSON with `cloudFiles.schemaLocation` set and no schema provided. All other options are defaults. A new field `coupon_code` starts showing up. What happens?
- A) The field is dropped silently and the stream keeps running
- B) The stream stops once, then a restart loads the new column
- C) The field lands in `_rescued_data` and the stream keeps running
- D) The stream stays down until someone edits the schema by hand

**W2.** A brand-new Python stream, `spark.readStream.format("cloudFiles")`, reads CSV files with a header and default options. In the Bronze table, `amount` comes out as `STRING`. Why?
- A) Auto Loader reads CSV and JSON columns as strings by default
- B) The first sampled file had an empty `amount` value in every row
- C) Bronze tables in Unity Catalog store every column as a string
- D) Auto Loader only infers numeric types from files over 1,000 rows

**W3.** The Silver team wants a schema that never changes on its own. But unexpected fields must not be lost, and the stream must not fail when they appear. Which `cloudFiles.schemaEvolutionMode` fits?
- A) `addNewColumns`
- B) `none`
- C) `failOnNewColumns`
- D) `rescue`

**W4.** An Auto Loader stream watches a landing location that already holds millions of files and keeps growing. Each micro-batch spends most of its time just finding new files. What does Databricks recommend?
- A) Directory listing with a higher `cloudFiles.maxFilesPerTrigger`
- B) `COPY INTO` with a `PATTERN` that matches only today's folder
- C) File notification mode using file events on the location
- D) Directory listing with `cloudFiles.includeExistingFiles` off

---

## 1) The idea in one breath

Auto Loader is a streaming source that finds new files, remembers them in a checkpoint (so each file is processed exactly once), **infers** a schema, and decides what to do when that schema **drifts**. Today is about the two choices you make: how it finds files, and how it reacts to new columns.

## 2) The fundamental (tool-agnostic)

**Schema-on-write vs schema-on-read**

| | Schema-on-write | Schema-on-read |
|---|---|---|
| When the schema is checked | When data is written | When data is read |
| Bad data | Rejected at the door | Lands, and breaks someone later |
| Typical home | Warehouse tables, Silver/Gold | Raw files, data lake landing zones |
| Delta's default | Yes, enforcement | — |

A lakehouse mixes both. Raw files are schema-on-read: CSV or JSON with no promises. The Delta tables you write are schema-on-write. **Ingestion is where you convert one into the other**, so it is where you decide what drift is allowed.

**Schema drift: three honest answers.** When a source adds a column, a pipeline can:
1. **Evolve:** add the column to the table. Good for Bronze, where you want to mirror the source.
2. **Quarantine:** keep the unexpected value aside (a "rescue" column or a dead-letter table) and keep running. Good when downstream contracts are strict but you can't lose data.
3. **Fail:** stop and wait for a human. Good when a schema change must go through review.

What's never acceptable is silently dropping data nobody knows about.

**Finding new files: pull vs push**
- **Pull (directory listing):** list the folder every run and compare with what you've seen. Simple and needs no extra setup, but cost grows with the **number of files in the folder**, not the number of new ones.
- **Push (notifications):** the storage service emits an event per new file into a queue, and you read the queue. Cost grows with **new files only**. It's the same idea as a webhook vs polling an API.

Your older note goes deeper on Delta's side of this: [Schema enforcement vs. schema evolution on Delta](../../../explanations/schema-enforcement-and-evolution.md). For streaming basics (checkpoints, triggers): [Spark Structured Streaming course notes](../../../courses/structured-streaming/02-spark-structured-streaming.html).

## 3) How Databricks does it

**SQL (what you'd use in a pipeline or on a SQL warehouse):** a streaming table over `read_files`, which runs Auto Loader underneath. The pipeline manages the checkpoint and schema location and restarts after schema changes for you.

```sql
CREATE OR REFRESH STREAMING TABLE main.bronze.orders_raw
AS SELECT *, _metadata.file_path AS source_file
FROM STREAM read_files(
  '/Volumes/main/raw/landing/orders',
  format => 'json',
  schemaHints => 'amount DECIMAL(10,2), order_ts TIMESTAMP'
);
-- Later: find drift without losing rows
SELECT source_file, _rescued_data
FROM main.bronze.orders_raw
WHERE _rescued_data IS NOT NULL;
```

**Python (a job outside a pipeline):** you own the schema location and checkpoint.

```python
(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", "/Volumes/main/raw/_meta/orders/schema")
   .option("cloudFiles.inferColumnTypes", "true")          # else JSON/CSV -> strings
   .option("cloudFiles.schemaHints", "amount DECIMAL(10,2)")
   .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # the default
   .option("cloudFiles.useManagedFileEvents", "true")      # file events (recommended)
   .load("/Volumes/main/raw/landing/orders")
 .writeStream
   .option("checkpointLocation", "/Volumes/main/raw/_meta/orders/checkpoint")
   .trigger(availableNow=True)                            # incremental batch, then stop
   .toTable("main.bronze.orders_raw"))
```

**Inference.** Auto Loader samples the first **50 GB or 1,000 files** (whichever comes first) and stores the schema in `_schemas` under `cloudFiles.schemaLocation`. For JSON, CSV and XML, the `cloudFiles` source infers **every column as a string** unless `cloudFiles.inferColumnTypes = true`. Parquet and Avro use the types in the files. `schemaHints` overrides types for the columns you know, and can add a column that isn't there yet.

**Evolution modes** (`cloudFiles.schemaEvolutionMode`), when a new column arrives:

| Mode | Schema changes? | Stream fails? | New column ends up |
|---|---|---|---|
| `addNewColumns` (default, no schema given) | Yes, appended at the end | Once (`UnknownFieldException`); a restart continues | As a real column after restart |
| `rescue` | Never | No | In `_rescued_data` |
| `failOnNewColumns` | No | Yes, until you fix the schema or remove the file | Nowhere yet |
| `none` (default when you give a schema) | No | No | Ignored, unless `rescuedDataColumn` is set |

There's also `addNewColumnsWithTypeWidening` (Public Preview): like `addNewColumns`, plus safe widening such as `int` → `long`.

**`_rescued_data`.** Added automatically when Auto Loader infers the schema. It's a JSON blob holding values that didn't fit: unknown columns, **type mismatches** and **case mismatches**, plus the source file path. With a schema you provide, turn it on with `rescuedDataColumn`.

**File discovery modes**
- **Directory listing** (the default): no setup, fine for small or medium folders.
- **File notification**: Databricks now recommends it **with file events** for most workloads (`cloudFiles.useManagedFileEvents = true`, with file events enabled on the Unity Catalog external location). The older flavor, `cloudFiles.useNotifications = true`, has Auto Loader create its own queue, and needs cloud permissions for that.

## 4) Tradeoffs and exam traps

- **Trap: "the stream failed on a new column, it's broken."** With `addNewColumns`, stopping once is by design: the schema was already updated before the error. Run it in a Lakeflow Job with retries (or inside a pipeline) so the restart is automatic.
- **Trap: `rescue` vs `none`.** Both keep the schema fixed and don't fail. `rescue` keeps the new data in `_rescued_data`; `none` drops it unless you set `rescuedDataColumn`.
- **Trap: `failOnNewColumns` doesn't fix itself.** A restart fails again until someone changes the schema or removes the file.
- **Trap: strings everywhere.** The Python `cloudFiles` source reads JSON/CSV/XML as strings unless `inferColumnTypes` is on. `read_files` is the opposite: it infers real types by default. Exam questions about Auto Loader usually mean the `cloudFiles` behaviour.
- **Trap: `addNewColumns` with a provided schema.** Not allowed. If you want to pin some types and still evolve, use `schemaHints` instead of a full schema.
- **Trap: changing existing types.** `addNewColumns` adds columns; it never changes the type of an existing one. A type mismatch is rescued instead.
- **Trap: schema location vs checkpoint.** Schema location = the inferred schema and its history. Checkpoint = which files were processed (RocksDB) and stream progress. You can point both at the same folder, but you need **one checkpoint per source** feeding a table.
- **Trap: "file notification needs an always-on stream."** It doesn't. Discovery mode and trigger are separate choices; `availableNow` works with both.
- **Tradeoff:** evolve in Bronze, enforce in Silver and Gold. A loose Bronze keeps every row; a strict Silver makes drift visible instead of quietly breaking dashboards.

## 5) Warm-up answers

**W1 → B (stops once, restart picks it up).** With no schema provided, the default is `addNewColumns`: Auto Loader merges the new column into the schema location, then throws `UnknownFieldException`. The next run uses the new schema. A) Dropping new columns is `none`, which is only the default when you provide a schema. C) Rescuing without evolving is `rescue` mode, not the default. D) Staying down until a manual fix is `failOnNewColumns`.

**W2 → A (strings by default).** For JSON, CSV and XML, the `cloudFiles` source infers every column as a string, to avoid type clashes later. Fix it with `cloudFiles.inferColumnTypes = true` or a `schemaHints` entry for `amount`. B) Empty values wouldn't explain it; every column would be a string even with perfect data. C) Unity Catalog has no rule like that; the type comes from the stream. D) There's no row-count threshold; the sample is up to 50 GB or 1,000 files.

**W3 → D (`rescue`).** The schema never evolves, the stream never fails on new columns, and unexpected values are kept in `_rescued_data`. A) `addNewColumns` changes the schema, which the Silver team doesn't want. B) `none` keeps running but drops the new fields unless `rescuedDataColumn` is set. C) `failOnNewColumns` stops the stream.

**W4 → C (file notification with file events).** Directory listing re-lists the whole location each time, so it slows down as the folder grows. Notifications only deal with new files, and Databricks recommends the file events flavor for most workloads. A) A bigger batch size doesn't make the listing itself any cheaper. B) `COPY INTO` still lists files and is meant for thousands, not millions. D) `includeExistingFiles` only matters on the first run; later listings still scan everything.

## 6) Interview angle

*"A source team keeps adding fields without telling you. How do you design for it?"*
Name the three choices (evolve, quarantine, fail) and match them to layers: Bronze evolves (Auto Loader `addNewColumns`, run with automatic retries), every row keeps a `_rescued_data` column and its source file path, and Silver has an explicit schema, so a new field shows up as a reviewed change, not a surprise. Then add the operational piece: alert when `_rescued_data IS NOT NULL` rows appear, so drift becomes a ticket, not an outage. If they ask about scale, mention notification-based discovery instead of listing a growing folder.

## 7) Optional hands-on (10 min)

Use a `catalog.schema` and a volume you own (replace `my_cat.my_sch.landing`):

```python
base = "/Volumes/my_cat/my_sch/landing"
dbutils.fs.put(f"{base}/al/o1.json", '{"id": 1, "amount": 10}', True)

def run():
    (spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", f"{base}/_meta/schema")
       .load(f"{base}/al")
     .writeStream
       .option("checkpointLocation", f"{base}/_meta/checkpoint")
       .option("mergeSchema", "true")
       .trigger(availableNow=True)
       .toTable("my_cat.my_sch.p2_orders")
     .awaitTermination())

run()                                   # 1 row; note amount is a STRING
dbutils.fs.put(f"{base}/al/o2.json", '{"id": 2, "amount": 20, "coupon": "X"}', True)
# run()  -> fails with UnknownFieldException (schema already updated)
# run()  -> succeeds; coupon is now a column
```

```sql
SELECT * FROM my_cat.my_sch.p2_orders;   -- look at the types and _rescued_data
DROP TABLE my_cat.my_sch.p2_orders;      -- clean up (and delete the _meta folder)
```

## Official docs

- [What is Auto Loader?](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [Schema inference and evolution in Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
- [Auto Loader file detection modes](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-detection-modes)
- [Auto Loader options](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options)
- [read_files (SQL function)](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files)
- [Exam guide](https://www.databricks.com/learn/certification/data-engineer-associate)

*Optional further reading (not homework):* [kengio guide: COPY INTO and Auto Loader](https://github.com/kengio/databricks-certification-study-guide/blob/main/certifications/data-engineer-associate/02-data-ingestion-and-loading/02-copy-into-and-auto-loader.md)

---
*Reply with your warm-up answers (e.g. `W1 B, W2 C ...`). Any misses go into Monday's review. Tomorrow: Lakeflow Connect, managed connectors for databases and SaaS apps.*
