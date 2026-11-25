# W2 · P1 — Batch, streaming, incremental, and COPY INTO

**Tue Nov 24, 2026 · ~15–20 min · Exam Section 2 (Data Ingestion and Loading)**
⚙ Exam topic: `COPY INTO` for incremental, idempotent file loads into Unity Catalog tables
🧱 Fundamental: batch vs streaming vs incremental, idempotent loads, and delivery semantics

---

## 0) Warm-up (answer before reading; answers in §5)

**W1.** A nightly job runs `COPY INTO main.bronze.orders FROM '/Volumes/main/raw/landing/orders' FILEFORMAT = CSV`. Last night it loaded 40 files. Tonight no new files arrived, and the job is rerun twice by mistake. What happens?
- A) Each rerun appends the same 40 files again as duplicates
- B) Both reruns succeed and load zero new rows
- C) The second rerun fails with a duplicate-file error
- D) Each rerun reloads the files and overwrites the table

**W2.** A vendor re-sends `orders_2026-11-20.csv` with corrected values, same name, same folder. `COPY INTO` already loaded the first version. How do you make `COPY INTO` load it again?
- A) Rerun as is; it notices the file's newer modification time
- B) Rerun with `FILES` naming that file and `'force' = 'true'`
- C) Run with `VALIDATE ALL` first to reset the loaded-file list
- D) Rerun with `'mergeSchema' = 'true'` so new values are accepted

**W3.** A pipeline retries every failed write, and the sink just appends rows with no key or dedup. Under failures, what guarantee do you really get?
- A) At-least-once: nothing is lost, but duplicates can appear
- B) At-most-once: duplicates can't appear, but data can be lost
- C) Exactly-once: the sink quietly absorbs every retry
- D) Effectively-once: the retry log drops the repeated rows

**W4.** About 2 million new JSON files land in a volume every day, and new columns show up now and then. A SQL-first team wants to schedule `COPY INTO` every 5 minutes. What do you recommend?
- A) Keep `COPY INTO`; it scales to that file count the same way
- B) Keep `COPY INTO` but add a `PATTERN` to shorten each listing
- C) Use Auto Loader, e.g. a streaming table over `read_files`
- D) Rebuild the table with CTAS over `read_files` every 5 minutes

---

## 1) The idea in one breath

*Batch vs streaming* is about **how** data is processed (bounded chunks vs an unbounded flow). *Incremental* is about **what** is processed (only what's new since last time), and you can do it either way. `COPY INTO` is the simplest incremental tool on Databricks: a SQL command that remembers which files it already loaded, so you can rerun it safely.

## 2) The fundamental (tool-agnostic)

**Three processing styles**

| | Full batch | Incremental batch | Streaming |
|---|---|---|---|
| Input | Everything, every run | Only new data since the last run | An unbounded flow, as it arrives |
| Needs state? | No | Yes: a watermark, a file list, or a checkpoint | Yes: a checkpoint |
| Latency | Hours | Minutes to hours | Seconds to minutes |
| Cost profile | Grows with total data | Grows with new data | Always-on compute (unless triggered) |
| Example | Nightly CTAS rebuild | `COPY INTO`, Auto Loader with `availableNow` | Auto Loader running continuously |

The key point is that incremental needs **memory of what was already done**. Whoever keeps that memory (you, a watermark column, a checkpoint, the tool) decides whether a rerun is safe.

**Delivery semantics.** When something fails and is retried, a pipeline gives one of three guarantees:

- **At-most-once:** never retry. No duplicates, but you can lose data.
- **At-least-once:** retry until it succeeds. No loss, but duplicates are possible.
- **Exactly-once:** each record affects the result once. In practice it is built as **at-least-once delivery plus an idempotent or transactional sink**.

**Idempotent** means running it twice gives the same result as running it once. That's what makes a load "safe to rerun." It's the most useful property in data engineering, because retries, backfills and "oops, I clicked Run again" all become harmless. Common ways to get it: track processed files, `MERGE` on a business key instead of `INSERT`, or overwrite a whole partition.

Your older notes go deeper: Delivery semantics and idempotency (older note, removed), Incremental extraction (older note, removed), End-to-end Bronze pipeline, safe to rerun (older note, removed).

## 3) How Databricks does it

`COPY INTO` loads files from cloud storage or a Unity Catalog volume into a Delta table. It records which files it loaded **in the target table's metadata**, and skips them on later runs. That gives you file-level idempotency for free.

```sql
-- 1) Optional: empty placeholder table, schema is inferred on first load
CREATE TABLE IF NOT EXISTS main.bronze.orders;

-- 2) Incremental, idempotent load
COPY INTO main.bronze.orders
FROM '/Volumes/main/raw/landing/orders'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
-- Rerun it: num_inserted_rows = 0 unless new files arrived

-- Dry run: check parsing without loading anything
COPY INTO main.bronze.orders
FROM '/Volumes/main/raw/landing/orders'
FILEFORMAT = CSV
VALIDATE 15 ROWS
FORMAT_OPTIONS ('header' = 'true');

-- Reload one file on purpose (turns idempotency off for this run)
COPY INTO main.bronze.orders
FROM '/Volumes/main/raw/landing/orders'
FILEFORMAT = CSV
FILES = ('orders_2026-11-20.csv')
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS ('force' = 'true');
```

The options to know:

| Option | What it does |
|---|---|
| `FILEFORMAT` | CSV, JSON, XML, Avro, ORC, Parquet, text, binary file |
| `FILES = (...)` | Explicit file list, up to 1,000 files. Can't be combined with `PATTERN` |
| `PATTERN = '...'` | Glob filter, e.g. `'2026-11-*.csv'` |
| `VALIDATE` | Checks the data (all rows or N rows) **without loading** |
| `FORMAT_OPTIONS ('mergeSchema')` | Infer a schema across many source files |
| `COPY_OPTIONS ('mergeSchema')` | Let the **target table** evolve with new columns |
| `COPY_OPTIONS ('force')` | Reload files even if they were loaded before |

**Where it fits.** `COPY INTO` is a good fit for SQL-first teams and folders with **thousands** of files. For millions of files, continuous arrival, or heavy schema drift, Databricks recommends **Auto Loader**, which SQL users get through a streaming table (tomorrow's pill):

```sql
CREATE OR REFRESH STREAMING TABLE main.bronze.orders_st
AS SELECT * FROM STREAM read_files('/Volumes/main/raw/landing/orders', format => 'csv');
```

In Python it's still the same command: `spark.sql("COPY INTO ...")`. Your older tool-choice note: Choosing an ingestion tool (older note, removed).

## 4) Tradeoffs and exam traps

- **Trap: "a rerun loaded 0 rows, something's broken."** That's idempotency working. Already-loaded files are skipped.
- **Trap: changed file, same name.** It is still skipped, even if modified after loading. Only `'force' = 'true'` reloads it, and the rows from the first version **stay** in the table. Fix duplicates downstream (dedup or `MERGE` into Silver).
- **Trap: idempotent ≠ deduplicated.** `COPY INTO` is idempotent **per file**. If two different files contain the same order, both rows land. Row-level dedup is your job.
- **Trap: the two `mergeSchema` options.** In `FORMAT_OPTIONS` it merges schemas while reading the source files. In `COPY_OPTIONS` it lets the target table get new columns. Without the second one, a new column makes the load fail on a schema mismatch.
- **Trap: `VALIDATE` loads nothing.** It's a dry run.
- **Trap: `FILES` and `PATTERN` together.** Not allowed; pick one.
- **Trap: "streaming" = always running.** Auto Loader with `trigger(availableNow=True)` is streaming code run as an incremental **batch**: process what's new, then stop.
- **Tradeoff:** `COPY INTO` = simple, SQL-only, no checkpoint to manage, retryable. Auto Loader = scales to millions of files, better schema evolution and rescued data, but needs a checkpoint and schema location (managed for you inside pipelines).

## 5) Warm-up answers

**W1 → B (zero new rows).** `COPY INTO` remembers loaded files and skips them, so reruns are harmless. A) Duplicates would only happen with `force` or a plain `INSERT`. C) Skipping is silent; there's no duplicate-file error. D) `COPY INTO` always appends; it never overwrites the table.

**W2 → B (`FILES` + `force`).** `force` turns off the skip-already-loaded check, and `FILES` limits the reload to that one file. Remember that the old rows are still there. A) Modified files are skipped too; the modification time doesn't trigger a reload. C) `VALIDATE` is a dry run and doesn't touch load tracking. D) `mergeSchema` is about new columns, not changed values in a known file.

**W3 → A (at-least-once).** Retries mean nothing is lost, but an append-only sink with no key will write a retried batch twice. B) At-most-once means *no* retries. C) Exactly-once needs an idempotent or transactional sink, which this one isn't. D) "Effectively once" also depends on the sink deduplicating, and nothing here does that.

**W4 → C (Auto Loader / streaming table).** Millions of files a day plus schema drift is exactly Auto Loader's case, and a streaming table over `read_files` keeps the team in SQL. A) Databricks positions `COPY INTO` for thousands of files, not millions per day. B) `PATTERN` filters names but doesn't change how `COPY INTO` scales or handles drift. D) A CTAS rebuild reprocesses everything every run, so it isn't incremental at all.

## 6) Interview angle

*"How do you make an ingestion pipeline safe to rerun?"*
Start with the fundamental: retries give at-least-once delivery, so exactly-once comes from making the sink idempotent. Then the layers: track what was processed (file list for `COPY INTO`, checkpoint for Auto Loader, a watermark for database extracts), write atomically (Delta transactions mean a failed run leaves nothing half-written), and dedup by business key with `MERGE` when moving to Silver. Close with a concrete example: "Our Bronze load is `COPY INTO` from a volume. If the job fails halfway, we just rerun it. Loaded files are skipped and the Delta commit is all-or-nothing."

## 7) Optional hands-on (10 min)

Use a `catalog.schema` you own and a volume in it (replace `my_cat.my_sch.landing`):

```python
dbutils.fs.put("/Volumes/my_cat/my_sch/landing/orders/o1.csv", "id,amount\n1,10\n2,20", True)
```

```sql
CREATE TABLE IF NOT EXISTS my_cat.my_sch.p1_orders;
COPY INTO my_cat.my_sch.p1_orders
FROM '/Volumes/my_cat/my_sch/landing/orders'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');   -- 2 rows inserted
-- Run the same COPY INTO again: 0 rows inserted
DESCRIBE HISTORY my_cat.my_sch.p1_orders; -- look at the COPY INTO operation
DROP TABLE my_cat.my_sch.p1_orders;       -- clean up
```

## Official docs

- [Get started with COPY INTO](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into/)
- [COPY INTO (SQL reference)](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into)
- [Ingest data into a lakehouse (overview)](https://docs.databricks.com/aws/en/ingestion/)
- [What is Auto Loader?](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [Exam guide](https://www.databricks.com/learn/certification/data-engineer-associate)

*Optional further reading (not homework):* [kengio guide: COPY INTO and Auto Loader](https://github.com/kengio/databricks-certification-study-guide/blob/main/certifications/data-engineer-associate/02-data-ingestion-and-loading/02-copy-into-and-auto-loader.md)

---
*Reply with your warm-up answers (e.g. `W1 B, W2 C ...`). Any misses go into Monday's review. Tomorrow: Auto Loader and schema evolution.*
