# Columnar storage, row updates, liquid clustering, and Z-order

## Why columnar storage fits analytics

Analytics queries usually read a few columns across many rows, for example `SELECT region, SUM(amount) FROM sales GROUP BY region`. Columnar formats (Parquet, which Delta uses) store each column's values together on disk, which gives four advantages:

- **Less I/O (column pruning).** The engine reads only the columns the query touches. On a 50-column table, that query reads 2 columns instead of whole rows.
- **Better compression.** Values in one column share a type and often repeat (country codes, statuses, dates). Dictionary, run-length, and bit-packing encodings shrink them a lot, so there are fewer bytes to read and cache.
- **Data skipping.** Parquet keeps min/max statistics per column chunk, and Delta keeps per-file column statistics in the transaction log. A filter like `WHERE event_date = '2026-10-08'` can skip whole files and row groups without opening them.
- **Vectorized CPU work.** Processing one column at a time suits vectorized execution: Photon and Spark's vectorized Parquet reader process batches of values in tight loops instead of decoding row by row.

## The tradeoff: row updates

Row stores (Postgres, MySQL) keep a whole record together in a page. Updating one row means changing that page in place, writing to the write-ahead log, and touching a few index entries. That's cheap and fast, which is why transactional (OLTP) systems store rows.

In Delta, Parquet files are **immutable**: a file is never edited after it's written. So `UPDATE`, `DELETE`, and `MERGE` work differently.

### Copy-on-write (the classic behavior)

1. Delta uses file statistics to find the files that may contain matching rows.
2. It reads each of those files and writes a **new full copy** with the change applied.
3. It commits a log entry that removes the old file and adds the new one.

The impact is that changing 1 row in a 1 GB file rewrites the whole 1 GB file. This is called write amplification. Reads stay fast because the files are clean, but writes are expensive.

### Merge-on-read (deletion vectors)

With deletion vectors (enabled by default on new tables in recent Databricks Runtime versions):

- A `DELETE` writes a small bitmap file marking which rows in a Parquet file are deleted. The data file isn't rewritten.
- An `UPDATE` marks the old row as deleted in the bitmap and writes the new version of the row to a new, small file.
- Readers apply the bitmaps and filter out deleted rows when they read.
- The rows are physically removed later by `OPTIMIZE`, `REORG TABLE ... APPLY (PURGE)`, or an automatic rewrite.

The impact is much cheaper writes, a small extra cost on reads until compaction, and fewer conflicts between concurrent writers. It also enables row-level concurrency, where conflicts are detected per row instead of per file.

### Side effects of frequent updates on Delta

- **Old files stay around** for time travel until `VACUUM` removes them (default retention is 7 days), so storage grows with update volume.
- **Many small updates create many small files and log entries**, which slows reads until `OPTIMIZE` compacts them. Auto compaction and optimized writes help with this.
- **Concurrent `MERGE`s on the same files can conflict** (optimistic concurrency), so jobs retry or fail.

### Bottom line

Delta is great for batch or micro-batch upserts (a `MERGE` of thousands or millions of rows at once) and bad for high-frequency single-row updates like an app changing one order's status every second. For that OLTP pattern, use a row store such as Lakebase (Postgres on Databricks) and sync it to or from Delta.

## Z-order

**Definition:** `OPTIMIZE table ZORDER BY (col1, col2)` rewrites the table's files so that rows with similar values in those columns are stored in the same files. It uses a space-filling curve (the Z-order curve), which keeps locality across several columns at once instead of sorting by one.

**Why it helps:** Data skipping depends on each file's min/max range for a column. Unsorted data gives every file a wide range (for example, `customer_id` from 1 to 9,999,999), so nothing can be skipped. After Z-ordering, each file covers a narrow range, so a filter like `WHERE customer_id = 42` reads a handful of files instead of all of them.

**Limitations:**

- **It isn't incremental.** New data written after `OPTIMIZE` isn't Z-ordered until you run it again, and each run rewrites a large part of the table, which costs a lot of compute.
- **It gets weaker with more columns.** Each added column dilutes the locality of the others, so 1 to 4 high-cardinality columns that are often used in filters work best.
- **It can't be used on partition columns.** Partitioning is a separate, coarser mechanism.
- **Changing the Z-order columns means rewriting** the data again with the new columns.

## Liquid clustering

**Definition:** You declare clustering keys on the table, for example `CREATE TABLE ... CLUSTER BY (customer_id, event_date)` or `ALTER TABLE ... CLUSTER BY (...)`, and Delta keeps the data organized by those keys over time. It replaces both Hive-style partitioning and Z-order, and a table can't combine liquid clustering with either of them. Databricks recommends liquid clustering for all new Delta tables.

**How it works:**

- **It's incremental.** `OPTIMIZE` clusters only the data that isn't clustered yet, so repeated runs are cheap compared with re-Z-ordering.
- **Keys can change without a rewrite.** `ALTER TABLE ... CLUSTER BY (new_cols)` applies to new data and future `OPTIMIZE` runs. Existing data keeps its old layout unless you run `OPTIMIZE ... FULL` to recluster everything.
- **Automatic option.** `CLUSTER BY AUTO`, with predictive optimization, lets Databricks pick and adjust the keys based on the table's query history.
- **Up to 4 clustering keys**, chosen from columns that are often used in filters and joins.
- **Clustering on write.** Some write operations cluster data as it lands once it's above a size threshold. The rest is handled by `OPTIMIZE`, which predictive optimization can run for you.

**Impact:**

- Data skipping works as well as Z-order or better, without its full-rewrite cost.
- It avoids partitioning's problems: no tiny files from high-cardinality partition columns, no skew from uneven partitions, and no table rebuild when query patterns change.
- It works well with deletion vectors and row-level concurrency, so concurrent writers conflict less.

## Side-by-side

| | Partitioning | Z-order | Liquid clustering |
|---|---|---|---|
| Defined by | `PARTITIONED BY` at create time | `OPTIMIZE ... ZORDER BY` each run | `CLUSTER BY` on the table (or `AUTO`) |
| Granularity | One directory per value (coarse) | Within files | Within files |
| Incremental | Yes (new partitions) | No, rewrites on each run | Yes |
| Change columns | Full table rewrite | Rewrite on the next run | `ALTER TABLE`, no rewrite needed |
| Good for | Low-cardinality columns on very large tables (legacy) | Existing tables that already use it | Default for new tables |
| Combine with others | Can't use with liquid | Can't Z-order partition columns | Not with partitioning or Z-order |

## Exam angles

- Column pruning and file or row-group skipping explain why selecting only the columns you need and filtering on clustered columns makes queries faster and cheaper.
- `UPDATE`, `DELETE`, and `MERGE` on Delta rewrite files (copy-on-write) unless deletion vectors are on (merge-on-read).
- `VACUUM` removes files no longer referenced after the retention period, and after it runs you can't time travel to older versions.
- For new tables, use liquid clustering rather than partitioning or Z-order. `CLUSTER BY AUTO` with predictive optimization is the hands-off choice.
- Lots of small files call for `OPTIMIZE` (compaction), and liquid clustering tables cluster during `OPTIMIZE`.
