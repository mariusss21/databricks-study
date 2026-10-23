# Choosing an ingestion tool: Connect vs Auto Loader vs COPY INTO vs JDBC

Date: Fri 2026-10-23

## Exam pill

The silver step on the exam is read a bronze table with PySpark or SQL, clean nulls, standardize types, and write a new silver table.

## Deep dive

**Choosing an ingestion tool: Lakeflow Connect vs Auto Loader vs COPY INTO vs JDBC**

All four get outside data into a Unity Catalog Delta table, but they solve different problems. The right pick depends on where the data lives (a SaaS app, a database, or files in cloud storage), how often it arrives, and who should own the incremental logic: Databricks or you.

**How each one tracks what's new.** This is the real difference between them:

- **Lakeflow Connect managed connectors:** you point a connector at a SaaS app or a database, and Databricks builds an ingestion pipeline that writes streaming tables. For databases, an ingestion gateway reads change data capture (CDC) from the source, so inserts, updates, and deletes come through incrementally without you writing watermark logic.
- **Auto Loader (`cloudFiles`):** a Structured Streaming source for files. It discovers new files by directory listing or file events, records every processed file in its checkpoint, and gives you exactly-once ingestion across restarts. It can infer and evolve the schema (`cloudFiles.schemaLocation`) and saves unexpected fields in `_rescued_data`.
- **`COPY INTO`:** a SQL command that loads files from a path into a table. It remembers which files it already loaded, so re-running it is idempotent and skips them. It's a bounded batch with no stream or checkpoint to manage.
- **JDBC reads:** `spark.read.format("jdbc")` pulls rows from a database you can reach over the network. Spark tracks nothing for you. Incremental means you write your own high-water-mark filter, and parallel reads need `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions`.

**A quick decision path:**

- **Source is Salesforce, Workday, SQL Server, and similar, and a managed connector exists:** use Lakeflow Connect.
- **Files keep landing in cloud storage or a volume:** use Auto Loader, ideally inside a Lakeflow Spark Declarative Pipeline, which manages the checkpoint and schema location for you.
- **A one-off or occasional bounded file load, run by a SQL-first team:** use `COPY INTO`.
- **A database with no managed connector, or a small lookup pull:** use JDBC in a notebook scheduled as a Lakeflow Job.

**Example.** A landing volume gets new CSVs every hour:

```sql
COPY INTO main.bronze.orders
FROM '/Volumes/main/landing/orders/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
```

Run it twice and the second run loads 0 rows, because those files are already recorded. That works fine for a few thousand files. Once the folder grows into a constant stream of arrivals, switch to Auto Loader with `trigger(availableNow=True)`. It still runs as a scheduled batch, but file discovery scales much better.

**Tradeoffs and gotchas:**

- `COPY INTO` and Auto Loader both skip a file that is overwritten under the same name, unless you opt in (`'force' = 'true'` for `COPY INTO`, `cloudFiles.allowOverwrites` for Auto Loader).
- If you load the same files into one table with both tools, you get duplicates, because each tool keeps its own record of what it loaded.
- JDBC full pulls hammer the source database, and a missing watermark means you reload everything on every run.
- Managed connectors cover a fixed list of sources and run on paid pipeline compute, so check that your source is supported before you design around one.

For the exam: expect scenario questions that hand you a source and ask for the tool. Remember that `COPY INTO` is idempotent and fits bounded loads, Auto Loader is the incremental and scalable choice for files that keep arriving, and Lakeflow Connect is the managed path for SaaS apps and databases.
