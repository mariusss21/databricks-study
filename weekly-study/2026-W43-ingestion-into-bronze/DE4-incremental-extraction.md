# Incremental extraction: high-water marks, API pagination, and backfills

**Date:** Thu 2026-10-22 · **Week:** 2026-W43 · Ingestion into Bronze · **Track:** Data Engineering (NJ in Dollar)
**Pairs with Gerald's Databricks pill:** JDBC, ODBC, and REST reads from notebooks, scheduled as Lakeflow Jobs

## The core idea

When you can't get a change log (Monday's CDC pill), you pull. The trick is to pull only what changed since last time, safely, on every run.

- **High-water mark (watermark):** you store the biggest `updated_at` (or id) you've already loaded. The next run asks for `WHERE updated_at > :last_mark`. It's simple and cheap, but it has well-known traps:
  - **Ties and late commits.** Rows with the same timestamp, or a transaction that commits after you read, can be skipped. Fix: read with a small **overlap window** (`>= mark - 10 min`) and dedup on the key in Bronze or Silver, or use a composite cursor `(updated_at, id)`.
  - **Deletes are invisible.** A deleted row never shows up in `updated_at > x`. You need soft deletes, real CDC, or a periodic full-key comparison.
  - **Untrusted columns.** If `updated_at` isn't indexed or isn't set by every writer, the "incremental" query is either slow or wrong. Check before you trust it.
- **Commit order matters.** Write the data first, then advance the mark. If the run dies in between, the next run re-reads the same window, and because Bronze is idempotent (Tuesday's pill), that's fine. Advancing the mark first is how you lose data.
- **API pagination:** prefer **cursor or keyset** pagination (`next_cursor`, `since_id`) over offset/page numbers, because offsets shift when data changes mid-read. Respect rate limits: on HTTP 429, honor `Retry-After`, and use exponential backoff with jitter.
- **Backfills:** reload history in bounded chunks (a day or a month at a time), each chunk idempotent, so you can re-run one chunk without touching the others. Backfill and the daily incremental should be the **same code with different parameters**.

## How Databricks does it

- **JDBC reads in parallel:** `spark.read.format("jdbc")` with `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` splits the read across executors. Push the filter down with the `query` option so the source does the work. Credentials go in Databricks secrets, never in the notebook.
- **Where the mark lives:** a small Delta control table (`source, table, last_mark, updated_at`) governed in Unity Catalog. It's auditable and easy to reset.
- **Lakeflow Jobs:** job parameters turn one notebook into "daily incremental" or "backfill 2025-01", and the **For each** task fans a backfill out over a list of dates with controlled concurrency.
- **Idempotent chunk reloads:** Delta `replaceWhere` overwrites exactly one date range, so re-running a backfill chunk replaces it instead of duplicating it.
- **REST sources:** the **PySpark custom data source** API lets you wrap a paginated API as `spark.read.format("my_api")`, with pagination and retries inside the reader.
- **Don't build it if you can buy it:** if Lakeflow Connect has a managed connector for the source (Salesforce, SQL Server, Workday, and others), it handles the cursor, retries, and deletes for you. Hand-rolled extraction is for the long tail.

## Hands-on (PySpark)

```python
from pyspark.sql import functions as F

src = "erp.orders"
mark = (spark.table("main.ops.ingest_marks")
          .where(F.col("source") == src)
          .first()["last_mark"])            # e.g. 2026-10-21 23:50:00

df = (spark.read.format("jdbc")
        .option("url", dbutils.secrets.get("erp", "jdbc_url"))
        .option("query", f"""
            SELECT * FROM orders
            WHERE updated_at >= TIMESTAMP '{mark}' - INTERVAL '10' MINUTE  -- overlap
        """)
        .load()
        .withColumn("_ingested_at", F.current_timestamp()))

df.write.mode("append").saveAsTable("main.bronze.erp_orders")   # 1) data first

new_mark = df.agg(F.max("updated_at")).first()[0]
if new_mark:                                                    # 2) then the mark
    spark.sql(f"""UPDATE main.ops.ingest_marks
                  SET last_mark = TIMESTAMP '{new_mark}', updated_at = current_timestamp()
                  WHERE source = '{src}'""")
```

Try this: kill the job between step 1 and step 2, run it again, and count duplicates in Bronze. Then explain why Saturday's dedup pill exists.

## Likely interview question

*"You need to pull 5 years of history plus daily changes from a Postgres table with 2 billion rows, and a REST API with a 100 requests per minute limit. Walk me through it."*

**Strong senior answer:** "First I'd ask whether there's a CDC option or a managed connector, because log-based CDC beats polling for both deletes and source load. If not: for Postgres, I'd backfill in monthly chunks with a parallel JDBC read on an indexed key, ideally from a read replica, each chunk written with `replaceWhere` so any chunk is safely re-runnable. A For each task runs the chunks with limited concurrency so we don't hammer the source. Then the same code switches to incremental mode with a high-water mark on `updated_at`, a small overlap window, and dedup on the primary key downstream. The mark is only advanced after the write commits. For deletes, I'd use soft-delete flags or a weekly key reconciliation. For the API, I'd use cursor pagination, a client-side rate limiter under the 100 per minute budget, and backoff on 429s, landing raw JSON in Bronze. I'd monitor rows per run and lag against the mark, so a stuck watermark pages someone instead of failing silently."

## English tip

Say "we **advance the watermark** only after the write commits" and "the backfill is **chunked and re-runnable**." Also, **"throttling"** is the natural US word for what an API does when you hit the rate limit ("the API started throttling us").

## Go deeper

- [Query databases using JDBC (Databricks)](https://docs.databricks.com/aws/en/connect/external-systems/jdbc)
- [Parameterize jobs (Databricks)](https://docs.databricks.com/aws/en/jobs/parameters)
- [PySpark custom data sources (Databricks)](https://docs.databricks.com/aws/en/pyspark/datasources)
