# End-to-end Bronze to Silver to Gold that is re-runnable

Date: 2026-11-10

## Exam pill

The Jobs UI shows status, the task DAG (to see which upstream task blocked the rest), run time, and failure rate.

## Deep dive

**End-to-end Bronze → Silver → Gold that is correct and re-runnable**

A medallion pipeline is only useful if you can run it twice and get the same result. Re-runnability is what lets you retry a failed job, backfill a day, or repair a run without duplicating rows or losing data.

**How it works, in order:**
1. **Bronze** ingests raw data append-only, with metadata (source file, ingest time). With Auto Loader or `COPY INTO`, a checkpoint or file-tracking state remembers which files were already loaded, so a retry doesn't load them again.
2. **Silver** cleans, casts types, deduplicates on the business key, and applies quality rules. Writing it with `MERGE INTO` (upsert on the key) is idempotent: the same input twice updates the same rows instead of adding new ones.
3. **Gold** is aggregated for consumption. Either rebuild the affected slice deterministically (`INSERT INTO ... REPLACE WHERE`, or dynamic partition overwrite), or use a materialized view that Databricks refreshes for you.

**Main options:**
- **MERGE INTO** – upsert on a key; use `WHEN MATCHED` / `WHEN NOT MATCHED` clauses. Duplicate source keys for one target row make it fail, so dedupe first.
- **INSERT INTO ... REPLACE WHERE** – atomically replaces only the rows matching a predicate (for example one date), so a re-run overwrites rather than appends.
- **Overwrite (`INSERT OVERWRITE` / `mode("overwrite")`)** – replaces the whole table; simple, but expensive on big data.
- **Plain append** – not idempotent. A retry duplicates data.
- **Materialized view / streaming table in Lakeflow pipelines** – declarative, and the engine handles incremental logic.

**Example:** a daily revenue Gold table.
```sql
MERGE INTO silver.orders t
USING (SELECT * FROM bronze_orders_batch QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1) s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

INSERT INTO gold.daily_revenue
REPLACE WHERE order_date = '2026-11-10'
SELECT order_date, sum(amount) AS revenue FROM silver.orders WHERE order_date = '2026-11-10' GROUP BY order_date;
```
Run it twice: Silver row count stays the same, and Gold's 2026-11-10 row is replaced with identical values, not doubled.

**Gotchas and tradeoffs:**
- Non-deterministic logic (`current_timestamp()`, random values) inside transforms makes re-runs differ. Pass the run date in as a parameter instead.
- Dedupe before `MERGE`, or it errors on multiple matches.
- Overwriting everything is safe but costly; scoping by a predicate is cheaper but needs a correct predicate.
- Delta time travel and `RESTORE` can undo a bad run, but within the retention window only.

**For the exam:** know which write patterns are idempotent (MERGE, REPLACE WHERE, overwrite) versus not (append), why checkpoints stop Bronze reloads, and that duplicate source keys break MERGE.
