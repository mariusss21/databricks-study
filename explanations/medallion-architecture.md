# Medallion architecture: Bronze, Silver, Gold

The medallion architecture organizes a lakehouse into three layers of Delta tables, Bronze, Silver, and Gold, and each layer makes the data a bit more trustworthy. It fixes the "one giant messy table" problem: raw, cleaned, and business-ready data each get their own home, so you can reprocess, debug, and govern each one separately.

## How it works under the hood

Data moves one way. A job or pipeline reads one layer's table and writes the next. Every layer is a real Delta table in Unity Catalog, so every write is an ACID commit in `_delta_log`. That gives you history and time travel, and you can rebuild a downstream layer from the one above it without going back to the source system. Unity Catalog records lineage as those reads and writes run, so Catalog Explorer can show which Silver tables feed a given Gold table.

- **Bronze:** raw or near-raw, mostly append-only, loaded with Auto Loader, `COPY INTO`, or Lakeflow Connect. The schema can flex here (a rescued-data column catches surprises), and you keep source metadata like file path and ingest time.
- **Silver:** cleaned, deduplicated, typed, and conformed, often joined across sources. The schema is enforced, and `MERGE INTO` is common for upserts and CDC.
- **Gold:** aggregates and business entities for BI and ML, often materialized views or curated tables, with stable column names.

## Ways to build it

- **Delta tables plus Lakeflow Jobs:** one notebook or SQL task per hop, wired as a DAG.
- **Lakeflow Spark Declarative Pipelines:** streaming tables for Bronze and Silver, materialized views for Gold, and expectations that enforce quality rules between layers.
- **Unity Catalog layout:** usually one schema per layer, such as `sales.bronze`, `sales.silver`, and `sales.gold`.

## Example (Declarative Pipeline, SQL)

```sql
CREATE OR REFRESH STREAMING TABLE bronze_orders AS
SELECT *, _metadata.file_path AS source_file, current_timestamp() AS ingest_ts
FROM STREAM read_files('/Volumes/sales/landing/orders/', format => 'json');

CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT CAST(order_id AS BIGINT) AS order_id,
       CAST(amount AS DECIMAL(10,2)) AS amount,
       to_date(order_ts) AS order_date
FROM STREAM(bronze_orders);

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue AS
SELECT order_date, sum(amount) AS revenue
FROM silver_orders GROUP BY order_date;
```

The pipeline graph shows three linked nodes, and the Silver node reports how many rows the expectation dropped. If daily revenue looks wrong, start at `gold_daily_revenue`, check `silver_orders` for dropped or mistyped rows, then look at the raw payloads in `bronze_orders`. Walk backward along the lineage instead of rebuilding the logic in a scratch notebook.

## Tradeoffs and gotchas

- **The names don't guarantee quality.** A table called "silver" is only clean if casts, dedup, and expectations make it clean.
- **Layers must be tables.** A Silver step that lives only as a temp view or DataFrame breaks lineage, and downstream jobs can't share it.
- **Bronze must not be too strict.** Enforcing a rigid schema at ingest turns every source change into a failed load. Save the strict contract for Silver and Gold.
- **Keep raw data long enough to replay.** If Bronze retention or `VACUUM` is too aggressive, you can't rebuild Silver after a logic bug.
- **Each hop costs something.** More layers mean more storage, compute, and latency, so don't add layers by habit.

## For the exam

Know what belongs in each layer (raw ingest, then cleaned and conformed, then aggregated for business use), that each layer is a Unity Catalog Delta table, and that streaming tables, materialized views, and expectations are how Declarative Pipelines put medallion into practice.
