# Gold layer objects: table vs view vs materialized view vs streaming table

Date: 2026-11-11 (W46, Wednesday)

## Exam pill

Skew in the Spark UI looks like most tasks finishing quickly while one task runs for minutes, with max shuffle read far above the median.

## Deep dive

**Gold layer objects: table vs view vs materialized view vs streaming table**

The Gold layer is what analysts and BI tools read, so the question is how each object gets its data: stored by you, computed on read, or maintained by the platform. Picking the wrong one costs you either freshness, query speed, or compute.

**How each one works**

A standard Delta table stores data that your own job writes (`INSERT`, `MERGE`, `CREATE OR REPLACE TABLE ... AS`). Nothing refreshes it unless your job runs. A view stores only a query; every read re-executes it against the sources, so it is always current but costs compute each time. A materialized view stores the precomputed result of a query and the platform refreshes it, incrementally when it can, otherwise with a full recompute. A streaming table is a table fed by a streaming query that processes only new data from an append-style source, which is why it fits ingestion and incremental freshness.

**The commands**

- **`CREATE VIEW`**: logical abstraction, no storage, good for renaming columns, joining, or hiding columns.
- **`CREATE MATERIALIZED VIEW`**: for aggregates and joins that are too slow on read. Refreshed by a pipeline or by `REFRESH MATERIALIZED VIEW`, and it can use a schedule.
- **`CREATE STREAMING TABLE`**: for incremental ingestion or transformation from a streaming source such as `STREAM read_files(...)` or another streaming table.
- **`GRANT SELECT`**: access is controlled in Unity Catalog, so analysts need `USE CATALOG` and `USE SCHEMA` plus `SELECT` on the object.

**Example**

```sql
CREATE MATERIALIZED VIEW main.gold.daily_revenue AS
SELECT order_date, SUM(amount) AS revenue
FROM main.silver.orders
GROUP BY order_date;

GRANT SELECT ON TABLE main.gold.daily_revenue TO `analysts`;
```

After creation, the first run builds the result. When new rows land in `main.silver.orders`, a refresh updates `daily_revenue` without your code writing to it. If you declare the same object as a view instead, every dashboard load would rescan Silver.

**Tradeoffs and gotchas**

- Materialized views and streaming tables are created and refreshed by serverless/pipeline compute, so you manage them through a pipeline or SQL warehouse, not by writing to them with `INSERT` or `MERGE`.
- A materialized view is not a streaming table: it is defined by a query over the full source and may recompute, while a streaming table sees each input row once.
- A view gives freshness but no speed, and a table gives speed but staleness until your job runs.
- Streaming tables suit append-only sources; updates or deletes in the source need extra handling.
- Permissions on a view depend on the underlying tables too, so check what the reader can access.

**For the exam:** expect scenario questions that ask which object to use for a freshness or performance need (view for light logic, materialized view when read-time compute is too slow, streaming table for incremental ingestion), plus the UC grants an analyst group needs.
