# Lakeflow Spark Declarative Pipelines: streaming tables vs materialized views, flows, update modes

Date: 2026-11-16

## Exam pill

Troubleshooting objectives explicitly include cluster startup failures, library conflicts, and out-of-memory errors.

## Deep dive

**Lakeflow Spark Declarative Pipelines: streaming tables, materialized views, flows, and update modes**

A declarative pipeline lets you describe the datasets you want, and Databricks works out the dependency graph, ordering, checkpoints, and retries. You stop writing orchestration code and just define tables.

**How it works.** You write SQL or Python source files. The pipeline analyzes them, builds a DAG from the references between datasets, and runs an *update* that materializes each dataset in dependency order. Each dataset is fed by a *flow*, which is the query that writes into it. A streaming table gets an append flow that reads incrementally. A materialized view gets a batch query whose result the platform keeps fresh. For streaming tables, checkpoints are managed for you.

**The two main dataset types**
- **Streaming table:** `CREATE OR REFRESH STREAMING TABLE bronze AS SELECT * FROM STREAM read_files(...)`. It processes each input row once, so it suits append-style sources such as Auto Loader, Kafka, or another streaming table.
- **Materialized view:** `CREATE MATERIALIZED VIEW daily_sales AS SELECT ...`. It stores the query result and refreshes it. The platform may refresh incrementally instead of recomputing everything, depending on the query. Use it for aggregations, joins, and gold tables.
- **Temporary views:** private to the pipeline and not published. They are good for intermediate logic.
- **Python equivalents:** the `@dp.table` decorator (older code uses `@dlt.table`) and `@dp.materialized_view`. Check the current docs for exact names in your runtime.

**Update modes**
- **Triggered:** the update processes available data, then stops and releases compute. It is cheaper and is usually driven by a Job or a schedule.
- **Continuous:** the pipeline keeps running for low latency, and it costs more because compute stays up.
- **Full refresh vs refresh:** a normal refresh is incremental. A full refresh clears the target and reprocesses all source data. Be careful with it on streaming tables whose source has already expired old data, because the data you lose cannot be rebuilt.

**Example.** Bronze is a streaming table over a landing volume. Silver is a streaming table that selects and cleans bronze. Gold is a materialized view that aggregates silver by day. When a new file lands and you trigger an update, only the new files flow through bronze and silver. Gold is then refreshed. In the pipeline graph you see each node's status and row counts.

**Tradeoffs and gotchas**
- A streaming table reads its source append-only. If upstream updates or deletes rows, the flow fails unless you handle that explicitly. In that case, consider an MV.
- Tables are owned by the pipeline, so don't write to them from outside. Change the definition in source.
- Define the catalog and schema once, as pipeline settings or parameters, and don't hard-code them.
- Run the pipeline as a service principal in production.
- Triggered vs continuous is a cost and latency decision. It is not a correctness one.

**For the exam:** know when to pick a streaming table (incremental, append-only ingestion) vs a materialized view (aggregations and results that need recomputing), and the difference between triggered and continuous updates.
