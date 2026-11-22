# Lakeflow as one path: Connect, Declarative Pipelines, Jobs

2026-11-22 (Sunday, DB7)

## Exam pill

The exam expects the Databricks CLI to validate and deploy those bundles in CI. The commands are databricks bundle validate and databricks bundle deploy.

## Deep dive

**Lakeflow as one path: Connect, Declarative Pipelines, Jobs**

Lakeflow is Databricks' answer to "how does data get in, get shaped, and run reliably". Three pieces, one flow: Connect lands data, Declarative Pipelines transform it, Jobs orchestrate everything around it.

**How it fits together, in order**
1. **Lakeflow Connect** ingests from a source (SaaS app or database) through a managed connector. It creates a gateway/ingestion pipeline and writes streaming tables in Unity Catalog, picking up changes incrementally after the initial load.
2. **Lakeflow Spark Declarative Pipelines** read those tables as Bronze and declare the rest: streaming tables for append-style incremental steps, materialized views for aggregates and joins, flows to feed a target. You declare the result; the engine works out dependencies, order, checkpoints and retries.
3. **Quality and CDC live inside the pipeline.** Expectations (`EXPECT ... ON VIOLATION WARN / DROP ROW / FAIL UPDATE`) check rows, and AUTO CDC (formerly APPLY CHANGES) applies change feeds as SCD Type 1 or Type 2.
4. **Lakeflow Jobs** schedule and chain it: a pipeline task runs the pipeline, other tasks (notebook, SQL, Python) run before or after, with task dependencies, parameters, task values, if/else and for-each.

**Where each concern lives**
- **Ingestion and incremental loading:** Connect, or Auto Loader for files.
- **Transformations and data quality:** Declarative Pipelines.
- **Scheduling, retries, alerts, repair:** Jobs.
- **Observability:** the pipeline event log for quality and update details, job run history and system tables for runs.

**Example scenario**
A Salesforce connector feeds `bronze.accounts`. A pipeline reads it, drops rows failing `account_id IS NOT NULL`, applies AUTO CDC into a Silver SCD2 table, and builds a Gold materialized view. A Job triggers the pipeline nightly, then runs a SQL task refreshing a dashboard dataset, and emails on failure. If the pipeline fails, you fix it and use a repair run, which reruns only failed and dependent tasks.

**Tradeoffs and gotchas**
- Managed connectors cover specific sources; others still need custom ingestion.
- Pipeline code is declarative: avoid side effects and don't treat it like a notebook you run cell by cell.
- Triggered updates process available data and stop; continuous keeps compute running (more cost, lower latency).
- A job's pipeline task starts an update; orchestration logic belongs in the job, not inside the pipeline.

**For the exam:** Know which tool does which job (Connect ingests, pipelines transform and check quality, Jobs orchestrate), the expectation actions, AUTO CDC SCD1 vs SCD2, and that repair runs rerun only failed tasks.
