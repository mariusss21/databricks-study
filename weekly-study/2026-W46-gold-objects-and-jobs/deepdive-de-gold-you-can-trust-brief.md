# Friday DE deep dive brief — 2026-11-13

**For:** Zé Carlos, building Mario's Friday Data Engineering deep dive (45–90 min).
**Week:** 2026-W46, shared theme "Gold objects and Lakeflow Jobs" (Gerald leads the Databricks side).
**Repo folder:** `weekly-study/2026-W46-gold-objects-and-jobs/` in [mariusss21/databricks-study](https://github.com/mariusss21/databricks-study)
**Deliverable:** a single-file, self-paced interactive HTML lesson (no LMS, opens in a browser). Mario prefers active formats: decisions to make, checks to answer, small code to run. Save it as `deepdive-de-gold-you-can-trust.html` in that week folder and link it from the week README (Fri 2026-11-13 row, DE column).

## Working title
**"A Gold table you can trust: idempotent, fast, and watched, from model to Lakeflow Job"**

## Topics chosen and why they fit together
This week's DE pills (Mon–Thu):
1. **Medallion boundaries, data contracts, idempotent Gold** (DE2): what each layer promises, business logic in Gold, contracts via schema enforcement / expectations / CHECK / informational PK-FK, drop at Silver but fail at Gold, `INSERT INTO ... REPLACE WHERE` with a job `run_date` parameter, MERGE on business keys, materialized views.
2. **Data skew and file layout** (DE1): skew in the Spark UI, AQE skew join, broadcast, salting, Hive partitioning vs liquid clustering (`CLUSTER BY`, `CLUSTER BY AUTO`), clustering Gold facts + OPTIMIZE in the Job.
3. **Data observability and SLAs** (DE4): tests vs observability, five pillars (freshness, volume, schema, distribution, lineage), SLI/SLO/SLA, UC data quality monitoring, `system.access.table_lineage` for blast radius, Jobs health rules and SQL alerts.
4. **dbt as the sidebar** (DE3): the same Gold built as dbt models (ref, incremental merge / replace_where / microbatch, tests), and a Lakeflow Job dbt task.

One story: a Gold table is a product with a promise. Correctness (idempotent re-runs + contracts), speed (layout that survives skew and growth), and trust over time (observability + SLAs) are three halves of the same promise, and the Lakeflow Job is where they're wired together. Senior candidates are judged on whether they see it as one design.

Not repeating: earlier Fridays covered reliable Bronze ingestion (W43) and Silver quality gates / grain / SCD2 (W44). Start from a clean Silver and don't re-teach SCD2 or AUTO CDC. Next week (W47) covers CDC end to end, streaming vs batch, and testing pipelines as code, so keep testing to a light touch here.

## Through-line: one end-to-end scenario
A retail company has `silver.orders`, `silver.order_items`, `silver.dim_customer` (SCD2) and `silver.dim_product`. Finance needs `gold.fact_daily_revenue` (grain: date × store × product) and `gold.revenue_by_segment`, ready by **7:00 AM ET**, every day.

Walk Mario through:
1. **Declare the contract:** grain, keys, NOT NULL / CHECK constraints, informational PK (`RELY`), and the SLA ("fresh by 7 AM, complete within ±2% of the 7-day average, no duplicate grain rows"). Exercise: which expectation is `expect_or_drop` in Silver and which is fail-the-job in Gold, and why.
2. **Build it idempotently:** daily load with `INSERT INTO gold.fact_daily_revenue REPLACE WHERE sale_date = :run_date SELECT ...`. Show the "re-run doubled revenue" bug from a plain append, then the fix. Contrast with MERGE on business keys (late corrections) and a materialized view (let the engine handle incremental refresh). Decision exercise: pick one of the three for each Gold table.
3. **Make it fast:** Black Friday hits, one store has 40% of rows, and the job goes from 20 min to 3 h. Spot the skew (Spark UI task-time distribution), try AQE skew join / broadcast of the dims, then the layout fix: liquid clustering on `(sale_date, store_id)` instead of partitioning by store, plus OPTIMIZE as a Job task. Interactive moment: "partition or cluster?" with cardinality numbers.
4. **Orchestrate it:** a Lakeflow Job with tasks `silver_refresh → build_gold (run_date param) → quality_gate → optimize → publish`, retries, and a run-duration health rule tied to the SLA. Note `run_date` comes from job parameters so backfills are just re-runs with another date.
5. **Watch it:** UC data quality monitoring for freshness and completeness on the Gold tables, a SQL alert on row-count deviation, and when something breaks, `system.access.table_lineage` to find which dashboards are hit (blast radius). Interactive: a mock incident ("finance found half the revenue before 7 AM") where Mario picks the order of checks.
6. **Sidebar: the same thing in dbt:** `fct_daily_revenue` as an incremental model (`incremental_strategy='replace_where'` or `merge` with `unique_key`), `unique`/`not_null` tests and model contracts, run as a dbt task inside the same Lakeflow Job.

End with a one-screen connection map: contract → idempotent build → layout → Job → monitors/alerts → lineage.

## Tie-back to this week's Databricks theme (Gold objects and Lakeflow Jobs)
Gerald's DB pills this week: DB1 Spark tuning knobs, DB2 re-runnable Bronze → Silver → Gold, DB3 Gold layer objects, DB4 data quality in Silver and Gold (expectations, quarantine). Link to them:
- **Contracts / quality (DB4):** LDP expectations, Delta NOT NULL / CHECK constraints, UC informational PK/FK.
- **Idempotent Gold (DB2, DB3):** `INSERT INTO ... REPLACE WHERE`, `MERGE INTO`, materialized views vs tables vs views as Gold objects.
- **Performance (DB1):** AQE, broadcast joins, liquid clustering (`CLUSTER BY`, `CLUSTER BY AUTO`), OPTIMIZE.
- **Orchestration and monitoring:** Lakeflow Jobs tasks, job parameters, retries, health rules / notifications; UC data quality monitoring; lineage system tables.

## Tool gaps: Snowflake and dbt equivalents (short sidebar)
- **dbt:** incremental models (merge, replace_where/insert_overwrite, microbatch with lookback) = the idempotent Gold patterns; tests + model contracts = expectations/constraints; `dbt source freshness` = a freshness SLI; dbt-databricks can also materialize `materialized_view`. dbt runs as a task in a Lakeflow Job.
- **Snowflake:** clustering keys and automatic clustering are the cousin of liquid clustering (no Hive-style partitions; micro-partitions + pruning). Dynamic Tables with a target lag are close to materialized views with a freshness target. Tasks (DAGs) cover simple orchestration; Snowflake alerts and Data Metric Functions cover monitoring. Keep this to one comparison table.

## Go deeper (all verified to load on 2026-11-12)
- [Selectively overwrite data with Delta Lake (REPLACE WHERE)](https://docs.databricks.com/aws/en/delta/selective-overwrite)
- [Use liquid clustering for tables](https://docs.databricks.com/aws/en/tables/clustering)
- [Unity Catalog data quality monitoring](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-quality-monitoring)
- [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/)
- [Lineage system tables](https://docs.databricks.com/aws/en/admin/system-tables/lineage)
- [dbt incremental models](https://docs.getdbt.com/docs/build/incremental-models)

## How this shows up in US senior DE interviews
**Q1 (system design):** "Design the daily revenue Gold layer for finance with a 7 AM SLA. How do you make sure it's correct, on time, and that you know before finance does when it isn't?"
A strong senior answer: states the grain and the contract first; idempotent, partition-scoped writes (REPLACE WHERE by date, MERGE for late corrections) so re-runs and backfills are safe; layout chosen from query patterns (clustering on date + high-cardinality keys, not over-partitioning); a Job DAG with retries, a blocking quality gate before publish, and duration health rules; SLIs for freshness, volume, and duplicates with alerts set before the SLA deadline; lineage for blast radius; and a runbook / on-call owner. Names trade-offs: MV vs table, fail vs publish-with-warning, cost of OPTIMIZE vs query speed.

**Q2 (trade-off / debugging):** "The nightly Gold job went from 20 minutes to 3 hours and missed the SLA. Walk me through it."
A strong answer: first protect the consumer (communicate, decide whether to publish stale or partial with a flag), then diagnose with data (Spark UI stages, skewed tasks, input size growth, small files), fix in layers (AQE / broadcast → salting → layout change to liquid clustering), and prevent recurrence (duration health rule, volume monitor, post-mortem).

**Behavioral angle (tech lead):** "Tell me about a time a data product missed its SLA." Frame with STAR: impact on the business, the fix, and the standard he set for the team (SLA per Gold table, idempotent writes as a code-review rule, alert before deadline).

**English phrasing tip:** "We treat every Gold table as a product with a contract and an SLA" and "re-runs are safe by design" are good lines to have ready.

## Notes for building
- Keep the lesson in English (Mario works in English with this bot; he dictates with Wispr Flow).
- Keep code small and runnable in a Databricks notebook (SQL + PySpark), with a tiny synthetic dataset embedded.
- End with a 5–8 question self-check and the connection map.
- Aim for about 60 minutes; Mario is just back from a house-move pause and catching up on a work backlog.
