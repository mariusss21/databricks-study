# Recap: Gold object, quality gate and Job trigger as one orchestrated path

Date: 2026-11-15

## Exam pill

Predictive optimization can automatically maintain eligible Unity Catalog managed tables, including OPTIMIZE, VACUUM, and stats, instead of a hand-scheduled maintenance job.

## Deep dive

**Recap: Gold object, quality gate and Job trigger as one orchestrated path**

This week was three separate answers to one question: how does a trustworthy Gold number reach a consumer on time? Today you stitch them into a single path: trigger → pipeline with quality gates → Gold object → grants.

**The path, in order**
1. **Trigger.** A Lakeflow Job starts on a schedule, a file arrival, or a table update (the last two are event-driven and avoid polling or guessing arrival times).
2. **Ingest and transform.** Tasks run as a DAG: Bronze → Silver → Gold, each task depending on the previous via `depends_on`. Run-if conditions decide whether a task runs when upstream tasks fail.
3. **Quality gate.** In a pipeline, expectations validate rows as they flow. In plain tables, `CHECK` and `NOT NULL` constraints reject bad writes.
4. **Gold object.** Materialized view, streaming table, view or table, depending on freshness and cost needs.
5. **Consumption.** `GRANT SELECT` on the Gold object only; consumers never see Bronze or Silver.

**Choices at each step**
- **Expectations:** `EXPECT` alone warns and keeps the row (metrics only); `ON VIOLATION DROP ROW` drops it; `ON VIOLATION FAIL UPDATE` stops the update. Pick by how bad a bad row is.
- **Quarantine table:** route failing rows to a separate table instead of losing them silently, so you can inspect and replay.
- **Retries and repair runs:** a transient failure gets task retries; a real failure gets a repair run, which re-executes only the failed tasks and their dependents, not the whole job.
- **Gold as a materialized view:** it refreshes incrementally where possible and is a natural fit for a once-daily aggregate.

**Example scenario**
A daily revenue Gold table. A file lands in a volume → file-arrival trigger fires the job → Silver task cleans orders → the Silver expectation `amount > 0` drops negatives (counted in the pipeline event log, failing rows also written to a quarantine table) → the Gold MV recomputes revenue by day → analysts have `SELECT` on Gold only. If the Gold task fails because of a warehouse hiccup, you hit **Repair run**: Bronze and Silver are not recomputed, only Gold and anything downstream.

**Tradeoffs and gotchas**
- A `FAIL UPDATE` gate protects consumers but can block the SLA; `DROP ROW` keeps the pipeline moving but can hide data loss unless you monitor drop counts.
- Event triggers reduce latency but can fire more often than you expected; know what a burst of files does.
- Repair runs only help when tasks are idempotent. If a task appends non-idempotently, a repair duplicates data.
- Grants on Gold are not enough without USE CATALOG and USE SCHEMA on the parents.

**For the exam:** expect scenario questions that combine these: pick the right trigger, the right expectation action (warn/drop/fail), the right Gold object type, and what a repair run re-executes.
