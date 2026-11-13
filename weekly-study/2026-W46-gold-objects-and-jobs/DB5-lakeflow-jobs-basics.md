# Lakeflow Jobs basics: task DAGs, dependencies, retries, and control flow

Date: 2026-11-13 (Fri)

## Exam pill

Disk spill on a stage means the task ran out of memory and wrote shuffle data to disk. The stage metrics show it. The job’s total time does not.

## Deep dive

**Lakeflow Jobs basics: task DAGs, dependencies, retries, and control flow**

A Lakeflow Job is Databricks' native orchestrator: a directed acyclic graph of tasks (notebook, Python script or wheel, SQL, dashboard, pipeline, dbt, and others) plus a trigger and compute settings. It solves "run these steps in the right order, react correctly when one fails, and show me what happened" without an external scheduler.

**How it works.** Each task declares `depends_on` for its upstream tasks. The scheduler starts every task whose dependencies are satisfied, so independent branches run in parallel. When a task finishes, its outcome is evaluated against each downstream task's **run-if** condition. If the condition isn't met, the downstream task is skipped (or marked upstream failed) instead of running on bad input. The run page shows the graph with a status per task, and a run is only successful if every task that was meant to run did.

**Main building blocks:**
- **Dependencies:** explicit edges between tasks. Put ordering in the graph, not in sleeps or notebook polling.
- **Run if:** conditions such as all succeeded (the default), at least one succeeded, none failed, all done, at least one failed, or all failed. "All done" or "at least one failed" suits cleanup and alert tasks.
- **Retries:** set per task (retry count, interval, and optionally retry on timeout). Retrying a task re-executes it, so it must be idempotent.
- **Task values:** `dbutils.jobs.taskValues.set(key, value)` in one task and `dbutils.jobs.taskValues.get(taskKey, key)` in a later one. Meant for small values such as a row count or a batch date, not data.
- **If/else condition task:** evaluates an expression (for example a task value or job parameter against a threshold) and routes to the true or false branch.
- **For-each task:** runs a nested task once per input in a list, optionally with a concurrency limit.
- **Job parameters:** values supplied at trigger time that tasks can read, handy for backfills.

**Example.** A daily job with `ingest_bronze` → `build_silver` → (`build_gold`, `data_quality_check`) → `notify`. `ingest_bronze` is set to retry twice. `data_quality_check` sets a task value `bad_rows`. An If/else task checks `bad_rows > 0`: the true branch runs `quarantine_and_alert`, the false branch runs `publish_gold`. If `build_silver` fails after its retries, everything downstream is skipped, and `notify` (run-if "at least one failed") still fires.

**Gotchas and tradeoffs:**
- Retries plus non-idempotent writes (plain `INSERT` appends) duplicate data. Prefer `MERGE`, `INSERT ... REPLACE WHERE`, or overwriting a partition by batch key.
- Task values are small by design. Pass table names or counts, never datasets.
- Putting `if` logic inside notebooks hides branching from the run graph and from repair runs.
- Skipped is not failed: check how the final status is derived before relying on it for alerting.

**For the exam:** know that a job is a task DAG with dependencies, that retries and run-if conditions live on tasks, how task values pass small data downstream, what If/else and For-each tasks are for, and why tasks must be idempotent.
