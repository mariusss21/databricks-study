# Monitoring and troubleshooting Lakeflow Jobs

Date: 2026-11-20 (Fri, W47)

## Exam pill

Declarative Automation Bundles, formerly Databricks Asset Bundles, package Lakeflow Jobs, declarative pipelines, and other assets so the same code promotes across dev, test, and prod.

## Deep dive

**Monitoring and troubleshooting Lakeflow Jobs**

A job that fails loudly is easy. The hard cases are jobs that flake, drift slower, or fail halfway. Jobs gives you run history, retries, repair runs, and notifications so you can find the problem and fix it without rerunning everything.

**How it works**

Every run records per-task status, duration, attempt count, and the parameters used. The matrix view of run history lets you compare a run against earlier ones, and a run that quietly takes twice as long as usual is often the first sign of data skew, a bigger input, or a changed plan. When a task fails, the run stops downstream tasks that depend on it. Tasks that already succeeded keep their results.

**Main tools**

- **Retries:** set per task, for transient problems such as cloud or API blips. They help only if the task is safe to run twice.
- **Timeouts:** a per-task or per-job timeout stops a wedged run from burning compute.
- **Repair run:** after fixing the cause, rerun only the failed tasks and their dependents. Successful tasks are not recomputed.
- **Notifications:** configure per job (email, Slack, webhook and so on) for start, success, failure, and duration warnings. Send them to the owning team, not a personal inbox.
- **System tables:** the `system.lakeflow` schema holds job, task and run-timeline history for querying across the workspace. Check the exact table names in the docs for your account.

**Example**

A nightly job has tasks `ingest` → `transform` → `publish`. `transform` fails on a bad schema in one file. You fix the source, then click Repair run (or use `databricks jobs repair-run` in the CLI). Only `transform` and `publish` run again. `ingest` keeps its earlier output, which saves compute and avoids duplicate loads.

**Gotchas**

- Retries on a non-idempotent write duplicate data. Use MERGE, idempotent COPY INTO, or streaming checkpoints so a retry is safe.
- Alert on repeated failure and on abnormal duration, not only on the final failure.
- A repair run reuses the original run's parameters unless you override them. Check them before you repair.
- Repairing after changing the code only helps if the task actually picks up the new code, so check which version your job references.

**For the exam:** know when to use retries versus a repair run, why idempotency matters under retry, and which Jobs features (run history, notifications, timeouts) you'd use to detect and recover from failures.
