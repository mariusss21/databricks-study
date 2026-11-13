# DE5: Orchestration in interviews: Airflow vs Lakeflow Jobs (DAGs, retries, backfills, data-aware triggers)

- **Date:** Fri 2026-11-13
- **Week:** 2026-W46, Gold objects and Lakeflow Jobs
- **Paired Databricks pill:** Gerald's DB5 (same row in the week README; not posted yet when this was written)
- **Same day:** Zé Carlos's deep dive `deepdive-de-gold-you-can-trust.html` (today's pill is the orchestration layer of that scenario)

---

## Why this shows up in interviews

"Airflow" is one of the most requested tools in US Senior DE postings. The interviewer rarely wants operator trivia. They want to know whether you understand what an orchestrator is *for*, and whether your pipelines stay correct when they re-run, run late, or run for last month.

You already run Airflow on Kubernetes for the code factory, so you have real stories here. Use them.

## The concept in one breath

An orchestrator decides **when** work runs, **in what order**, and **what happens when it fails**. It should *not* hold business logic, move data itself, or be the only place that knows which date a run covers.

Five things every senior answer touches:

1. **DAG and dependencies:** tasks plus edges, with conditional paths (on failure, all done).
2. **Triggers:** cron time, or *data-aware* (start when the upstream data actually landed).
3. **Retries and timeouts:** retry transient failures; make sure a retry cannot double-count (idempotent tasks, see DE2).
4. **Parameterized runs and backfills:** every run gets a logical date and processes only that slice, so "rerun Aug 9-10" is just 2 more runs.
5. **Observability:** run history, SLA/duration alerts, repair only the failed tasks.

## Airflow vs Lakeflow Jobs, side by side

| Concern | Apache Airflow (3.x) | Lakeflow Jobs (Databricks) |
|---|---|---|
| Define the DAG | Python `@dag` / `>>` dependencies | UI, Python SDK, or Declarative Automation Bundles (YAML) |
| Conditional flow | trigger rules (`all_done`, `one_failed`...), branching operators | **Run if** (All succeeded, None failed, All done, At least one failed...), **If/else condition** task, **For each** task, **Run Job** task |
| Data-aware triggers | assets (formerly datasets) | **Table update** and **File arrival** triggers on Unity Catalog objects, job completion (Beta) |
| Run date | `logical_date` / `{{ ds }}` | job parameters with dynamic values like `{{job.start_time.iso_date}}` |
| Backfill | backfill a date range of DAG runs | **Run backfill** from the Run now menu, uses `{{backfill.iso_datetime}}` |
| Partial re-run | clear failed task instances | **Repair run** (re-run only failed and skipped tasks) |
| Where compute runs | Airflow workers call out to Spark, dbt, APIs | serverless or job compute right next to Unity Catalog data and lineage |
| Best at | many systems outside one platform | everything inside the lakehouse |

**The honest senior take:** many US companies run *both*. Airflow (often managed: MWAA, Astronomer, Cloud Composer) is the cross-system conductor. It triggers a Databricks job with `DatabricksRunNowOperator`, and the job owns the Spark/SQL/pipeline tasks inside the lakehouse. If everything already lives in Databricks, Lakeflow Jobs removes a whole platform to operate.

## Hands-on: a re-runnable daily Gold job

The Gold step reads the run date from a job parameter, so a normal run, a retry, and a backfill all do the same thing (see DE2 for `REPLACE WHERE`):

```sql
-- Task "gold_daily_revenue" (SQL task). Job parameter: run_date = {{job.start_time.iso_date}}
INSERT INTO retail.gold.daily_revenue
REPLACE WHERE order_date = :run_date
SELECT order_date, store_id, SUM(net_amount) AS revenue, COUNT(*) AS orders
FROM retail.silver.orders
WHERE order_date = :run_date
GROUP BY order_date, store_id;
```

Job shape (Declarative Automation Bundle, trimmed):

```yaml
resources:
  jobs:
    daily_revenue:
      parameters:
        - name: run_date
          default: "{{job.start_time.iso_date}}"
      trigger:
        table_update:
          table_names: [retail.silver.orders]   # start when Silver actually changed
      tasks:
        - task_key: gold_daily_revenue
          sql_task: { file: { path: ../sql/gold_daily_revenue.sql }, warehouse_id: ${var.warehouse_id} }
          max_retries: 2
        - task_key: notify_on_failure
          depends_on: [{ task_key: gold_daily_revenue }]
          run_if: AT_LEAST_ONE_FAILED
          notebook_task: { notebook_path: ../notebooks/page_oncall }
```

Lost Aug 9-10? Use **Run backfill**, set the range, set `run_date` to the backfill date reference, and you get 2 idempotent runs. Nothing new to write.

The same idea in Airflow, for comparison:

```python
from airflow.sdk import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
import pendulum

@dag(schedule="@daily", start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False)
def daily_revenue():
    DatabricksRunNowOperator(
        task_id="run_gold_job",
        job_id=123,                       # the Lakeflow Job above
        job_parameters={"run_date": "{{ ds }}"},
        retries=2,
    )

daily_revenue()
```

## Likely interview question

> "Your daily pipeline failed at 3 AM on two days last week and nobody noticed until Monday. Walk me through how you'd orchestrate it so this doesn't happen again, and how you recover those two days."

**Strong senior answer (shape):**

1. **Recover first, safely.** "Because every run is parameterized by a run date and the Gold write is idempotent (`REPLACE WHERE` on that date), I backfill just those two dates. Re-running can't double-count. If the tasks weren't idempotent, fixing that comes before any backfill."
2. **Trigger on data, not just the clock.** "I'd switch from a fixed 3 AM cron to a table-update trigger on the Silver table, with a scheduled trigger as a backstop, so we run when the data is ready and still run if the upstream signal never comes."
3. **Retry what's transient, fail loudly on the rest.** "Retries with a cap for cluster or network errors, a timeout, and a failure branch (`Run if: at least one failed`) that pages on-call. Duration and freshness alerts tied to the 7 AM SLA, so a *late* run is an alert too, not only a failed one."
4. **Make recovery cheap.** "Repair run re-executes only the failed tasks. Backfills reuse the same job, so there is no special backfill script that drifts from production."
5. **Tool choice.** "If the whole flow lives in Databricks, I'd keep it in Lakeflow Jobs, deployed with bundles through CI. If it spans other systems, Airflow conducts and calls the Databricks job. Either way, the business logic stays in the tasks, not in the orchestrator."
6. **Tech-lead close.** "Then a short blameless postmortem: why nobody noticed (no alerting on lateness) and an owner for every job."

## English phrasing tip

- Say "**The orchestrator decides *when* and *in what order*; the tasks decide *what*.**" That one line signals separation of concerns.
- "Backfill" is a verb and a noun: "We **backfilled** two days" / "I ran a **backfill** for Aug 9-10."
- Avoid "the job fell" (from Portuguese *caiu*). Say "the job **failed**" or "the job **went down**".

## Go deeper (official docs)

- Lakeflow Jobs, schedules and triggers: https://docs.databricks.com/aws/en/jobs/triggers
- Task dependencies and Run if conditions: https://docs.databricks.com/aws/en/jobs/run-if
- Backfill jobs: https://docs.databricks.com/aws/en/jobs/backfill-jobs
- Airflow Dags (core concepts): https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
