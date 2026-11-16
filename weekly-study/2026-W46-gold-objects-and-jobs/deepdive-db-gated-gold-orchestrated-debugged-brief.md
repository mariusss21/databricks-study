# Databricks deep-dive brief for Monday 2026-11-16

From Gerald, for Zé Carlos. This brief synthesizes W46 (Nov 9–15, "Gold objects and Lakeflow Jobs"). Sources: /home/box/agent-data/dea/weekly-plans/2026-11-09.md (daily Databricks topics), the 2026-11-09..15 entries in /home/box/agent-data/dea/knowledge-pills.md (short exam pills) and technical-pills.md, and the W47 plan at weekly-plans/2026-11-16.md. Note: I couldn't confirm a W46 folder in the local copy of mariusss21/databricks-study, so stick to the plan and pill text quoted here. Don't add details that aren't in those files or the official docs.

## Scope notes
- Aim for about 60 minutes. Mario is still catching up after the move, so keep it focused.
- W47 (starting Monday) is a second Lakeflow week, and Monday's pill is Lakeflow Spark Declarative Pipelines (streaming tables vs materialized views, flows, triggered vs continuous). The lesson should close W46 and end with a short bridge into that pill. Don't teach pipelines in depth. That's the W47 job.
- Exam date is officially 2026-12-18. He's leaning toward January 2027, but that isn't confirmed.

## Topics (3)
1. **Gold objects and quality gates (Wed/Thu topics).** Table vs view vs materialized view vs streaming table in Unity Catalog, plus grants. Then data quality in Silver/Gold: constraints, pipeline expectations with warn/drop/fail, and quarantine tables. Supporting technical pills: declare Gold's grain and check it against source counts (11-14), and use `try_cast` plus counting the resulting nulls as a quality metric (11-10).
2. **Orchestrating it with Lakeflow Jobs (Fri/Sat topics).** Task DAG, dependencies, run-if, retries, task values, for-each and if/else. Triggers: schedule vs file arrival vs table update. Repair runs. Short pills: run history as a baseline, not a single green check (11-09), and the Jobs UI view of status, task DAG, run time and failure rate (11-10).
3. **Troubleshooting a slow or failing run (the week's exam pills).** Skew in the Spark UI (one long task, max shuffle read far above the median) (11-11). Sample question 1 in the current exam guide (August 2026, groupBy on `customer_id`) has answer D, salting the key. AQE skew handling only splits skewed joins, not aggregations, and adding executors leaves the one huge task alone. Note: the 11-12 short pill draft said the answer was AQE, which was wrong and was corrected before Mario saw it, so don't repeat it. Spill shows up in the stage metrics, not in the job total (11-13). Then table maintenance: liquid clustering `CLUSTER BY` (11-14) and predictive optimization for UC managed tables (OPTIMIZE, VACUUM, stats) instead of a hand-scheduled maintenance job (11-15). Technical pill 11-15: check shuffle, spill and skew before adding executors, and keep a tuning log.

## How they connect
It's one path, which was also the W46 Sunday recap: a Gold object is built behind a quality gate, a Job runs it on the right trigger, and when a run goes red or slow you read run history and then the Spark UI to find the cause. You fix it with a repair run, salting, AQE (for skewed joins) or clustering, not with more hardware. The hands-on can follow a single daily revenue Gold table through those steps.

## Where to go a bit deeper
- When to pick a materialized view vs a streaming table for Gold. This is the bridge to Monday's Declarative Pipelines pill.
- What expectations with warn/drop/fail each do to the bad rows, compared with a quarantine table.
- Repair run vs full rerun, and how task values and run-if change which tasks run.
- Reading skew and spill in the Spark UI, and why predictive optimization can replace a maintenance job.

## Why it matters for Mario
These topics line up with the exam guide sections on transformation (22%), Lakeflow Jobs (16%) and troubleshooting, and the skew scenario is sample question 1 in the current exam guide. For a senior DE role in the US, "build it gated, orchestrate it, debug it from evidence" is the story interviewers ask about. Include 3–4 interview questions on diagnosing a slow job and choosing a trigger.
