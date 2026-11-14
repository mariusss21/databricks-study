# Job triggers: schedule vs file arrival vs table update, and repair runs

Date: 2026-11-14 (Sat)

## Exam pill

Liquid clustering (CLUSTER BY) keeps rows with similar key values in the same files so later queries can skip files. It is a named exam objective.

## Deep dive

**Job triggers: schedule vs file arrival vs table update, and repair runs**

A Lakeflow Job trigger decides *when* a run starts. The problem it solves is readiness: a cron guess ("the upstream usually finishes by 2 AM") either runs too early on missing data or wastes runs when nothing changed.

**How it works.** You attach one trigger to a job. With a schedule, the platform starts a run at the cron time in the timezone you pick. With a file arrival trigger, Databricks watches a Unity Catalog volume or external location path and starts a run when new files show up, after a minimum wait you can configure. With a table update trigger, it watches the Delta tables you list and starts a run when they receive new commits. The run itself is the same task DAG in all three cases; only the starter differs. Triggers can be paused, and manual "Run now" still works alongside them.

**The options:**
- **Scheduled (time-based):** Quartz-style cron plus timezone. Best when the business clock dominates, like a nightly close or a report due at 7 AM.
- **File arrival:** fits landing-zone drops. The job's identity needs read access to the monitored location, which is a Unity Catalog permission, not a cluster setting.
- **Table update:** fits cascading medallion pipelines. Silver commits, the Gold job starts, and you no longer guess how long upstream takes.
- **Continuous:** keeps one run active at all times and restarts it; it is a different mode, mostly for streaming.

**Repair runs.** When a multi-task run fails, a repair run re-executes only the failed tasks and the tasks downstream of them. Succeeded tasks are not rerun, and their outputs (including task values) stay. You can change parameters for the repair. The run ID stays the same, and the repair is recorded in the run history. Compare with a fresh "Run now", which re-executes the whole DAG.

**Example.** A Gold revenue job has tasks `build_dim`, `build_fact`, `publish`. It has a table update trigger on `main.silver.orders`. A Silver commit starts a run; `build_fact` fails on a bad cast. You fix the code and click Repair run: only `build_fact` and `publish` execute, and `build_dim` is reused.

**Tradeoffs and gotchas:**
- Data-driven triggers start on new commits or files, not on "data is complete". A partial drop can start a run, so use the wait settings and design tasks to be idempotent.
- Table update fires per commit, so frequent small writes can mean many runs unless you set a minimum time between runs.
- File arrival depends on the path being under UC governance; missing permissions mean silent non-triggering, so check the trigger status.
- Repair only helps if tasks are safe to rerun; a non-idempotent task can duplicate data.

**For the exam:** know when to pick schedule vs file arrival vs table update (match the trigger to data readiness), and that a repair run reruns only failed and downstream tasks instead of the whole job.
