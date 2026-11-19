# Lakeflow Jobs deeper: parameters, task values, for-each, if/else, pipeline task

Date: 2026-11-19 (Thu, W47)

## Exam pill

Git Folders, formerly Repos, are where you create branches, commit, push, and open pull requests from the workspace UI.

## Deep dive

**Lakeflow Jobs deeper: parameters, task values, for-each, if/else, and pipeline tasks**

A Lakeflow Job is a DAG of tasks. The problem this pill solves: how to make that DAG reusable and dynamic (same job, different inputs) without hardcoding values or splitting it into many near-identical jobs.

**How it works.** When a run starts, the job resolves its parameters, then schedules tasks whose `depends_on` edges are satisfied. Each task gets its own compute and runs independently. Data flows between tasks in two ways: through tables/files (the durable way), and through small values passed in the run context.

**The moving parts**
- **Job parameters:** key/value pairs defined at job level, overridable per run (manual, API, CLI, or trigger). They are pushed down to tasks. In a task you reference them with dynamic value references such as `{{job.parameters.my_key}}`.
- **Task parameters:** set per task (notebook widgets/base parameters, Python args, SQL parameters). Dynamic references also expose run metadata such as `{{job.run_id}}` and `{{task.name}}`.
- **Task values:** a task sets small values with `dbutils.jobs.taskValues.set(key, value)`, and downstream tasks read them with `{{tasks.<task_name>.values.<key>}}` or `dbutils.jobs.taskValues.get(...)`. Meant for small control data (a row count, a file list), not datasets.
- **If/else condition task:** compares two operands (parameters, task values) and routes downstream tasks to the true or false branch.
- **For each task:** loops a nested task over a list of inputs (static JSON or a task value) and can run iterations in parallel up to a configured concurrency.
- **Pipeline task:** triggers a Lakeflow Spark Declarative Pipeline update from the job, so the job orchestrates the pipeline plus the notebooks/SQL around it.

**Example scenario.** A `list_tenants` task builds a list of tenant ids and publishes it as a task value. A for-each task takes that list as its input and, per iteration, runs a notebook with `tenant_id={{input}}`. After it, an if/else task checks `{{tasks.validate.values.bad_rows}} > 0`; the true branch runs a quarantine task, the false branch runs a pipeline task that refreshes the gold tables. In the UI you see one iteration per tenant, and only the chosen branch executes (the other is shown as excluded).

**CLI/bundles angle.** All of this is declarative in a bundle's job YAML; `databricks bundle run <job> --params key=value` overrides job parameters at run time.

**Tradeoffs and gotchas**
- Task values are for small payloads; large data belongs in a Delta table.
- Parameters arrive as strings, so cast them in code.
- Downstream tasks only see task values from tasks they depend on (directly or transitively).
- For-each fans out compute, so set concurrency deliberately to avoid cost spikes or hitting limits.
- Hardcoding environment values in tasks defeats the point; use parameters and bundle variables.

**For the exam:** know job vs task parameters, how task values pass data between tasks, when to use if/else and for-each, and that a pipeline task lets a job orchestrate a declarative pipeline.
