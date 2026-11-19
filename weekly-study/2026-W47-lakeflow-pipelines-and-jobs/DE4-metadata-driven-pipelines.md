# DE4: Metadata-driven pipelines (config-driven frameworks, and when they're over-engineering)

- **Date:** Thu 2026-11-19
- **Week:** 2026-W47
- **Theme:** Lakeflow pipelines and Jobs in depth
- **Paired Databricks pill (Gerald):** DB4: Lakeflow Jobs deeper: parameters, task values, for-each and if/else, pipeline task

---

Good morning, Mario. Gerald covers the **Jobs features** today (parameters, for-each, task values). This pill is the **design question** behind them: "You have 300 source tables. Do you write 300 pipelines?" That's a classic US senior prompt, and you've lived it with scrapers in your code factory.

## 1. The idea in one line

Write the pipeline logic **once**, and describe each source in **config** (a YAML file or a control table). The framework loops over the config. Adding table #301 is a config change and a PR, not new code.

Typical config per source: `source_path / connection`, `format`, `target table`, `keys`, `load_type` (full / incremental / cdc), `watermark_column`, `sequence_by`, `scd_type`, `quality rules`, `schedule/tier`, `owner`.

## 2. Two ways to do it on Databricks

| Pattern | How | Best for |
|---|---|---|
| **Pipeline metaprogramming** | A Python loop inside a Lakeflow Declarative Pipeline creates one streaming table / AUTO CDC flow per config entry | Many similar tables, one dependency graph, built-in lineage and expectations |
| **Job-level fan-out** | A Lakeflow Job reads the config, passes the list via a task value, and a **For each** task runs one parameterized notebook per entry | Heterogeneous sources (APIs, JDBC), per-table retries, concurrency control |

Config lives in **Git** (YAML in the bundle, reviewed by PR) or a **Unity Catalog control table** (editable, queryable, but needs its own change control). Senior answer: Git for definitions, tables for runtime state (watermarks, last run).

## 3. Hands-on: one loop, N bronze-to-silver CDC tables

```python
# sources.yml (in the bundle)
# - name: customers
#   path: /Volumes/raw/crm/customers
#   keys: [customer_id]
#   sequence_by: lsn
#   scd_type: 2
# - name: orders
#   path: /Volumes/raw/erp/orders
#   keys: [order_id]
#   sequence_by: updated_at
#   scd_type: 1

import yaml
from pyspark import pipelines as dp
from pyspark.sql.functions import col

with open("/Workspace/.../sources.yml") as f:
    SOURCES = yaml.safe_load(f)

def build(src):  # function = closure fixes the loop-variable bug
    @dp.table(name=f"bronze_{src['name']}")
    def bronze():
        return (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "json")
                .load(src["path"]))

    dp.create_streaming_table(f"silver_{src['name']}")
    dp.create_auto_cdc_flow(
        target=f"silver_{src['name']}",
        source=f"bronze_{src['name']}",
        keys=src["keys"],
        sequence_by=col(src["sequence_by"]),
        stored_as_scd_type=src["scd_type"],
    )

for src in SOURCES:
    build(src)
```

Gotcha interviewers like: defining the decorated function directly in the `for` loop can make every table capture the **last** loop value. Wrapping it in `build(src)` avoids it.

Job version (sketch): task `load_config` sets `dbutils.jobs.taskValues.set("sources", [...])`, then a **For each** task iterates over `{{tasks.load_config.values.sources}}` and runs `ingest_one` with `{{input}}` as a parameter, with a concurrency limit.

## 4. When it's over-engineering

Build a framework when you have **many sources that share a shape** and a team that keeps adding them. Don't when:
- You have 8 tables, each with unique logic: config just hides `if source == "x"` branches.
- The config grows into a programming language (conditions, custom SQL in YAML). That's the "inner-platform effect".
- Nobody owns the framework: debugging becomes "read the engine to understand one table".

Middle ground: a **thin** framework for the 80% (ingestion, bronze, standard CDC) and plain hand-written code for the 20% (complex Gold business logic). Make "escape hatches" explicit.

## 5. Tech gap: how others do it

- **dbt**: metadata-driven by design. Models are SQL plus YAML (`sources.yml`, tests, configs); packages and Jinja macros generate repetitive models. Similar spirit to a pipeline loop, but SQL-first and transformation-only.
- **Azure Data Factory / Airflow**: classic "control table + ForEach / dynamic task mapping" pattern, the same shape as Jobs For each. Airflow's `.expand()` is the twin of For each.
- **dlt-meta / Databricks Labs**: an open-source metadata framework on top of Declarative Pipelines, worth knowing by name.

## 6. Interview question

**"We ingest 250 tables from 6 source systems and every new table takes a week. How would you fix that?"**

Strong answer: "First I'd group the tables by shape. Most likely 80% are standard: files or JDBC, with a key and an `updated_at` or a CDC log. For those I'd build a thin config-driven ingestion layer. Each source is a YAML entry in Git, reviewed by PR, and one Declarative Pipeline per source system loops over the entries to build bronze and AUTO CDC silver tables, with expectations from the config. Runtime state like watermarks lives in a Unity Catalog table, not in the YAML. Heterogeneous API sources go through a Lakeflow Job with a For each task, so each table retries on its own and I can cap concurrency. I'd keep Gold hand-written, because that's business logic, and I'd add config validation in CI (schema check on the YAML, plus a dry run with `bundle validate`). Success metric: a new standard table goes from a week to a one-file PR. I'd also be honest about the cost: the framework needs an owner, docs, and good error messages that name the failing source, or it becomes a black box."

Tech-lead angle: mention you did this with scrapers. One engine, many site configs, and the trade-off of "when a site is too weird, it gets custom code".

## 7. English phrasing tip

Say **"config-driven"** or **"metadata-driven"**, and to push back politely: *"I'd be careful not to over-engineer this. With only a dozen tables, plain code is easier to read and debug."* Avoid "parametrizable" (rare); say **"parameterized"** (puh-RAM-uh-tuh-rized).

## Go deeper

- For each task in Lakeflow Jobs: https://docs.databricks.com/aws/en/jobs/for-each
- Task values: https://docs.databricks.com/aws/en/jobs/task-values
- Pipeline parameters (Lakeflow Declarative Pipelines): https://docs.databricks.com/aws/en/ldp/parameters
