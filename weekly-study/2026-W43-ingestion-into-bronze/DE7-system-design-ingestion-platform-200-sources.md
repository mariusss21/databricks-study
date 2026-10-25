# System design: an ingestion platform for 200+ sources

**Date:** Sun 2026-10-25 · **Week:** 2026-W43 · Ingestion into Bronze · **Track:** Data Engineering (NJ in Dollar)
**Pairs with Gerald's Databricks recap:** an end-to-end Bronze pipeline that's safe to re-run

## The prompt you'll get

"We have 200+ sources: Postgres and MySQL databases, Salesforce and other SaaS apps, vendor files dropped in S3, Kafka topics, and a long tail of REST APIs. Design the ingestion platform."

This is the week's capstone. Every pill from Monday to Saturday is one box in this diagram. The senior signal is that you **don't design 200 pipelines**. You design about five patterns and a metadata layer that turns a new source into a config change.

## Step 1: clarify before drawing (2-3 minutes)

Ask, out loud:

- **Freshness:** which sources need minutes, which are fine daily?
- **Volume and shape:** biggest tables, events per second, how many sources are tiny?
- **Change capture:** do we get DB logs (CDC), or only `updated_at` columns? Do we need deletes?
- **Team:** how many engineers own this? (Drives build vs buy.)
- **Compliance:** PII, retention, who can see raw data?

Then state assumptions: "I'll assume 80% of sources are small and daily, ~20 databases need near-real-time CDC, and a team of 4."

## Step 2: group sources into archetypes

| Archetype | Example | Pattern | Databricks tool |
|---|---|---|---|
| SaaS apps | Salesforce, Workday | Buy a managed connector | Lakeflow Connect (or Fivetran) |
| Operational DBs | Postgres, SQL Server | Log-based CDC, append change events | Lakeflow Connect managed CDC, or Debezium to Kafka |
| Files | Vendor CSV/JSON in S3 | Incremental file discovery | Auto Loader |
| Events | Kafka topics | Streaming read | Structured Streaming / Lakeflow Declarative Pipelines |
| Long-tail APIs | Small REST vendors | One generic, config-driven extractor | Lakeflow Jobs with For each, or a PySpark custom data source |

## Step 3: the metadata-driven core

One control table (or YAML files in Git) describes each source: name, archetype, connection secret scope, landing path, format, schedule tier, owner, freshness SLA, PII flag. Generic code reads the config and builds the pipelines. Onboarding source #201 is a pull request that adds one YAML entry, reviewed and deployed with Declarative Automation Bundles.

Every Bronze table gets the same contract from this week: append-only, `_ingested_at`, source file or offset, `_rescued_data`, tolerant schema (Wed), checkpointed exactly-once delivery (Tue), dedup keys recorded for Silver (Sat).

## Step 4: run, isolate, observe

- **Schedule tiers**, not 200 schedules: streaming, hourly, daily. Group small sources into shared pipelines so you don't pay cluster startup 200 times.
- **Blast radius:** one bad source must not block the others. Separate flows per source, retries per task, and a quarantine for poison records.
- **Governance:** Unity Catalog with one schema per source (`bronze.salesforce`, `bronze.billing_pg`), service principals per pipeline, PII tags.
- **Observability:** a freshness check per table against its SLA, row-count anomaly alerts, and the pipeline event log plus system tables for cost per source.

## Hands-on: one pipeline, many Bronze tables

A Lakeflow Declarative Pipeline in Python can generate tables in a loop from config:

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCES = [  # in real life, read from a YAML file or a control table
    {"name": "vendor_orders",  "path": "/Volumes/raw/vendors/orders/",  "format": "json"},
    {"name": "vendor_returns", "path": "/Volumes/raw/vendors/returns/", "format": "csv"},
]

def make_bronze(src):
    @dp.table(name=f"bronze_{src['name']}", comment=f"Raw {src['name']}, append-only")
    def _bronze():
        return (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", src["format"])
                .option("cloudFiles.schemaEvolutionMode", "rescue")
                .load(src["path"])
                .withColumn("_ingested_at", F.current_timestamp())
                .withColumn("_source_file", F.col("_metadata.file_path")))

for src in SOURCES:
    make_bronze(src)  # the function wrapper avoids the late-binding loop bug
```

Adding a source means adding a dict. That's the whole idea in 20 lines.

## Interview question

**"How would you design ingestion for 200+ heterogeneous sources with a small team?"**

Strong answer (about 2 minutes):

"First I'd clarify freshness, volume, and whether we get database logs. Then I'd refuse to build 200 bespoke pipelines. I'd group sources into about five archetypes: SaaS, operational databases, files, event streams, and long-tail APIs, and buy where it's commodity. On Databricks that's Lakeflow Connect for SaaS and database CDC, Auto Loader for files, and streaming reads for Kafka. Only the long tail gets custom code, and that code is one config-driven extractor, not one script per vendor.

Everything is metadata-driven: a source is a YAML entry with owner, SLA, schedule tier, and PII flag, deployed through bundles in CI. Every Bronze table follows one contract: append-only, ingestion metadata, rescued data for drift, and checkpointed delivery so any run is safe to replay. Dedup and modeling happen in Silver.

Operationally I'd run tiers instead of individual schedules, isolate failures per source so one vendor can't block the rest, govern with Unity Catalog schemas per source, and alert on freshness against each source's SLA. The trade-off I'd call out is cost versus control: managed connectors cost more per row but save engineer time, which with a team of four is the scarcer resource. At my current company as tech lead I made that same call..." (then add one real example from Stack Tecnologias).

## English phrasing tip

Signpost your structure so the interviewer can follow: "I'll start with requirements, then the source archetypes, then the shared core, then operations." And when you choose: "I'd lean toward X here, **because**... The trade-off is..." Avoid "it depends" on its own; say "it depends on freshness. If it's under five minutes, I'd..., otherwise...".

## Go deeper

- [Lakeflow Connect overview](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/)
- [Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/)
- [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)
