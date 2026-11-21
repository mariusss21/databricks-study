# Lakeflow Connect: managed connectors

Date: 2026-11-21 (Sat, W47)

## Exam pill

Bundle variables and per-target overrides change catalog, warehouse, or schedule by environment without forking the codebase.

## Deep dive

**Lakeflow Connect: managed ingestion connectors**

Lakeflow Connect is Databricks' set of managed connectors that ingest data from SaaS apps (such as Salesforce) and databases (such as SQL Server) into Delta tables in Unity Catalog, without you writing or running the extraction code. It solves the "build and babysit a custom ingestion job" problem: auth, pagination, schema drift, retries and incremental state are handled for you.

**How it works**
- You create a **connection** (a Unity Catalog securable holding credentials), then an **ingestion pipeline** that references it and lists the source objects (tables or reports) to copy.
- The ingestion pipeline is a Lakeflow pipeline under the hood: it lands the source data into **streaming tables** in a destination catalog and schema you pick. Database connectors typically also use a **gateway** that reads the source's change data and stages it, so the ingestion pipeline can apply it.
- First run does an initial full snapshot of each selected object. Later runs are incremental: only changes are read and applied, so cost tracks change volume, not table size.
- You run it like any pipeline: manually, on a schedule, or as a pipeline task inside a Lakeflow Job.

**Options worth knowing**
- **Selection:** pick specific objects, or a whole schema, and optionally choose columns to include or exclude.
- **History tracking:** keep SCD Type 1 (overwrite) or SCD Type 2 (history rows) for the destination tables.
- **Schema evolution:** new source columns generally flow through, while incompatible changes can fail or require a full refresh of that table.
- **Full refresh:** reloads a table from scratch. It is the repair tool, and also the expensive one.
- **Authoring:** UI, Declarative Automation Bundles, REST API or the CLI, so the pipeline can live in Git.

**Example scenario**
You ingest a Salesforce `Opportunity` object into `main.bronze.opportunity`. Day 1: full snapshot. Day 2: you edit a record in Salesforce, the next scheduled update pulls only that change, and the streaming table reflects it. Check results in the pipeline's update page and its event log, then build Silver as a normal materialized view or pipeline reading from that bronze table.

**Governing and monitoring**
- Destination tables are Unity Catalog tables, so use GRANTs on the catalog and schema, and lineage works as usual.
- The connection is its own securable: control who can create pipelines with it.
- Monitor through the pipeline event log, run history, and Job notifications if you wrap it in a Job.

**Tradeoffs and gotchas**
- Managed means less control: you can't inject custom extraction logic, and unsupported objects or sources need another approach (Auto Loader, custom code, partner tools).
- Source-side prerequisites matter (permissions, change tracking or CDC enabled on databases) and are the usual cause of a failed first run.
- Source deletes and renames need checking against your history setting.
- Don't edit the destination tables by hand; the pipeline owns them.

**For the exam:** know that Lakeflow Connect is the managed, low-code way to ingest from SaaS and databases into Unity Catalog streaming tables, that it is incremental after an initial snapshot, and when to choose it over Auto Loader (files in cloud storage) or custom code.
