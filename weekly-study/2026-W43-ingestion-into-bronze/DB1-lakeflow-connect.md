# Lakeflow Connect: managed vs standard connectors

**Date:** Mon 2026-10-19

## Exam pill

Auto Loader schema evolution can add new columns or keep unexpected fields in a rescued-data column instead of failing the load.

## Deep dive

**Lakeflow Connect: managed vs standard connectors**

Lakeflow Connect is Databricks' name for its whole ingestion layer, meaning every supported way of getting outside data into Unity Catalog–governed Delta tables. It solves the "who writes and babysits the extract" problem. Managed connectors hand that job to the platform, and standard connectors give you building blocks you wire up yourself.

**How a managed connector works under the hood**

A managed connector is a pipeline that Databricks generates and runs for you, built on Lakeflow Spark Declarative Pipelines. The flow goes like this:
1. You create a **Unity Catalog connection** that stores the source credentials as a governed object, so notebooks never hold raw secrets.
2. You pick the source objects (tables, reports, schemas) and a destination catalog and schema.
3. For **SaaS sources** (Salesforce, Workday, ServiceNow, and others), a serverless **ingestion pipeline** pulls the data incrementally and lands it as streaming tables.
4. For **database sources** like SQL Server, there's an extra piece: an **ingestion gateway** that runs continuously, reads snapshots and change logs from the source, and stages them in a Unity Catalog volume. The ingestion pipeline then applies those changes to the destination tables.

This is the "CDC as a service" piece NJ covers today. The gateway reads the database's change log so you don't have to run a Debezium-style stack.

**Main options**
- **Managed connectors:** these are SaaS and database connectors with incremental loads, schema handling, retries, and monitoring done for you.
- **Standard connectors:** these are Auto Loader, `COPY INTO`, and `read_files` for cloud object storage, plus streaming sources like Kafka, Kinesis, Event Hubs, and Pub/Sub. You write and own the code.
- **History tracking:** managed connectors default to SCD type 1 (they overwrite in place), and you can switch to SCD type 2 when you need history.
- **Full refresh:** you can re-snapshot a table when an incremental load can't absorb a source change.

**Example**

Your team wants Salesforce `Account` and `Opportunity` data in `bronze.crm`. With a managed connector, you create the Salesforce connection, pick those two objects, and set the destination. Databricks then creates and schedules the pipeline. On the first run you'll see a full snapshot land as two streaming tables, and later runs bring only the changed rows. Because it's an ordinary pipeline with an `ingestion_definition` block, you can also create it from the Pipelines API or the CLI, or deploy it in a bundle alongside your jobs.

Compare that with S3 JSON drops from a vendor. There's no app to connect to, just files arriving in a bucket. That's a standard-connector job, so you'd use Auto Loader with a checkpoint (tomorrow's topic).

**Tradeoffs and gotchas**
- **Coverage:** each managed connector supports specific objects and data types, and some changes still need a full refresh. Always check that connector's limitations page.
- **Source prerequisites:** database CDC needs change tracking or CDC turned on at the source, plus a user with the right permissions. That's often the actual blocker.
- **Compute:** the ingestion pipeline is serverless, but the database gateway runs continuously on classic compute, and that has a cost.
- **Flexibility:** you get less custom logic than with your own JDBC notebook. The tradeoff is that you're no longer the one woken up at 3am.
- **Governance:** grants, ownership, and tags on the destination tables are still your job.

For the exam: when a scenario names an enterprise SaaS app or database with a supported connector and asks for the least operational overhead, the answer is a Lakeflow Connect managed connector. When the scenario says files land in cloud storage, think Auto Loader or `COPY INTO`. Both paths land data in Unity Catalog.
