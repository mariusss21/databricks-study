# Week 1 kickoff — baseline diagnostic

Monday, November 16, 2026 (America/Fortaleza). About 15 minutes.

This is a warm-up, not a mock exam. There is no previous week to review, so these 10 original questions take a first look at all 7 official exam sections, with extra weight on this week's foundations (Delta / ACID / the lakehouse, compute, and Unity Catalog managed vs external tables).

Reply with your answers in one line, like:

```
1B 2C 3A 4D 5B 6C 7A 8B 9D 10C
```

Misses will be tracked and can be carried into later Monday reviews. Answers and short explanations (including why the other options are wrong) are at the bottom — try the questions first.

Official outline: [certification page](https://www.databricks.com/learn/certification/data-engineer-associate) and the [August 2026 exam guide](https://www.databricks.com/sites/default/files/2026-10/Databricks-Certified-Data-Engineer-Associate-Exam-Guide-August-2026.pdf).

## What this week looks like

Tue/Wed/Thu are 15–20 minute pills: Delta Lake (transaction log, ACID, time travel) and why a lakehouse is not just a lake plus a warehouse; compute choices and storage/compute separation; Unity Catalog managed vs external tables. Friday is a 40–60 minute deep dive on the anatomy of a governed table. Weekends are off.

---

## Questions

### 1. Databricks Intelligence Platform (6%) — Delta Lake, ACID, and the lakehouse

A team is replacing a data lake of Parquet files plus a separate warehouse with a lakehouse on Databricks. They need concurrent writers that cannot leave a table half-updated, a way to roll back a bad overnight load, and one copy of the data that both BI and ETL can use.

Which approach meets these requirements?

A. Keep schema-on-read Parquet in object storage and take a nightly copy of the whole prefix to a second bucket, using that copy as the rollback mechanism.

B. Store the data as Delta tables so the transaction log provides ACID commits, time travel can restore a prior table version after a bad load, and Unity Catalog governs access to that same copy of the data.

C. Cache every query result in Spark driver memory so BI and ETL always share one in-memory snapshot.

D. Convert every table to CSV on DBFS and treat file timestamps as the audit trail.

### 2. Databricks Intelligence Platform (6%) — compute and storage/compute separation

Analysts run concurrent ad hoc SQL on curated Delta tables throughout the day. Nightly ETL must not share a cluster with those analysts. The team wants fast startup for the interactive workloads and does not want to pay for an all-purpose cluster that sits idle between jobs.

The Delta tables already live in cloud object storage and are registered in Unity Catalog.

Which compute choice is the best fit?

A. One large all-purpose cluster shared by the analysts and the nightly ETL, sized for peak concurrency.

B. A single-node cluster for the analysts, and a fixed-size all-purpose cluster left running 24/7 for ETL.

C. A serverless SQL warehouse with autoscaling for the analysts, and job compute that starts and terminates with each scheduled ETL run.

D. A classic job cluster with a file-arrival trigger for the analysts' ad hoc SQL, because job compute is always the cheaper interactive option.

### 3. Data Ingestion and Loading (21%) — COPY INTO

A landing zone in cloud object storage (S3, ADLS, or GCS) receives a daily dump of CSV files. Files that were already loaded must not be loaded again. A late file may appear later in a folder that was already processed. The load is a scheduled batch, not a continuously running stream, and the target is a Unity Catalog Delta table.

Which ingestion approach is the best first choice?

A. `COPY INTO` from the landing path into the Delta table, which tracks already-ingested files and will pick up new files on the next run — including late arrivals.

B. A one-shot `CREATE TABLE ... AS SELECT` from the folder, which automatically skips files that were loaded yesterday.

C. A Lakeflow Connect managed connector built for SaaS applications, because every ingest into Unity Catalog should go through a managed connector.

D. Auto Loader in continuous mode with file notifications, because a daily file dump cannot be loaded unless a streaming query is running 24/7.

### 4. Data Ingestion and Loading (21%) — choosing Auto Loader, Connect, and files

You have three sources that all need to land in Unity Catalog Delta tables:

1. An operations database that already has change tracking, for near-continuous CDC.
2. A partner SaaS application that has a supported Lakeflow Connect managed connector.
3. A high volume of JSON files arriving in an S3 (or equivalent) bucket.

Which pairing is the most appropriate?

A. `COPY INTO` for the database CDC, Auto Loader for the SaaS application, and a JDBC notebook for the S3 JSON.

B. Auto Loader for the database CDC, `COPY INTO` for the SaaS application, and a Lakeflow Connect managed connector for the S3 JSON.

C. Lakeflow Connect (or JDBC/ODBC scheduled by a Job) for the database, a Lakeflow Connect managed connector for the SaaS application, and Auto Loader for the S3 JSON files.

D. A single Lakeflow Connect managed connector for all three sources, because managed connectors replace Auto Loader and `COPY INTO`.

### 5. Data Transformation and Modeling (22%) — joins

A large Silver orders table is joined to a small, slowly changing `dim_region` (a few thousand rows) on `region_id`, to attach a region name to every order. The job currently shuffles the fact table on every run. You must keep the same join result and want to avoid that shuffle.

Which approach is appropriate?

A. Write a cross join and then filter on `region_id`. Spark will always rewrite this into a broadcast inner join, so the result and the cost are the same.

B. Broadcast the small dimension — explicitly, or by staying under `spark.sql.autoBroadcastJoinThreshold` — so the fact table does not need to be shuffled for this join.

C. Raise `spark.sql.shuffle.partitions` so each fact partition is smaller. That removes the need to move data for the join.

D. `UNION ALL` the two tables and then `dropDuplicates("region_id")`, which is equivalent to a left join that attaches the region name.

### 6. Data Transformation and Modeling (22%) — Gold objects

The BI team needs a Gold object in Unity Catalog that always reflects the latest Silver data with no pipeline lag, can be queried with SQL, and should not store its own copy of the data. Incremental refresh and streaming semantics are not required.

Which Gold object should you create?

A. A materialized view, because every Gold object must be precomputed to be queryable with SQL.

B. A streaming table, because Gold layers must always be incremental.

C. A view over the Silver tables: a stored query with no separate data copy, always current when it is queried.

D. A managed Delta table that you overwrite once a week, which is current enough and uses no extra storage after each overwrite.

### 7. Working with Lakeflow Jobs (16%) — triggers

A Bronze ingestion job should start as soon as new files land in an external location, not on a clock. A downstream Silver job should start only after the Bronze output table is updated. Analysts also want a weekly dashboard refresh at 08:00 on Monday, whether or not new data arrived.

Which trigger combination matches these requirements?

A. A scheduled trigger for Bronze, a file-arrival trigger for Silver, and a table-update trigger for the dashboard.

B. A file-arrival trigger on the external location for Bronze, a table-update trigger on the Bronze table for Silver, and a scheduled trigger for the dashboard.

C. A single table-update trigger on the external location, attached to all three tasks, because a file arrival is a table update.

D. File-arrival triggers for Bronze, Silver, and the dashboard, so the dashboard never runs against stale data and never runs on a clock.

### 8. Implementing CI/CD (10%) — Git folders, bundles, and the CLI

A team keeps Lakeflow Jobs and pipeline definitions in Git. They want the same codebase promoted to dev, test, and prod, with environment-specific catalogs and warehouse IDs, and a CI workflow that validates and deploys without copying objects by hand in the workspace UI.

Which approach matches this model?

A. Use Databricks Git folders (formerly Repos) to branch, commit, and open pull requests; define a Declarative Automation Bundle (formerly Databricks Asset Bundles) in `databricks.yml` with target-specific variables and overrides; and run `databricks bundle validate` and `databricks bundle deploy` from the Databricks CLI in CI.

B. Copy notebooks from the dev workspace to prod in the UI after each change, and store prod warehouse IDs in a comment at the top of the notebook.

C. Export each job as JSON from the Jobs UI, attach the file to a GitHub Issue, and import it manually in prod after approval.

D. Treat notebook revision history in the workspace as the system of record, and use Unity Catalog model aliases to represent job versions.

### 9. Troubleshooting, Monitoring, and Optimization (10%) — run history and Spark UI

A Lakeflow Job that used to finish in about 20 minutes now takes about 70. In the Jobs UI, run history shows the regression starting three days ago. In the Spark UI, the longest stage has most tasks finishing in under a minute, but one task runs for almost an hour. That stage's maximum shuffle read is many times the median, and the slow executor shows disk spill.

What is the most likely diagnosis, and what should you do next?

A. The job is stuck on cluster startup or a library conflict. Ignore the Spark UI and restart the workspace.

B. The SQL warehouse was down. Increase `spark.sql.shuffle.partitions` to a very large number and stop using the Spark UI.

C. A wide transformation is skewed, and the oversized partition is spilling. Compare this run to the historical baseline in run history, inspect the skewed stage's task metrics, and consider salting, liquid clustering, or skew handling. Adding executors alone will not split that one fat partition.

D. Predictive optimization has failed. The only fix is to convert the table from managed to external.

### 10. Governance and Security (15%) — managed vs external tables

A data engineer creates a Unity Catalog table with no `LOCATION` clause. Later, a colleague wants the same logical table to use data files in a company-owned bucket that is already registered as a Unity Catalog external location. They also want to know what `DROP TABLE` will do in each case.

Which statement is correct?

A. Omitting `LOCATION` creates an external table. `DROP TABLE` always leaves the data files in place, and the table cannot be converted.

B. Omitting `LOCATION` creates a managed table: Databricks owns the data location, and `DROP TABLE` removes the data (after any recovery window). An external table is one whose files live in a governed external location you specify; dropping an external table removes the Unity Catalog entry but leaves the data files.

C. Managed and external tables behave the same way on `DROP TABLE`. The `LOCATION` clause is only a comment for the storage credential.

D. Unity Catalog allows only managed tables. An external table cannot be registered, so the colleague must copy the files into the metastore root.

---

## Answers

### 1 — B

**B is correct.** Delta's transaction log gives ACID commits so a writer cannot leave a table half-updated. Time travel lets you roll the table back to a version from before the bad load. Unity Catalog then governs that same Delta copy for both BI and ETL, which is the lakehouse idea: one storage layer, not a lake plus a warehouse copy.

- **A is wrong:** plain Parquet with schema-on-read has no multi-writer ACID and no first-class time travel. A nightly prefix copy is a brittle workaround, not a table-level rollback.
- **C is wrong:** driver memory is ephemeral. It is not a durable source of truth and it does not provide transactions or rollbacks.
- **D is wrong:** CSV on DBFS is not transactional, and file timestamps are not an audit or time-travel mechanism.

### 2 — C

**C is correct.** Analysts need interactive SQL with fast startup and concurrency: a serverless SQL warehouse with autoscaling is built for that. Nightly ETL should use job compute that exists only for the run, so you do not pay for idle all-purpose capacity. Both query the same Unity Catalog tables in object storage — that is storage/compute separation.

- **A is wrong:** a shared all-purpose cluster mixes interactive and scheduled work, stays billed while idle, and lets a heavy ad hoc query contend with ETL.
- **B is wrong:** a single-node cluster is for lightweight development, not concurrent BI, and a 24/7 all-purpose cluster wastes money between ETL runs.
- **D is wrong:** job compute and file-arrival triggers are for scheduled or data-triggered jobs, not for interactive ad hoc SQL.

### 3 — A

**A is correct.** `COPY INTO` incrementally loads files from cloud object storage into a Unity Catalog table, records which files have already been ingested, and will pick up new files — including late arrivals in a previously seen folder — on the next scheduled run. That matches a daily batch dump.

- **B is wrong:** `CREATE TABLE ... AS SELECT` is not incremental. It does not track ingested files and will not "automatically skip yesterday's files."
- **C is wrong:** Lakeflow Connect managed connectors are for supported enterprise and SaaS sources, not for a folder of CSVs in object storage. For files, start with `COPY INTO` or Auto Loader.
- **D is wrong:** Auto Loader can also run in triggered (batch) mode. A daily dump does not require a continuously running file-notification stream. `COPY INTO` is the simpler first choice here.

### 4 — C

**C is correct.** Pick the tool per source. CDC from a database is a connector or a JDBC/ODBC/REST client (usually scheduled by a Job). A supported SaaS application is a Lakeflow Connect managed connector. High-volume files in object storage are Auto Loader (or `COPY INTO` for simpler batch).

- **A is wrong:** `COPY INTO` reads files in object storage, not database CDC. Auto Loader also reads files, not a SaaS API. JDBC is a fallback for databases, not for S3 JSON.
- **B is wrong:** Auto Loader is not the tool for database CDC, `COPY INTO` is not the tool for a SaaS application, and a managed connector is not the tool for a bucket of JSON files.
- **D is wrong:** managed connectors do not replace Auto Loader or `COPY INTO`. You still choose per source, volume, frequency, and governance needs.

### 5 — B

**B is correct.** A small dimension can be broadcast to every executor so the large orders table is not shuffled for this join. You can broadcast it explicitly or stay under `spark.sql.autoBroadcastJoinThreshold`. The join type and the result stay the same; only the execution plan changes.

- **A is wrong:** a cross join changes cardinality and cost. You should not rely on Spark to rewrite a cross join plus a filter into the join you actually meant.
- **C is wrong:** raising `spark.sql.shuffle.partitions` changes parallelism. It does not remove the shuffle of a non-broadcast join.
- **D is wrong:** `UNION ALL` stacks rows. It does not attach region names to orders, and `dropDuplicates` is not a join.

### 6 — C

**C is correct.** A view is a stored query. It holds no separate copy of the data and, when queried, always sees the current Silver tables. That matches "no lag, no extra copy, SQL is enough, no streaming required."

- **A is wrong:** a materialized view is precomputed and refreshed. It does store results (or a materialization) and can lag between refreshes. It is a good Gold choice when you want speed, not when you want zero lag and no copy.
- **B is wrong:** a streaming table is for incremental or streaming processing. It is not required, and it is not a lag-free non-materialized query.
- **D is wrong:** a managed table is a stored snapshot. A weekly overwrite is lag, and the table does store its own copy of the data.

### 7 — B

**B is correct.** File-arrival triggers watch an external location and start Bronze when new files appear (data-driven, no compute while the location is quiet). A table-update trigger starts Silver when the Bronze table is updated. The Monday dashboard is time-based, so it uses a scheduled trigger and runs even if no new data arrived.

- **A is wrong:** it inverts the choices. Bronze should not wait for a clock, Silver should not watch the landing files directly, and the dashboard should not wait for a table update.
- **C is wrong:** an external location is not a table, so it cannot use a table-update trigger. File arrival and table update are different trigger types, and one trigger cannot express all three semantics.
- **D is wrong:** Silver is better driven by the Bronze table update (or by a dependency in the same job). A dashboard that must run every Monday needs a scheduled trigger, not file arrival.

### 8 — A

**A is correct.** Git folders are how you branch, commit, push, and open pull requests from the workspace. Declarative Automation Bundles package jobs, pipelines, and other assets as versioned configuration, with variables and target overrides so the same codebase is promoted to dev, test, and prod. The Databricks CLI (`bundle validate`, `bundle deploy`, and `bundle run`) is what CI uses to do that without click-ops.

- **B is wrong:** copying notebooks in the UI drifts between environments. Comments are not environment configuration.
- **C is wrong:** exporting and importing job JSON by hand is not a repeatable promotion with targets and overrides.
- **D is wrong:** workspace notebook history is not a Git workflow, and Unity Catalog model aliases version models, not job or pipeline definitions.

### 9 — C

**C is correct.** Run history tells you when the regression started. The Spark UI pattern — one long task, shuffle-read max far above the median, disk spill — is classic data skew on a wide stage. The next step is to inspect that stage and address the skew (salting, liquid clustering, or skew handling). More executors will not split a single oversized partition.

- **A is wrong:** cluster startup or library conflicts usually fail early, or prevent the run from starting. They do not produce one long Spark task with uneven shuffle reads.
- **B is wrong:** blindly raising `spark.sql.shuffle.partitions` does not fix a single skewed key, and you should use the Spark UI, not abandon it.
- **D is wrong:** managed vs external is about data location and ownership, not skew. Predictive optimization is unrelated to this symptom pattern.

### 10 — B

**B is correct.** A Unity Catalog table created without a `LOCATION` clause is managed: Databricks owns the data location, and dropping the table removes the data (after any recovery window). An external table points at files in a governed external location you supply. Dropping an external table removes the catalog entry and leaves the files. You can also convert between the two kinds of table when you need to change that ownership model.

- **A is wrong:** omitting `LOCATION` creates a managed table, not an external one. Dropping a managed table does not, by default, leave the data in place, and conversion is supported.
- **C is wrong:** `DROP TABLE` behavior is a main difference between managed and external tables. `LOCATION` is how you register an external table, not a comment.
- **D is wrong:** Unity Catalog supports both managed and external tables. You do not have to copy files into the metastore root in order to govern them.
