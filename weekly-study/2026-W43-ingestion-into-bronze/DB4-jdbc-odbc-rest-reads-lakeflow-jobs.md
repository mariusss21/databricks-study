# JDBC, ODBC, and REST reads from notebooks, scheduled as Lakeflow Jobs

**Date:** Thu 2026-10-22

## Exam pill

JDBC, ODBC, or REST reads inside a notebook are an allowed ingest path, usually scheduled as a Lakeflow Job.

## Deep dive

**JDBC, ODBC, and REST reads from notebooks, scheduled as Lakeflow Jobs**

When a source has no managed Lakeflow Connect connector and isn't files in cloud storage, the fallback is code: a notebook that pulls from a database over JDBC (or ODBC), or from an HTTP API, and appends to a Bronze table. A Lakeflow Job then runs that notebook on a schedule, so "custom code plus a job" becomes your ingestion pipeline.

**How it works under the hood**

A JDBC read is a Spark data source. The driver node opens a connection, asks the database for the schema, then plans the read. With no partitioning options it's one task and one connection, so one executor core pulls the whole table. With partitioning, Spark splits a numeric, date, or timestamp column into ranges and runs one query per range in parallel. Filters you apply can be pushed down into the source's `WHERE` clause. REST is different: a `requests` loop runs only on the driver, so you collect pages into Python, build a DataFrame, and write it. Nothing about it is distributed unless you design it to be.

**The options that matter**

- **`url`, `dbtable` or `query`**: where to connect and what to read. Use one of `dbtable` and `query`, not both, and `query` can't be combined with `partitionColumn`.
- **`partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`**: set all four together. The bounds only decide the range width; they don't filter rows, so rows outside the range still land in the edge partitions.
- **`fetchsize`**: how many rows come back per round trip. Some drivers default very low, which makes big reads slow.
- **Credentials via `dbutils.secrets.get(scope, key)`**: secret values are redacted in notebook output. Never hardcode passwords.
- **ODBC**: in Databricks it's mostly the reverse direction (BI tools connecting into a SQL warehouse). Reading a source with `pyodbc` in a notebook is possible, but it runs on the driver only, like REST.

**Example**

```python
pwd = dbutils.secrets.get("erp", "jdbc-password")
last = spark.sql("SELECT max(updated_at) FROM bronze.erp_orders").first()[0]

df = (spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://erp-host:5432/erp")
      .option("dbtable", f"(SELECT * FROM orders WHERE updated_at > '{last}') o")
      .option("user", "ingest").option("password", pwd)
      .option("partitionColumn", "order_id")
      .option("lowerBound", 1).option("upperBound", 5000000)
      .option("numPartitions", 8)
      .load())

from pyspark.sql.functions import current_timestamp

df.withColumn("_ingested_at", current_timestamp()) \
  .write.mode("append").saveAsTable("bronze.erp_orders")
```

You'd see eight parallel queries against Postgres in the Spark UI, then an append to Bronze. Wrap it as a notebook task in a job with a schedule trigger and retries, pass the source name as a job parameter, and you have a repeatable load.

**Tradeoffs and gotchas**

- **Retries replay the run.** If the job dies after the write but before anything records progress, the retry appends again. Store the high-water mark yourself and make the write idempotent (dedup downstream, `MERGE`, or `replaceWhere` on a load partition).
- **Too many partitions can hurt the source.** Eight parallel scans on a production OLTP database is a real load; agree on it with the database owner.
- **The driver must be on your compute.** Common databases ship with Databricks, but a niche JDBC driver needs installing, and serverless compute limits what you can install.
- **REST is your problem end to end**: pagination, rate limits, and auth refresh. Keep it simple and land the raw responses first.
- **Lakehouse Federation** lets you query some external databases through Unity Catalog without copying. It's a different tool from ingestion, so don't mix them up.

**For the exam:** know that a notebook doing JDBC or REST reads, scheduled by a Lakeflow Job, is a valid ingestion path when no connector fits, that secrets come from `dbutils.secrets`, and that the choice versus Lakeflow Connect, Auto Loader, or `COPY INTO` comes down to the source type and how data arrives.
