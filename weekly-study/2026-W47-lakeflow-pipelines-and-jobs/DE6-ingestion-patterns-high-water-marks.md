# DE6: Ingestion patterns (full vs incremental, high-water marks, APIs, and build vs buy)

- **Date:** Sat 2026-11-21
- **Week:** 2026-W47
- **Theme:** Lakeflow pipelines and Jobs in depth
- **Paired Databricks pill (Gerald):** DB6: Lakeflow Connect: configuring a managed connector, incremental vs full, governing and monitoring the destination tables
- **Format:** light weekend pill

---

Good morning, Mario! Light Saturday pill. Gerald shows you how to configure **Lakeflow Connect**. This pill covers the engineering judgment behind it: when a source has **no CDC**, how do you pull only what changed, and when should you write the extractor yourself?

## Three ways to get data out of a source

| Pattern | How it works | Good for | Weak spot |
|---|---|---|---|
| **Full snapshot** | Re-read the whole table every run | Small tables, sources with no reliable change column | Cost grows with table size, not with change volume |
| **Incremental by high-water mark** | `WHERE updated_at > last_max` (the "cursor column") | Medium/large tables with a trustworthy `updated_at` or increasing id | **Misses deletes**, late commits, clock skew |
| **Log-based CDC** | Read the DB log (WAL/binlog), as in Wednesday's DE3 | Big, hot OLTP tables, when you need deletes and order | Needs DB setup and permissions, more moving parts |

## High-water marks: the four traps seniors mention

1. **Late commits.** A transaction starts at 10:00:59 and commits at 10:01:05. If your run read up to 10:01:00, that row has `updated_at` 10:00:59, so it's below your new mark and you never see it. **Fix:** re-read an **overlap window** (for example, the last 15 minutes) and make the write **idempotent** with `MERGE`.
2. **Deletes are invisible.** A deleted row has no `updated_at`. **Fix:** ask for soft deletes, or run a periodic full snapshot and reconcile it.
3. **Ties and boundaries.** Use `>=` with dedupe, not `>`. Otherwise you lose rows that share the boundary timestamp.
4. **Commit the mark last.** Save the new high-water mark only **after** the target write succeeds. Keep it in a small Unity Catalog control table, not in a notebook variable.

## API ingestion in one breath

Use **cursor-based pagination** (more stable than offset), handle **429s with exponential backoff and jitter**, use an `updated_since` parameter as your high-water mark, keep credentials in **secrets or UC connections**, and **land the raw JSON in a UC Volume** so Auto Loader picks it up into Bronze. Then you can always replay from raw.

## How it maps to Databricks

- **Lakeflow Connect managed connectors.** Database CDC connectors (MySQL, PostgreSQL, SQL Server), SaaS connectors (Salesforce, Workday, HubSpot, Jira, and more), and **query-based connectors**, which query the source directly without CDC setup, so they're the managed version of the high-water-mark pattern. Some SaaS connectors (Salesforce, for example) pick a cursor column from a set list. All of them produce UC-governed ingestion pipelines on serverless compute.
- **Snapshots to SCD2 for free.** If all you can get is full snapshots, `AUTO CDC FROM SNAPSHOT` (`dp.create_auto_cdc_from_snapshot_flow`) diffs consecutive snapshots and builds SCD Type 1/2, deletes included.
- **The long tail.** For an API with no connector, write a **PySpark custom data source** (`DataSourceReader`, or `DataSourceStreamReader` for incremental) and keep the extractor inside Spark and UC.
- **Build vs buy** (recap of W43 DE5): Fivetran/Airbyte or Lakeflow Connect for standard SaaS and databases. Build only for the long tail, or when the managed connector can't meet your latency or cost.

## Hands-on: high-water mark with overlap + idempotent MERGE (PySpark)

```python
from pyspark.sql import functions as F

ctl = "main.ops.ingest_watermarks"   # (source STRING, hwm TIMESTAMP)
src = "orders"
hwm = spark.table(ctl).where(F.col("source") == src).first()["hwm"]

changes = (spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("query", f"""SELECT * FROM public.orders
        WHERE updated_at >= TIMESTAMP '{hwm}' - INTERVAL '15 minutes'""")  # overlap window
    .load()
    .dropDuplicates(["order_id", "updated_at"]))
changes.createOrReplaceTempView("changes")

spark.sql("""
MERGE INTO main.bronze.orders t
USING (SELECT * FROM changes
       QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1) s
ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *   -- re-reads are no-ops
WHEN NOT MATCHED THEN INSERT *
""")

# Only after the write succeeds: advance the mark
new_hwm = changes.agg(F.max("updated_at")).first()[0]
if new_hwm:
    spark.sql(f"UPDATE {ctl} SET hwm = TIMESTAMP '{new_hwm}' WHERE source = '{src}'")
```

Run it twice: the second run re-reads the overlap but changes nothing. That's the idempotency you'll describe in interviews.

## Likely interview question

**"A Postgres table has no CDC and only an `updated_at` column. Design incremental ingestion. What can go wrong?"**

**Strong answer:** "First I'd check whether `updated_at` is reliable: set by a trigger or the app on every write, and indexed. Then I'd use a high-water mark stored in a control table, re-read an overlap window to catch late commits and clock skew, and apply with an idempotent MERGE guarded by `updated_at`, so reruns and backfills are safe. I'd advance the mark only after a successful write. The two gaps are deletes and missed rows, so I'd ask for soft deletes or run a weekly full snapshot to reconcile, and alert on row-count drift between source and target. On Databricks I'd try a Lakeflow Connect query-based connector before building this myself. If the table gets big or hot, or the business needs deletes and ordering, I'd push for log-based CDC with Debezium or the Postgres CDC connector."

## English phrasing tip

Keep **high-water mark** (your ingestion cursor) separate from **watermark** (in streaming, the late-data threshold for stateful operations). Interviewers notice when you mix them up. A clean line: *"We re-read a 15-minute overlap window and rely on an idempotent merge, so late commits are never lost and reruns are no-ops."*

## Go deeper

- [Lakeflow Connect connector concepts](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/)
- [The AUTO CDC APIs (including AUTO CDC FROM SNAPSHOT)](https://docs.databricks.com/aws/en/ldp/cdc)
- [PySpark custom data sources](https://docs.databricks.com/aws/en/pyspark/datasources)

Tomorrow: the W47 capstone, a system design for near-real-time CDC from Postgres to Gold dashboards. Enjoy the workshop! 🌎💵
