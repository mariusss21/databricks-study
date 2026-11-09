# DE1: Data skew and file layout as an interview story: salting, partitioning vs liquid clustering

- **Date:** Mon 2026-11-09
- **Week:** 2026-W46
- **Theme:** Gold objects and Lakeflow Jobs (Gerald leads)
- **Paired Databricks pill:** Gerald's DB1 for this week (Spark tuning side)

---

Welcome back, Mario! We're easing in with a topic that shows up in almost every senior DE loop: "a job got slow, what did you do?" Skew and file layout are the two halves of that story.

## The concept

**Data skew** means a few keys hold most of the rows. Spark hashes rows by key into shuffle partitions, so one partition gets 50 million rows while the other 199 get 50 thousand. 199 tasks finish in seconds and one runs for 40 minutes, maybe spilling to disk or dying with OOM. In the Spark UI you see it as one long task in a stage, with max task time and shuffle read far above the median.

Three fixes, from least to most effort:

1. **Let AQE handle it.** On Databricks, Adaptive Query Execution is on by default. Its skew-join handling splits an oversized shuffle partition into smaller ones at runtime (`spark.sql.adaptive.skewJoin.enabled`). Check this first before hand-tuning.
2. **Broadcast the small side.** If the other side of the join fits in memory, a broadcast join has no shuffle, so there's nothing to skew.
3. **Salt the hot key.** Add a random suffix (0..N-1) to the key on the big side, replicate the small side N times with every suffix, and join on (key, salt). For aggregations, aggregate on (key, salt) first, then roll up to key. This spreads one hot key across N tasks.

**File layout** is the write-side twin of skew. Classic Hive-style **partitioning** (`PARTITIONED BY (country)`) creates one folder per value, so a skewed or high-cardinality column gives you a few huge folders and thousands of tiny files. Databricks now recommends **liquid clustering** for new Delta tables: `CLUSTER BY (cols)` co-locates data by those columns without fixed folders, works with high-cardinality and skewed columns, and lets you change the keys later with `ALTER TABLE ... CLUSTER BY` without rewriting the table. `CLUSTER BY AUTO` lets Databricks pick keys from your query patterns (with predictive optimization). Liquid clustering replaces both partitioning and Z-ORDER on the same table; you can't mix them.

**Gold link for this week:** Gold tables are read by dashboards filtering on date, customer, and region. Cluster Gold facts on the columns those queries filter on, and schedule `OPTIMIZE` (or let predictive optimization do it) as a task in the Lakeflow Job that builds Gold.

## Hands-on

```python
from pyspark.sql import functions as F

N = 16  # salt buckets

# Big fact with one hot customer_id; small dim
orders = spark.table("silver.orders")
customers = spark.table("silver.customers")

orders_salted = orders.withColumn("salt", (F.rand() * N).cast("int"))
customers_salted = customers.crossJoin(
    spark.range(N).withColumnRenamed("id", "salt")
)

joined = orders_salted.join(customers_salted, ["customer_id", "salt"]).drop("salt")

# Two-step aggregation for a skewed group by
agg = (orders_salted
       .groupBy("customer_id", "salt").agg(F.sum("amount").alias("partial"))
       .groupBy("customer_id").agg(F.sum("partial").alias("revenue")))
```

```sql
-- Gold fact with liquid clustering instead of partitions
CREATE OR REPLACE TABLE gold.fct_daily_sales (
  sale_date DATE, customer_id BIGINT, region STRING, revenue DECIMAL(18,2)
) CLUSTER BY (sale_date, customer_id);

-- Query patterns changed? Change keys, no rewrite needed
ALTER TABLE gold.fct_daily_sales CLUSTER BY (sale_date, region);
OPTIMIZE gold.fct_daily_sales;
```

## Interview question

**"A nightly Spark job went from 20 minutes to 3 hours. Walk me through what you'd do."**

**Strong answer:** "First I'd find where the time went instead of guessing. In the Spark UI I'd look for the slow stage and compare max task duration and shuffle read to the median. If one task is 50x the median, that's skew, and I'd check which key is hot with a quick `groupBy(key).count()` on the input. Then I'd go from cheapest fix to most invasive. I'd confirm AQE skew-join is on, see whether the small side can be broadcast, and only then salt the hot key, because salting adds complexity the next engineer has to understand. If the issue is on the read side, like scanning too many small files or a partition column that's high-cardinality, I'd move the table to liquid clustering on the columns we actually filter on and add OPTIMIZE to the job. Finally I'd add a guard, like an alert on job duration or row counts per key, so we catch the next new hot key before the SLA breaks. In my current team, I also write the root cause up so it becomes a pattern, not tribal knowledge."

That last line is your tech-lead signal: you fix it, then you make the team better at it.

## English phrasing tip

Say "**one task was a straggler**" or "**the work wasn't evenly distributed across tasks**." Avoid "the data was too big," which sounds vague. And use "**I'd start with the cheapest fix**" to show you think in trade-offs.

## Go deeper

- [Use liquid clustering for tables (Databricks docs)](https://docs.databricks.com/aws/en/delta/clustering)
- [Adaptive query execution (Databricks docs)](https://docs.databricks.com/aws/en/optimizations/aqe)
- [Spark SQL performance tuning (Apache Spark docs)](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
