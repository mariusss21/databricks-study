# Aggregations and profiling: count, countDistinct vs approx_count_distinct, summary/describe, picking a Gold grain

Date: Fri 2026-10-30

## Exam pill

The tuning parameters named in the guide are spark.sql.shuffle.partitions, spark.default.parallelism, executor and driver memory, and spark.sql.autoBroadcastJoinThreshold, and you re-measure after changing them.

## Deep dive

**Aggregations and profiling: counting right, then picking a Gold grain**

Aggregation is where Silver rows become the numbers people actually read, and profiling is how you check the data before you trust those numbers. Most wrong Gold metrics come from two places: counting the wrong thing, or aggregating at a grain nobody wrote down.

**How it works under the hood.** A `groupBy().agg()` runs in two stages. Each task first does a partial aggregate on its own partition (a local sum or count per key), then Spark shuffles those partial results by the grouping key, and a second stage merges them. That's why `sum` and `count` scale well: only one small partial row per key per partition crosses the network. An exact distinct count is harder, because Spark has to dedupe the actual values per key, so far more data moves in the shuffle and memory pressure goes up.

**The functions and what they really count:**
- **`count(*)`** counts rows, nulls included. **`count(col)`** skips rows where `col` is null, and that difference is a classic trick question.
- **`countDistinct(col)`** (SQL `count(DISTINCT col)`) is exact and ignores nulls. It's the right call for reconciliations and anything financial.
- **`approx_count_distinct(col, rsd)`** uses the HyperLogLog++ sketch, which keeps a small fixed-size summary instead of every value. `rsd` is the allowed relative error, and a lower value is more accurate but uses more memory. Use it on huge, high-cardinality columns where "about 4.2 million users" is good enough.
- **`avg`/`mean`, `min`, `max`, `sum`** ignore nulls, so an average over a column with many nulls is an average of the non-null rows only.
- **`describe()`** returns count, mean, stddev, min and max. **`summary()`** adds the 25%, 50% and 75% percentiles, which are approximate. In a Databricks notebook, `display(df)` also offers a Data Profile tab.

**Example.** Profile Silver, then build Gold at a stated grain:

```python
from pyspark.sql import functions as F

s = spark.table("main.silver.orders")
s.select("amount_brl", "customer_id").summary().show()

gold = (s.groupBy(F.to_date("order_ts").alias("order_date"), "store_id")
          .agg(F.count("*").alias("orders"),
               F.countDistinct("customer_id").alias("customers"),
               F.sum("amount_brl").alias("revenue_brl")))
```

The grain is one row per date per store. You'd then check that `sum(revenue_brl)` in Gold equals `sum(amount_brl)` in Silver for the same window, and that no `(order_date, store_id)` pair appears twice.

**Tradeoffs and gotchas:**
- **Distinct counts don't add up.** You can't sum daily `customers` to get monthly uniques, because one customer can show up on many days. Recompute at the coarser grain, or keep the raw rows.
- **Explode before aggregating changes counts.** A fan-out from a join or `explode` inflates `count(*)` and `sum`, so check the grain first.
- **Shuffle size matters.** After a wide aggregation, `spark.sql.shuffle.partitions` and AQE decide how many tasks the merge stage gets. That's tomorrow's topic.
- **Alias every aggregate** so BI tools don't see names like `sum(amount_brl)`, and keep units in the name.

For the exam: know `count(*)` vs `count(col)`, exact `countDistinct` vs `approx_count_distinct`, what `describe` and `summary` return, and why Gold needs a declared grain checked against Silver totals.
