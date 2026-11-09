# Spark tuning knobs: shuffle partitions, broadcast joins, AQE, Spark UI

Date: 2026-11-09 (Mon, W46)

## Exam pill

Job run history is how you compare today’s duration with the historical baseline, not a single green check.

## Deep dive

**Spark tuning knobs: shuffle partitions, broadcast joins, AQE, and reading the Spark UI**

Spark tuning is mostly about controlling how data moves between executors (shuffles) and how evenly work is split. A few settings decide that, and the Spark UI tells you which one is hurting you.

**How it works**

A wide transformation (join, groupBy, distinct, orderBy) forces a shuffle: each task writes shuffle files, and the next stage reads the pieces it needs. The number of output partitions of that shuffle is set by `spark.sql.shuffle.partitions` (the default is 200). Too few means big partitions that spill to disk; too many means lots of tiny tasks and scheduling overhead. `spark.default.parallelism` only affects RDD operations, not DataFrame or SQL shuffles, so don't reach for it in DataFrame code.

**Main knobs**

- **`spark.sql.shuffle.partitions`:** partitions after a shuffle in DataFrame and SQL code. With AQE on, it acts as the starting point that Spark can shrink.
- **`spark.sql.autoBroadcastJoinThreshold`:** the size under which Spark broadcasts the small side of a join to every executor, avoiding a shuffle of the big side. Setting it to `-1` disables auto broadcast.
- **AQE (`spark.sql.adaptive.enabled`):** re-plans at runtime using real shuffle statistics. It coalesces small shuffle partitions, switches a sort-merge join to a broadcast join when one side turns out small, and splits skewed partitions in joins. It is on by default in current Databricks runtimes.
- **`broadcast()` hint:** `df_big.join(broadcast(df_small), "key")` forces a broadcast when you know the size and the optimizer doesn't.

**Example**

```python
spark.conf.set("spark.sql.shuffle.partitions", "64")
result = orders.join(broadcast(dim_customer), "customer_id").groupBy("country").sum("amount")
result.explain()
```

In the plan you should see `BroadcastHashJoin` instead of `SortMergeJoin`, and one fewer exchange.

**Reading the Spark UI**

In the Stages tab, compare the min, median, and max task duration and the shuffle read size. A max far above the median points to skew. Any non-zero "Spill (memory)" or "Spill (disk)" means partitions don't fit in memory, so you need more partitions or more memory. Many tasks of a few KB each mean too many partitions.

**Tradeoffs and gotchas**

- Broadcasting a table that is too big can cause out-of-memory errors on the driver or executors.
- Manual tuning matters less with AQE and serverless compute, which handle much of this for you.
- Raising partitions doesn't fix skew on a single hot key; AQE skew handling or salting does.
- Cached or already-planned results don't re-optimize, so check `explain()` after changes.

For the exam: know that shuffle comes from wide transformations, which knob controls shuffle partition count, what AQE adjusts at runtime, when a broadcast join helps, and that spill and uneven task times in the Spark UI are the signs of a tuning problem.
