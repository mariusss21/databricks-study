# Spark parallelism on single-node and serverless compute

Parallelism in Spark comes from **tasks running on cores**, not from having several machines. Each partition of your data becomes one task, and each task runs on one core. So the real question is always how many cores are available.

## Single-node compute (one machine)

Databricks runs Spark in local mode, where the driver and the executor live in the same JVM on that machine. A 16-core node still runs 16 tasks at once, one per partition, so you still get parallelism, just inside one box. What you give up:

- **There's no horizontal scaling.** RAM and disk are capped at what one machine has, so large shuffles, joins, or `collect()` calls hit the limit sooner and spill to disk or run out of memory.
- **There's no fault isolation.** A worker failing on a cluster just means its tasks get retried elsewhere, but here the driver and the work share one machine, so if it dies, everything dies.
- **The driver competes with tasks** for the same CPU and memory.
- **Shuffles don't cross the network**, which actually makes them cheaper. That's why single node is often faster for small or medium data, and it's the usual pick for development, pandas or ML work, and light jobs.

## Serverless compute

Here you don't pick machines at all. Your notebook or job talks to Spark through Spark Connect, a client that sends query plans to a cluster Databricks manages. Behind that sits a real distributed Spark cluster with a driver and executors, and Databricks sizes it automatically. You can't switch autoscaling off or choose worker counts, so "serverless without autoscale" doesn't really exist as a setting. You control behavior through the performance mode (standard versus performance-optimized) and, for jobs and pipelines, budget policies. A query on a big table still fans out across many executors.

## What stays the same everywhere

The driver still plans the query into stages and tasks, the number of partitions still sets the maximum parallelism, and a shuffle still splits stages. Partition sizing and skew matter on every compute type. On one machine, a badly skewed partition just stalls one of your cores instead of one of your workers.

## Exam angles

- Single node means driver only with no workers, good for small data and development.
- Serverless means managed, auto-scaled distributed Spark with fast startup and no cluster configuration, plus Spark Connect limits (for example, no RDD API and no `sparkContext`).
