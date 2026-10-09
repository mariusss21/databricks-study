# Adaptive Query Execution (AQE)

AQE re-plans a query **while it runs**, using real statistics instead of guesses. It's on by default in Databricks.

## Why it exists

Normally the optimizer picks the whole plan up front from estimates: table sizes, row counts, filter selectivity. Those estimates are often wrong, especially after filters, joins, or UDFs. A bad guess can mean the wrong join strategy, too many or too few partitions, or one huge skewed task.

## How it does it

Spark splits a query into stages at every shuffle (joins, aggregations, `repartition`). At the end of a shuffle stage, the map side has written its output, so Spark knows the **exact size of every shuffle partition**. AQE pauses at those boundaries, called query stages, and re-optimizes the rest of the plan with those real numbers, then runs the next stage. It repeats this at each shuffle.

## What it can change

- **Coalescing shuffle partitions.** The `spark.sql.shuffle.partitions` setting defaults to 200, which is often too many for small data. That creates lots of tiny tasks with scheduling overhead. AQE merges adjacent small partitions into ones close to a target size (`spark.sql.adaptive.advisoryPartitionSizeInBytes`, 64 MB by default in open-source Spark). On Databricks you can set shuffle partitions to `auto` and let AQE decide.
- **Switching the join strategy.** If the plan said sort-merge join because a table looked big, but after filtering one side turns out to be small (under the broadcast threshold), AQE switches to a **broadcast hash join**, which sends the small side to every executor and avoids a second shuffle. It can also pick a shuffled hash join when that's cheaper than sorting.
- **Handling skewed joins.** If one partition is much larger than the median (by default more than 5 times bigger and over 256 MB), AQE **splits it into smaller pieces** and duplicates the matching partition from the other side, so one giant task doesn't hold up the whole stage. Without this you see 199 tasks finish in seconds and 1 run for 20 minutes.
- **Spotting empty relations.** If a stage produces zero rows, AQE can cut out the joins or aggregations that depend on it.

## Where you see it

In the Spark UI or the SQL tab, the plan shows `AdaptiveSparkPlan isFinalPlan=true`, plus nodes like `AQEShuffleRead` (coalesced or skewed partitions) and the join that was actually used, which may differ from the initial plan.

## Limits

AQE can only change what comes after a shuffle, because that's where it gets real statistics. It can't fix the first scan stage or a query with no shuffle, and it doesn't apply to streaming micro-batches the same way. It also can't fix skew in a `groupBy` the way it does for joins. For that you still use salting or better keys.

## Exam angles

AQE uses runtime statistics at shuffle boundaries to coalesce partitions, switch to broadcast joins, and split skewed partitions. It's enabled by default, and turning it off is rarely the fix.
