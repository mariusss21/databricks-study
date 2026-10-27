# Join types on Databricks: inner, left, cross, multi-key, broadcast (plus union vs unionByName)

**Date:** Tue 2026-10-27

## Exam pill

explode() turns each array element into its own row and drops null or empty arrays. explode_outer() keeps those rows.

## Deep dive

**Join types on Databricks: inner, left, cross, multi-key, broadcast (plus union vs unionByName)**

A join glues two DataFrames side by side on matching keys; a union stacks them on top of each other. Bronze-to-Silver work is mostly these two moves, and most silent data bugs (duplicated rows, vanished rows, shifted columns) come from picking the wrong variant.

**How it works under the hood.** Spark picks a physical strategy at planning time. If one side is small enough, it uses a **broadcast hash join**: the driver collects the small table, ships a copy to every executor, and each task joins its partition of the big table locally, with no shuffle. Otherwise it usually does a **sort-merge join**: both sides get shuffled by the join key so matching keys land in the same partition, then sorted and merged. With AQE on (the default on Databricks), Spark can look at real runtime sizes after a shuffle stage and switch a sort-merge join to a broadcast join mid-query.

**The variants you need by name:**
- **`inner`** (default): keeps only rows whose keys match on both sides.
- **`left` / `left_outer`**: keeps every left row; unmatched right columns become NULL. The workhorse for enriching a fact with a dimension.
- **`right`, `full` / `full_outer`**: mirror image, or keep everything from both sides.
- **`left_semi`**: keeps left rows that have a match, but returns only left columns and never duplicates. Think `WHERE EXISTS`.
- **`left_anti`**: keeps left rows with *no* match. Great for "which orders have no customer?" checks.
- **`crossJoin()` / `CROSS JOIN`**: every row times every row, with no key. Rarely what you want on real data.
- **Broadcast hint**: `F.broadcast(dim_df)` in PySpark or `/*+ BROADCAST(d) */` in SQL forces it. The automatic cutoff is `spark.sql.autoBroadcastJoinThreshold` (10 MB by default; `-1` disables it).

**Example.**
```python
from pyspark.sql import functions as F

orders = spark.table("main.silver.orders")
cust   = spark.table("main.silver.customers")

enriched = orders.join(F.broadcast(cust), on=["customer_id", "country"], how="left")
```
Passing `on` as a list of column names gives you one `customer_id` and one `country` column in the output. If you write `orders.customer_id == cust.customer_id` instead, both columns survive and a later `select("customer_id")` fails as ambiguous. In the Spark UI you'd see `BroadcastHashJoin` and no exchange on the orders side.

**Union vs unionByName.** `df1.union(df2)` matches columns **by position**, so if one file has `(id, name)` and the other `(name, id)`, values land in the wrong columns without any error as long as the types are compatible. `unionByName()` matches **by name**, and `allowMissingColumns=True` fills absent columns with NULL, which suits Bronze files with drifting schemas. Neither DataFrame method removes duplicates (they behave like SQL `UNION ALL`); SQL `UNION` without `ALL` does dedup.

**Gotchas:**
- NULL keys never match with `==`. Use `eqNullSafe` (`<=>` in SQL) if NULL should equal NULL.
- Duplicate keys on the dimension side multiply fact rows (fan-out). Check that the key is unique before you join.
- Broadcasting something large can blow up driver or executor memory. The hint is a promise that the table really is small.
- A left join followed by a `WHERE` on a right-side column quietly turns into an inner join.

**For the exam:** expect to pick the right join type for a scenario (semi/anti versus inner/left), recognize when a broadcast join avoids a shuffle, and know that `unionByName` is the safe choice when column order differs.
