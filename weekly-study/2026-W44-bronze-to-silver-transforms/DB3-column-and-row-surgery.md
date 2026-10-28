# DB3: Column and row surgery: withColumn/select/alias, drop/rename, filter, split, explode and grain changes

**Date:** Wed 2026-10-28

## Exam pill

Dedup on the exam is dropDuplicates or DISTINCT. Named aggregations include count, approximate count distinct, mean, and summary.

## Deep dive

**Column and row surgery: select, withColumn, filter, split, explode, and grain changes**

Most Bronze-to-Silver work is reshaping: keep the columns you need, rename them to something sane, derive new ones, drop the junk rows, and sometimes turn one row holding an array into many rows. The tricky part isn't the syntax, it's knowing when an operation changes the grain of your table (what one row means).

**How it works under the hood.** Every one of these calls is a lazy transformation. `withColumn`, `select`, `drop`, and `filter` just add a step to the logical plan; nothing runs until an action or a write. Catalyst then collapses consecutive projections and pushes filters down toward the source, so a `filter` written after a `select` usually still runs early, and Delta can skip files using column stats. Column-level ops (select, withColumn, rename) never change the row count. `filter` can only shrink it. `explode` and friends are generators: they can multiply rows, and that's where grain changes.

**Column operations:**
- **`select` / `alias`:** `df.select(col("cust_id").alias("customer_id"), "amount")` picks and renames in one pass. `selectExpr("cast(amount as decimal(10,2)) as amount")` lets you write SQL snippets.
- **`withColumn` / `withColumns`:** adds or replaces one column (or several, from a dict). Calling `withColumn` in a long loop bloats the plan; prefer one `select` or `withColumns`.
- **`withColumnRenamed` / `drop`:** rename or remove. Neither errors if the column doesn't exist, so a typo fails silently.
- **`split(col, pattern)`:** returns an array; the pattern is a regex, so `split(col, ".")` doesn't do what you think, use `"\\."`. Index with `getItem(0)` or `[0]`.

**Row operations:**
- **`filter` / `where`:** same thing. Remember SQL null logic: `filter(col("status") != "X")` also drops rows where status is null.
- **`explode`:** one output row per array element (or key/value pair for a map). Null or empty arrays produce zero rows, so those parents vanish.
- **`explode_outer`:** same, but keeps parents with null or empty arrays, emitting a null element.
- **`posexplode` / `posexplode_outer`:** also return the element's position (`pos`, starting at 0), handy as part of a child key.

**Example.** Bronze orders have `order_id`, `total`, and `items` (an array of structs):

```python
from pyspark.sql.functions import col, explode_outer
lines = (spark.table("main.bronze.orders")
  .select("order_id", "total", explode_outer("items").alias("item"))
  .select("order_id", "total", col("item.sku").alias("sku"), col("item.qty").alias("qty")))
```

An order with three items becomes three rows; an order with no items stays as one row with null `sku`. In SQL: `SELECT order_id, item.sku FROM orders LATERAL VIEW OUTER explode(items) t AS item`. Now `sum(total)` on `lines` counts each order's total once per item, the classic fan-out bug. Order-level measures belong on the order table; line-level measures on the exploded one.

**Gotchas:**
- Only one generator is allowed per `select`; explode nested arrays in successive steps.
- `explode` silently loses parents; use `_outer` when every parent must survive.
- `cast` returns null on bad input when ANSI mode is off and raises an error when it's on (it's on by default in Databricks SQL warehouses), so `try_cast` is the safe choice for dirty Bronze strings.
- After an explode, re-check the grain with `groupBy(keys).count().filter("count > 1")`.

**For the exam:** expect to pick the right function from a scenario: `explode` vs `explode_outer` when nulls matter, `split` to break a delimited string, `select`/`withColumn`/`alias` for reshaping, `filter` for row removal, and to spot when a transform changes row counts.
