# DE3: Grain control: flattening nested data into child tables, join fan-out traps, testing the grain

- **Date:** Wed 2026-10-28
- **Week:** 2026-W44, Bronze to Silver transforms
- **Paired Databricks pill:** DB3 (Gerald): column and row surgery, including `explode` and grain changes. DB3 shows the functions; this pill covers the design decision behind them.

---

## The idea in one breath

The **grain** is what one row means ("one row per order line"). Every transform either keeps the grain, makes it finer (explode, join to a child), or makes it coarser (group by). Bugs happen when the grain changes and nobody noticed. The classic one: revenue doubles after a "harmless" join.

## 1. Flattening nested data: child tables, not one wide explode

Bronze JSON often looks like an order with an `items` array and a `payments` array. The tempting move is to explode both into one table. Don't:

- Exploding `items` gives one row per item. Fine.
- Exploding `payments` on top of that gives **items x payments** rows. 3 items and 2 payments make 6 rows, and `SUM(amount)` is now wrong.

The senior pattern is **one Silver table per grain**:

- `silver.orders`: one row per `order_id` (header fields, structs flattened with `col.*`).
- `silver.order_items`: one row per `(order_id, line_no)`.
- `silver.order_payments`: one row per `(order_id, payment_seq)`.

Each child keeps the parent key, plus a position from `posexplode` so the key is stable on re-runs. Use the `_outer` variants (`explode_outer`, `posexplode_outer`) when an empty array must not make the order disappear.

## 2. Join fan-out traps

A join keeps the grain only if the right side is **unique on the join key**. Common traps:

- A dimension that isn't unique, such as an SCD2 table joined without `is_current` or a date-range condition.
- Joining two children of the same parent (items and payments) directly, which is the same cartesian blow-up as above.
- A "lookup" table that got a duplicate during ingestion.

The fix: aggregate the many side to the target grain **before** joining, or join on the full unique key.

## 3. Test the grain, don't hope

Make the grain a checked rule in the pipeline, not a comment:

- A uniqueness check on the grain key (`COUNT(*) = COUNT(DISTINCT key)`), wired as an expectation or a job assertion.
- Row-count reconciliation: rows after the join should equal rows before it for a many-to-one join.
- Unity Catalog `PRIMARY KEY` constraints are informational on Databricks (not enforced), so declare them for documentation and the optimizer, but still test.

## Hands-on (Databricks SQL)

```sql
-- Child table at (order_id, line_no) grain
CREATE OR REPLACE TABLE silver.order_items AS
SELECT o.order_id,
       i.pos + 1             AS line_no,
       i.col.sku             AS sku,
       i.col.qty             AS qty,
       i.col.unit_price      AS unit_price
FROM bronze.orders_raw o
LATERAL VIEW OUTER posexplode(o.items) i AS pos, col;

-- Grain test: must return zero rows
SELECT order_id, line_no, COUNT(*) AS n
FROM silver.order_items
GROUP BY ALL
HAVING n > 1;

-- Safe header + payments: aggregate payments to order grain first
WITH pay AS (
  SELECT order_id, SUM(amount) AS paid_amount
  FROM silver.order_payments
  GROUP BY order_id
)
SELECT o.order_id, o.order_total, p.paid_amount
FROM silver.orders o
LEFT JOIN pay p USING (order_id);
```

In a Lakeflow Declarative Pipeline you'd put the uniqueness rule next to the table as an expectation (from DE1), so a fan-out fails the update instead of reaching Gold.

## Interview question

**"After a pipeline change, a revenue dashboard shows numbers about 2x too high. How do you debug it, and how do you stop it from happening again?"**

Strong answer:

> "My first suspect is a grain change, usually a join fan-out. I'd compare row counts and the revenue total step by step through the pipeline to find where it jumps. At that join, I'd check whether the right side is unique on the join key; typical culprits are an SCD2 dimension joined without the current-row filter, or two child tables joined to each other. The fix is to join on the full key or aggregate the many side to the target grain first. To prevent it, I'd write down the grain of every Silver and Gold table, add uniqueness and row-count checks as pipeline expectations so a fan-out fails the run, and add a reconciliation test of total revenue against the source. On my team, I'd also make 'state the grain' part of code review for any new model."

That answer shows a method (bisect by row count), the root cause, the fix, and a prevention step at the team level, which is what interviewers want from a tech lead.

## English phrasing tip

Say **"the join fanned out"** or **"the join introduced duplicates,"** and **"this table is at the order-line grain."** Avoid "the join duplicated the lines" without saying why; "the right side wasn't unique on the key" is the sentence that signals seniority.

## Go deeper

- [explode function (Databricks SQL reference)](https://docs.databricks.com/aws/en/sql/language-manual/functions/explode)
- [Transform complex data types (Databricks docs)](https://docs.databricks.com/aws/en/semi-structured/complex-types)
- [Constraints on Databricks (informational PK/FK)](https://docs.databricks.com/aws/en/tables/constraints)
