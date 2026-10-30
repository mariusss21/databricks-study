# DE5: Incremental Silver-to-Gold with Delta Change Data Feed vs full recompute

- **Date:** Fri 2026-10-30
- **Week:** 2026-W44, Bronze to Silver transforms
- **Paired Databricks pill:** DB5 (Gerald): Aggregations and profiling, picking a Gold grain
- **Track:** Data engineering interview concepts (light day)

---

## The idea in one breath

Gold tables are aggregates of Silver. Every run you can either **rebuild Gold from all of Silver** (full recompute) or **touch only what changed since last run** (incremental). Full recompute is simple and always correct, but cost grows with history. Incremental is cheap and fast, but you have to answer one hard question: *which Gold rows did this change affect?*

## Three ways to build Gold

1. **Full recompute.** `CREATE OR REPLACE TABLE gold AS SELECT ... GROUP BY ...`. Correct by construction, idempotent, easy to reason about. Great when Silver is small (say under a few hundred GB) or the logic is complex.
2. **Append-only incremental.** If Silver only ever gets inserts, you can stream new rows and add them to the totals. Breaks the moment Silver gets updates or deletes, which it will after this week's MERGE and SCD work.
3. **Change-driven incremental ("recompute the affected slice").** Read what changed in Silver from the **Delta Change Data Feed (CDF)**, find the Gold keys those changes touch (for example `order_date` and `customer_id`), and recompute *only those groups* from Silver, then MERGE or overwrite them into Gold.

Option 3 is the senior answer, because it handles updates, deletes, and non-additive measures.

## Why "recompute the slice" beats "add the delta"

You can't just add the change to the old total:

- A **SUM** is additive, but an update has a before and an after (`update_preimage` and `update_postimage`), so you'd have to subtract the old value and add the new one. Doable, fragile.
- A **COUNT(DISTINCT customer_id)** or a **median** is not additive at all. You can't fix it from the delta alone.

So: use the changes to find *which groups* moved, then recompute those groups from Silver. Small read, always correct.

## The Databricks link

- **Delta Change Data Feed.** Turn it on with `delta.enableChangeDataFeed = true`. Each changed row comes with `_change_type` (`insert`, `update_preimage`, `update_postimage`, `delete`), `_commit_version`, and `_commit_timestamp`. Read it with `table_changes('silver.orders', <from_version>)` in SQL or `.option("readChangeFeed", "true")` in PySpark, batch or streaming.
- **Track your last processed version** in a small control table (or let a streaming checkpoint do it). That's your idempotency story: re-running the same version range produces the same Gold.
- **Materialized views in Lakeflow Declarative Pipelines** do this for you. On serverless, a materialized view refresh tries an **incremental refresh** first and falls back to a **full recompute** when the query isn't eligible (for example a recursive CTE) or the compute is classic. You can also set a **REFRESH POLICY** to control whether it may fall back or should fail instead. In interviews: "I'd start with an MV and only hand-roll CDF logic when I need control the MV doesn't give me."
- **Ties to this week:** DB4's MERGE into Silver and DE4's SCD2 both produce updates, which is exactly why append-only Gold logic stops working.

## Hands-on (SQL)

```sql
-- 1) Enable CDF on Silver (once)
ALTER TABLE silver.orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- 2) Which Gold groups did changes since the last run touch?
--    (last_version comes from your control table, e.g. 41)
CREATE OR REPLACE TEMP VIEW affected_days AS
SELECT DISTINCT order_date
FROM table_changes('silver.orders', 42)          -- from last_version + 1
WHERE _change_type IN ('insert', 'update_preimage', 'update_postimage', 'delete');

-- 3) Recompute only those days from Silver (non-additive measure included)
CREATE OR REPLACE TEMP VIEW gold_slice AS
SELECT o.order_date,
       SUM(o.amount)                 AS revenue,
       COUNT(DISTINCT o.customer_id) AS buyers
FROM silver.orders o
JOIN affected_days d USING (order_date)
GROUP BY o.order_date;

-- 4) Upsert the slice into Gold; days that became empty get removed
MERGE INTO gold.daily_sales g
USING (
  SELECT d.order_date, s.revenue, s.buyers
  FROM affected_days d LEFT JOIN gold_slice s USING (order_date)
) src
ON g.order_date = src.order_date
WHEN MATCHED AND src.revenue IS NULL THEN DELETE
WHEN MATCHED THEN UPDATE SET revenue = src.revenue, buyers = src.buyers
WHEN NOT MATCHED AND src.revenue IS NOT NULL THEN INSERT (order_date, revenue, buyers)
  VALUES (src.order_date, src.revenue, src.buyers);

-- 5) Save the new high-water version in the control table
```

Note we include `update_preimage`: if an order moved from Oct 28 to Oct 29, **both** days must be recomputed.

The no-code alternative:

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.daily_sales AS
SELECT order_date, SUM(amount) AS revenue, COUNT(DISTINCT customer_id) AS buyers
FROM silver.orders
GROUP BY order_date;
```

## Likely interview question

**"Your Gold daily revenue job does a full rebuild and now takes 3 hours. How would you make it incremental without breaking correctness?"**

Strong senior answer:

> "First I'd check if a full rebuild is actually the problem. If Silver is modest, sometimes the right fix is better clustering or a bigger warehouse, because full recompute is the simplest correct thing. If it really needs to be incremental, I'd enable Change Data Feed on Silver and process by commit version. From each batch of changes I take the distinct Gold keys they affect, including the *pre-image* side of updates so moved rows fix both the old and the new group, and I recompute just those groups from Silver and MERGE them into Gold. That works for non-additive measures like distinct counts, where adding deltas doesn't. I store the last processed version in a control table so reruns are idempotent. On Databricks I'd try a materialized view on serverless first, since it does incremental refresh automatically and falls back to full recompute when needed. And I'd keep a scheduled full rebuild, for example weekly, plus a reconciliation check comparing incremental Gold to a full recompute on a sample, to catch drift. One gotcha: CDF history follows the table's retention, so if the job is down longer than that, I fall back to a full rebuild."

## English phrasing tip

Use **"affected"** and **"blast radius"** naturally: *"I only recompute the partitions affected by the change, so the blast radius of each run stays small."* And for trade-offs, *"Full recompute buys simplicity at the cost of compute; incremental buys speed at the cost of complexity"* sounds crisp and senior.

## Go deeper

- [Delta Change Data Feed (Databricks docs)](https://docs.databricks.com/aws/en/delta/delta-change-data-feed)
- [Incremental refresh for materialized views (Databricks docs)](https://docs.databricks.com/aws/en/ldp/incremental-refresh)
- [table_changes function (Databricks SQL reference)](https://docs.databricks.com/aws/en/sql/language-manual/functions/table_changes)
