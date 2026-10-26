# DE1: Silver data quality and conformance (expectations, quarantine, fail vs drop vs warn)

- **Date:** Mon 2026-10-26
- **Week:** 2026-W44, Bronze to Silver transforms
- **Paired Databricks pill:** DB1 (Gerald), Bronze-to-Silver cleaning: reading Bronze by UC name, explicit casts, null rules, timestamp standardization
- **Read time:** about 5 minutes

---

Last week you made Bronze tolerant: land everything, lose nothing. This week Silver becomes the strict layer, the first place where a downstream team can trust a row. Gerald's DB1 shows the *how* (casts, null handling, timestamps). This pill is the *why and the policy*: what counts as a bad row, and what you do with it.

## 1. Two jobs in Silver: validate and conform

- **Validate:** does the row obey the rules? Not-null keys, valid ranges, allowed values, parseable timestamps, referential checks.
- **Conform:** make the same thing look the same everywhere. One timestamp standard (UTC), one currency code format, one spelling of country and status, consistent types and column names across sources. Conformance is what lets you join `crm.customers` with `erp.customers` later without surprises.

## 2. Fail vs drop vs warn: the core decision

Every rule needs an action, and the action is a business decision, not a technical one.

| Action | What happens | Use it when |
|---|---|---|
| **Warn** (keep the row, record the metric) | Row flows through, violations are counted | Soft rules: a missing optional email, an odd but possible value |
| **Drop** | Row is removed from the target, counted in metrics | The row is useless downstream, and losing it is acceptable |
| **Fail** | The update stops; nothing is written | A violation means something upstream is badly broken (e.g. primary key null on every row, wrong schema), so writing would be worse than writing nothing |
| **Quarantine** (a pattern, not a keyword) | Bad rows go to a side table with the reason | You can't afford to lose data silently, and someone must fix and replay it |

The senior instinct: **drop is a silent data-loss decision.** If finance would care about the missing row, quarantine it instead.

## 3. How Databricks does it

**Expectations in Lakeflow Declarative Pipelines** are the native tool. Each expectation is a name, a SQL boolean constraint, and an action:

- Warn: `CONSTRAINT x EXPECT (...)` / `@dp.expect`
- Drop: `ON VIOLATION DROP ROW` / `@dp.expect_or_drop`
- Fail: `ON VIOLATION FAIL UPDATE` / `@dp.expect_or_fail`
- Groups of rules: `@dp.expect_all`, `@dp.expect_all_or_drop`, `@dp.expect_all_or_fail`

Pass and fail counts land in the pipeline event log, so you get data quality metrics for free.

**Outside pipelines,** Delta `CHECK` and `NOT NULL` constraints on a Unity Catalog table act as a hard "fail" gate on every write, from any job. Use them as the last line of defense on Silver tables, not as your only rule engine.

**Over time,** Unity Catalog data quality monitoring watches freshness, volume, and distribution drift on the tables, which catches the problems a row-level rule can't see (e.g. "today we got 10% of the usual rows").

## 4. Hands-on: expectations plus a quarantine table

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_amount":   "amount >= 0",
    "valid_ts":       "order_ts IS NOT NULL",
}
# coalesce: a NULL comparison counts as a failure, not as "unknown"
QUARANTINE_IF = "NOT coalesce(({}), false)".format(" AND ".join(f"({r})" for r in RULES.values()))

@dp.table(comment="Bronze orders, cast and conformed, with a validity flag")
def orders_conformed():
    return (
        spark.readStream.table("main.bronze.orders_raw")
        .select(
            F.col("order_id").cast("bigint").alias("order_id"),
            F.col("amount").cast("decimal(18,2)").alias("amount"),
            F.to_utc_timestamp(F.col("order_ts"), "America/Sao_Paulo").alias("order_ts"),
            F.upper(F.trim("currency")).alias("currency"),
        )
        .withColumn("is_quarantined", F.expr(QUARANTINE_IF))
    )

@dp.table(name="silver_orders")
@dp.expect_all_or_drop(RULES)            # good rows only
@dp.expect("known_currency", "currency IN ('USD','BRL','EUR')")  # warn only
def silver_orders():
    return spark.readStream.table("orders_conformed").drop("is_quarantined")

@dp.table(name="silver_orders_quarantine")
def silver_orders_quarantine():
    return (
        spark.readStream.table("orders_conformed")
        .filter("is_quarantined")
        .withColumn("quarantined_at", F.current_timestamp())
    )
```

And the hard gate on a plain Delta table, for jobs that write outside the pipeline:

```sql
ALTER TABLE main.silver.orders ALTER COLUMN order_id SET NOT NULL;
ALTER TABLE main.silver.orders ADD CONSTRAINT non_negative_amount CHECK (amount >= 0);
```

## 5. Interview question

> "How do you handle data quality between your raw and curated layers? What happens to a bad record?"

**Strong senior answer:**

"I treat it as a policy per rule, agreed with the data consumers, not a blanket filter. Bronze stays append-only and tolerant, so the raw record is never lost and I can always replay. In Silver I split rules into three tiers. Hard rules, like a null primary key or a broken schema, fail the run, because writing garbage is worse than being late, and the on-call gets alerted. Row-level rules, like a negative amount, route the row to a quarantine table with the rule name and a timestamp instead of silently dropping it, so someone owns fixing it and we replay it after the fix. Soft rules just warn and feed a metric. On Databricks I implement that with Lakeflow Declarative Pipelines expectations, warn, drop, and fail, plus a quarantine flag pattern, and I add Delta CHECK constraints on the Silver tables as a last gate. Then I track pass rates from the event log, and use table monitoring for volume and freshness, because many real incidents aren't bad rows, they're missing rows. As a tech lead, the part I push hardest is ownership: every quarantine table has an owner and an SLA, otherwise it becomes a graveyard."

## 6. English phrasing tip

- Say **"we quarantine bad records"** and **"the pipeline fails fast on schema breaks."** "Fail fast" is a phrase US interviewers love.
- Avoid "we eliminate the wrong data." Say **"we filter out invalid rows"** or **"we drop records that violate the constraint."**
- "Conform" is a good word here: **"Silver is where we conform the sources to one standard."**

## 7. Go deeper

- [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/ldp/expectations)
- [Expectation recommendations and advanced patterns, including quarantine](https://docs.databricks.com/aws/en/ldp/expectation-patterns)
- [Constraints on Databricks (NOT NULL and CHECK)](https://docs.databricks.com/aws/en/tables/constraints)

Tomorrow (DE2): dimensional modeling basics, facts, dimensions, grain, and surrogate keys.
