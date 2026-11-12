# Data quality in Silver and Gold: constraints, expectations, quarantine

Date: 2026-11-12 (Thu, W46)

## Exam pill

For that skew pattern, the exam guide’s retired sample answer is to confirm adaptive query execution skew-join handling is on, not to add more executors (sample question 1, answer B).

## Deep dive

**Data quality in Silver and Gold: constraints, expectations, quarantine**

Bad rows are cheap to stop at the Silver boundary and expensive to find in a Gold dashboard. Databricks gives you two enforcement layers: table constraints on Delta tables, and expectations inside Lakeflow Spark Declarative Pipelines.

**How it works**

- **Delta constraints** are checked at write time. `NOT NULL` and `CHECK` constraints are enforced by the writer: if any row in the write violates one, the whole transaction fails and nothing is committed. `PRIMARY KEY` and `FOREIGN KEY` in Unity Catalog are informational only (not enforced), so don't count on them to reject duplicates.
- **Pipeline expectations** are declared on a dataset in a pipeline. Each is a name plus a SQL boolean condition, evaluated per row as data flows through. Violations are counted and shown in the pipeline event log and UI.

**The three expectation actions**

- **warn (default)**: invalid rows are kept and written; the metric is recorded. SQL: `CONSTRAINT valid_id EXPECT (id IS NOT NULL)`. Python: `@dp.expect(...)`.
- **drop**: invalid rows are removed before writing. SQL: `... EXPECT (...) ON VIOLATION DROP ROW`. Python: `expect_or_drop`.
- **fail**: the update stops on the first violation and the table isn't changed. SQL: `... ON VIOLATION FAIL UPDATE`. Python: `expect_or_fail`.

**Quarantine pattern**

Dropping loses the evidence. A common approach is to keep two downstream datasets from the same source: one with the rules applied (valid rows) and one with the inverted rule (invalid rows), so bad data lands in a quarantine table you can inspect and replay.

```sql
CREATE OR REFRESH STREAMING TABLE orders_silver (
  CONSTRAINT valid_amount EXPECT (amount >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT has_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
) AS SELECT * FROM STREAM(orders_bronze);

CREATE OR REFRESH STREAMING TABLE orders_quarantine AS
SELECT * FROM STREAM(orders_bronze)
WHERE NOT (amount >= 0 AND order_id IS NOT NULL);
```

What you'd see: the pipeline UI shows pass/fail counts per expectation for `orders_silver`, valid rows flow on to Gold, and the bad ones sit in `orders_quarantine`. With `FAIL UPDATE` instead, the update would stop and you'd fix the data or rule first.

**Tradeoffs and gotchas**

- A plain Delta `CHECK` failing aborts the entire write, which is strict but can block a whole batch for one bad row; expectations with drop are softer.
- Warn keeps bad data flowing, so Gold can be wrong while the pipeline looks green. Someone has to watch the metrics.
- Expectations only see the row; they can't compare across rows (e.g., uniqueness) without extra logic.
- Adding a `CHECK` constraint to an existing table validates existing data and fails if some rows violate it.

**For the exam:** know the three actions (warn default, drop, fail), that constraints and expectations are different mechanisms, and that quarantine tables preserve the dropped rows.
