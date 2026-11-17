# Pipeline expectations in practice and the pipeline event log

Date: 2026-11-17 (Tue, W47)

## Exam pill

A job cluster exists for one job run and then terminates. An all-purpose cluster stays up for interactive work and keeps billing if it is left on.

## Deep dive

**Pipeline expectations in practice and the pipeline event log**

Expectations are row-level data quality rules you attach to a dataset in a Lakeflow Spark Declarative Pipeline. They solve the "bad data silently reached Silver" problem: each rule is checked as data flows through, and every result is recorded so you can watch quality over time.

**How it works.** An expectation has a name and a SQL boolean predicate. When a flow processes a batch of rows, the predicate is evaluated per row. The row passes or fails, the outcome is counted, and the configured policy decides what happens to failing rows. The counts (passed and failed records per expectation) are written to the pipeline's event log, along with lineage, update progress, and errors. The event log is itself a queryable Delta table, so quality becomes data you can trend.

**The three policies:**
- **Warn (default):** `CONSTRAINT valid_id EXPECT (id IS NOT NULL)`. Bad rows are kept and written; only the metric records the violation.
- **Drop:** `... EXPECT (...) ON VIOLATION DROP ROW`. Failing rows are discarded and counted.
- **Fail:** `... EXPECT (...) ON VIOLATION FAIL UPDATE`. The update stops with an error and the transaction is not applied. Fix the data or the rule, then rerun.

In Python the equivalents are the decorators `@dp.expect`, `@dp.expect_or_drop`, `@dp.expect_or_fail` (plural forms like `expect_all` take a dict of rules). Check the current module name in your runtime docs, since the API moved from `dlt` to `pyspark.pipelines`.

**Example.**
```sql
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT order_id_present EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT amount_positive  EXPECT (amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT has_country      EXPECT (country IS NOT NULL)
) AS SELECT * FROM STREAM(bronze_orders);
```
If 3 rows have `amount <= 0`, they are dropped and the update still succeeds. If one row has a null `order_id`, the whole update fails. Rows with a null country are kept, but the metric shows them.

**Reading the event log.** Query it with the `event_log()` table-valued function (by pipeline ID or table name), or from the table if you published it. Quality metrics sit in events of type `flow_progress`, inside the details under `flow_progress.data_quality.expectations`, with name, dataset, passed_records and failed_records. Errors and failed updates show up in the same log, so it is also your first stop when a run fails.

**Tradeoffs and gotchas.**
- Rules are per row. Cross-row checks (uniqueness, referential integrity) need a different approach, such as an aggregate or a join in a quarantine flow.
- Drop loses data from the target. If you need to inspect bad rows, route them to a quarantine table using the inverse predicate instead of discarding.
- Use fail only where wrong data is worse than late data, such as keys and amounts.
- Predicates should be cheap and deterministic.

**For the exam:** know the three policies and their default (warn keeps the row), that metrics land in the event log, and which policy fits "stop the pipeline" versus "remove bad rows and continue".
