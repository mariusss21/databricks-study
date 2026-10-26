# Bronze-to-Silver cleaning: reading Bronze by UC name, explicit casts, null rules, timestamp standardization

Date: Mon 2026-10-26

## Exam pill

In Spark SQL, UNION drops duplicate rows and UNION ALL keeps them, but DataFrame.union() behaves like UNION ALL, not like SQL UNION.

## Deep dive

**Bronze-to-Silver cleaning: casts, null rules, and timestamps**

Bronze keeps data the way it arrived, usually as strings with gaps and mixed time formats. Silver is where you turn it into typed, consistent columns that downstream joins and aggregations can trust, and you do it in a way you can re-run.

**How it works**

1. **Read Bronze by its Unity Catalog name**, never by a storage path: `spark.read.table("main.bronze.orders")` for batch, `spark.readStream.table(...)` for incremental, or `FROM main.bronze.orders` in SQL. The three-level name means Unity Catalog's grants and lineage apply.
2. **Cast explicitly** to the target types. Don't trust schema inference for Silver.
3. **Apply null rules** column by column, based on what the business says a missing value means.
4. **Standardize timestamps** to a single type and zone.
5. **Write to a Silver table**, typically as Delta, with a known schema.

**The tools, by step**

- **Casts:** `CAST(amount AS DECIMAL(10,2))` in SQL or `col("amount").cast("decimal(10,2)")` in PySpark. When ANSI mode is on (`spark.sql.ansi.enabled`, which is the default in Databricks SQL), a value that can't be cast fails the query. `try_cast(...)` returns NULL instead, so you can catch the bad rows yourself.
- **Coalesce:** `coalesce(country, 'UNKNOWN')` returns the first non-null value. Use it when a fallback is meaningful.
- **Fill and drop:** `df.na.fill({"qty": 0})` and `df.na.drop(subset=["order_id"])`. Drop rows only when the missing key makes them unusable.
- **Sentinels:** values like `-1` or `'UNKNOWN'` keep a row joinable, but they're real values to `avg()` and `count()`.
- **Timestamps:** `to_timestamp(col, 'yyyy-MM-dd HH:mm:ss')` and `to_date(...)` parse strings, and `to_utc_timestamp(ts, 'America/Sao_Paulo')` converts local time to UTC. A Spark `TIMESTAMP` is an instant that gets displayed in the session time zone (`spark.sql.session.timeZone`). `TIMESTAMP_NTZ` is the type with no zone attached.

**Example**

```sql
CREATE OR REPLACE TABLE main.silver.orders AS
SELECT
  CAST(order_id AS BIGINT)                AS order_id,
  try_cast(amount AS DECIMAL(10,2))       AS amount,
  coalesce(upper(trim(country)), 'UNKNOWN') AS country,
  to_utc_timestamp(to_timestamp(order_ts, 'yyyy-MM-dd HH:mm:ss'),
                   'America/Sao_Paulo')   AS order_ts_utc
FROM main.bronze.orders
WHERE order_id IS NOT NULL;
```

Run it on a row where `amount = 'abc'` and that row still lands, but with `amount` as NULL. A row with no `order_id` is filtered out. Re-run it and you get the same table, because `CREATE OR REPLACE` rebuilds the result. For big tables you'd switch to an incremental pattern later in the week (MERGE).

**Tradeoffs and gotchas**

- `try_cast` hides bad data. Count the NULLs it produced, or send those rows to a quarantine table, instead of letting them disappear silently.
- If you parse with the wrong pattern, the timestamp comes back NULL, or with ANSI on the query errors. Check with `try_to_timestamp` before you commit to a format.
- Converting a time zone twice shifts every event by hours. Decide once where the conversion to UTC happens.
- Sentinels distort metrics. `count(country)` counts `'UNKNOWN'` but skips NULL.
- `na.fill` only touches columns whose type matches the fill value.

**For the exam:** expect to pick the right cleaning function (`cast`, `coalesce`, `na.drop`/`na.fill`, `to_timestamp`) for a scenario, to read tables by three-level Unity Catalog name, and to know that Silver is the layer where data gets typed, cleaned, and conformed.
