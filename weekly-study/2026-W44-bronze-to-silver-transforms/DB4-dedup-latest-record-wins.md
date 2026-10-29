# Dedup and "latest record wins": row_number() windows vs dropDuplicates, and MERGE INTO Silver

Date: Thu 2026-10-29

## Exam pill

approx_count_distinct estimates cardinality and is cheaper than an exact count(distinct).

## Deep dive

**Dedup and "latest record wins": row_number() windows vs dropDuplicates, and MERGE INTO Silver**

Bronze usually holds several versions of the same business entity: resent files, CDC updates, retries. Silver should hold one trusted row per key, so you need a survivor rule ("the newest version wins") and a safe way to apply it to an existing table.

**How it works.** A window function partitions rows by the business key, sorts each partition, and numbers the rows. With `row_number()`, every row in a partition gets a unique number 1, 2, 3 and so on. Filter `rn = 1` and you keep exactly one row per key, and the ORDER BY decides which one. This needs a shuffle, because all rows for a key have to end up on the same task. Once the batch is deduplicated, `MERGE INTO` matches it to Silver on the key and applies updates and inserts in a single Delta commit, so readers see either the whole change or none of it.

- **`row_number()`**: one survivor per key, chosen by your ORDER BY. This is the "latest record wins" pattern.
- **`rank()` / `dense_rank()`**: ties share a number, so `= 1` can return more than one row. Don't use them for dedup.
- **`dropDuplicates(["key"])`**: simpler, but which row survives is not something you control. With no columns it drops only rows that are identical in every column.
- **`QUALIFY`**: Databricks SQL lets you filter on a window result directly, without wrapping it in a subquery.
- **Streaming**: `dropDuplicatesWithinWatermark` dedups within a watermark window so state doesn't grow forever.

**Example.**

```sql
MERGE INTO main.silver.customers AS t
USING (
  SELECT * FROM main.bronze.customers_updates
  QUALIFY row_number() OVER (
    PARTITION BY customer_id
    ORDER BY updated_at DESC, _ingested_at DESC) = 1
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Run it twice and the second run changes nothing. Matched keys already have the newest timestamp, so the `AND` condition skips them. That makes the job safe to retry. `DESCRIBE HISTORY main.silver.customers` shows a MERGE commit with operation metrics such as rows updated and rows inserted.

**Gotchas.**
- If more than one source row matches the same target row, MERGE fails with an error about multiple source rows matching. That's the main reason you dedup the source before merging.
- Sort with a deterministic tiebreaker (for example the ingest timestamp, then the file name), or reruns can pick different survivors.
- The `s.updated_at > t.updated_at` guard stops a late-arriving older record from overwriting a newer one.
- MERGE rewrites the files that hold matched rows. On big tables, add predicates that narrow the target, and pair it with clustering on the key.
- `UPDATE SET *` / `INSERT *` need matching column names. New source columns need schema evolution turned on explicitly.
- `WHEN NOT MATCHED BY SOURCE` can delete or flag target rows that are missing from the source, but only use it when the source is a full snapshot.

**For the exam:** know that `row_number()` plus a filter gives a deterministic latest-row dedup, that `dropDuplicates` doesn't let you choose the survivor, and that `MERGE INTO` is the atomic upsert into Silver. Also expect a scenario where duplicate source keys break a merge.
