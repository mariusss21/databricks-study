# Platform wrap: Delta, Unity Catalog, and compute working together

Most Associate questions don't test one feature on its own. They describe a business need and expect you to spot which layer of the platform answers it: Delta Lake for reliable data, Unity Catalog for governance, and the compute type for cost and isolation.

## How the three layers fit

When you write to a table, the data lands as Parquet files in cloud storage, and Delta records the change as a new commit in the table's `_delta_log`. Readers only see committed versions, which is what gives you ACID transactions, and older versions stay queryable until retention settings and `VACUUM` remove their files. Unity Catalog sits above that. It maps the three-level name `catalog.schema.table` to the storage location, checks grants on every query, and captures lineage automatically when compute reads and writes through it. Compute is the engine that runs the query, and UC enforces permissions there, which is why the compute's access mode matters.

- **Delta:** `DESCRIBE HISTORY`, `VERSION AS OF` / `TIMESTAMP AS OF`, `RESTORE TABLE`, and `MERGE INTO` for upserts.
- **Unity Catalog:** `GRANT SELECT ON TABLE ... TO <group>`, managed vs external tables, volumes for non-tabular files, and lineage in Catalog Explorer.
- **Compute:** serverless (fast start, nothing to manage), all-purpose clusters for interactive work, job compute for scheduled runs (cheaper, and it shuts down after the run), and SQL warehouses for BI and SQL. The standard (formerly shared) and dedicated (formerly single user) access modes both work with UC.

## Example

Someone ran a bad `UPDATE` on the gold revenue table this morning.

```sql
DESCRIBE HISTORY main.sales.gold_daily_revenue;
RESTORE TABLE main.sales.gold_daily_revenue TO VERSION AS OF 41;
```

`RESTORE` writes a new commit that puts the table back to how it looked at version 41. The bad version stays in history too, so the rollback itself can be audited. Because the table lives in UC, the analysts' `SELECT` grant never changed, and the lineage graph still shows which jobs feed it.

## Tradeoffs and gotchas

- **Time travel needs the old files.** Running `VACUUM` with a short retention period removes the files that older versions point to, and then you can't roll back to them.
- **Raw CSV in a bucket has none of this.** There are no transactions, no history, and access control only at the storage level.
- **Managed vs external drop behavior.** Dropping a managed table removes its data too (UC gives you a short window to `UNDROP` it), while dropping an external table leaves the files in place.
- **Wrong compute costs money.** Running a nightly job on an all-purpose cluster costs more than job compute or serverless jobs.
- **Legacy paths skip governance.** No-isolation clusters and tables in the old Hive metastore don't get UC permissions or lineage.

## For the exam

When a question mixes "roll back," "audit," "governed," or "single source of truth," the answer is usually a Delta table registered in Unity Catalog, run on the cheapest compute that fits the workload. Raw files on object storage are almost never the right answer.
