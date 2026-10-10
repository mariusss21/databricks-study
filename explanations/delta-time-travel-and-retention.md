# Delta time travel and retention: why "go back to last Tuesday" sometimes fails

Time travel lets you query or restore a Delta table as it was at an earlier version or timestamp. It works only while two things still exist: the commit history in `_delta_log` and the old Parquet files those commits point to. Two separate table properties control how long each survives, and mixing them up is the classic way to lose a rollback you were counting on.

## How it works under the hood

Every write (insert, update, merge, optimize) adds a JSON commit to `_delta_log` that lists the files it added and the files it logically removed. A "removed" file isn't deleted right away. It stays in storage, unreferenced by the current version but still referenced by older ones. That's what makes `VERSION AS OF 12` possible: Delta replays the log up to version 12 and reads the files that version listed.

Cleanup happens in two independent ways:

- **Log cleanup** happens automatically when Delta writes checkpoints. Commit entries older than the log retention window are dropped, so those versions disappear from history.
- **File cleanup** happens only when `VACUUM` runs. It physically deletes unreferenced files older than the deleted-file retention window. After that, any version that needed those files can't be read anymore, even if its log entry is still there.

## The settings

- **`delta.logRetentionDuration`** controls how long history stays (default `interval 30 days`).
- **`delta.deletedFileRetentionDuration`** controls what `VACUUM` may delete (default `interval 7 days`).
- **`VACUUM table RETAIN n HOURS`** overrides the window for one run. Going below the 7-day default is blocked by a safety check unless you turn off `spark.databricks.delta.retentionDurationCheck.enabled`, which you almost never should.
- **`VACUUM table DRY RUN`** lists what would be deleted without deleting it.

## Example

```sql
DESCRIBE HISTORY sales.orders;                        -- find the bad load's version
SELECT * FROM sales.orders TIMESTAMP AS OF '2026-10-01';
RESTORE TABLE sales.orders TO VERSION AS OF 41;

ALTER TABLE sales.orders SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 90 days',
  'delta.deletedFileRetentionDuration' = 'interval 90 days'
);
```

With the defaults, `DESCRIBE HISTORY` can still show a version from three weeks ago, yet querying it fails after a vacuum because its data files are gone. To really keep 90 days of rollback, set both properties to at least 90 days.

## Gotchas

- **History isn't the same as recoverability.** Seeing a version in `DESCRIBE HISTORY` doesn't mean you can read it.
- **Vacuum can run without you.** On Unity Catalog managed tables, predictive optimization can run `VACUUM` automatically, so the retention properties are your real guardrail.
- **Retention costs storage.** Longer windows keep every rewritten file around, and that adds up fast on tables with frequent `MERGE` or `OPTIMIZE`.
- **Aggressive vacuuming can break readers.** Long-running queries or streams that still need older files can fail.
- **`RESTORE` is itself a new commit.** It doesn't erase the bad versions from history.

## For the exam

Know `VERSION AS OF`, `TIMESTAMP AS OF`, `DESCRIBE HISTORY`, and `RESTORE`, and remember that `VACUUM` with the default 7-day retention is what limits how far back you can actually time travel.
