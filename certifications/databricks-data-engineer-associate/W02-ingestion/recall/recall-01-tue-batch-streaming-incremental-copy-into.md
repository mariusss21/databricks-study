# Evening recall · W2 Tue · Batch, streaming, incremental, and COPY INTO

**Tue Nov 24, 2026 · ~5 min · Exam Section 2 (Data Ingestion and Loading)**
Pill: [01-tue-batch-streaming-incremental-copy-into.md](../01-tue-batch-streaming-incremental-copy-into.md)

Answer from memory, without opening the pill. Rough or partial answers are perfect.

## Questions

**Q1. Brain dump.** Without looking, write down the 3-5 things you remember about batch vs streaming vs incremental loading and `COPY INTO`.

**Q2. Explain or apply.** Your Airflow task runs `spark.sql("COPY INTO main.bronze.orders FROM '/Volumes/main/raw/landing/orders' FILEFORMAT = CSV ...")`. It timed out and Airflow retried it 3 times. A teammate worries the table now has duplicate rows. What do you tell them, and where could duplicates still sneak in?

**Q3. Connect.** Someone reloads a file with `COPY_OPTIONS ('force' = 'true')` by mistake, and that day's rows are now doubled in Bronze. Which Week 1 Delta command undoes this, and why that one and not its "cousin"?

## Hints (only if stuck)

- Q1: Think "how vs what", the three delivery guarantees, what "idempotent" means, and when Databricks prefers something else over `COPY INTO`.
- Q2: Where does `COPY INTO` keep its memory of loaded files, and is it idempotent per file or per row?
- Q3: One command undoes a change to a table that still exists; the other undoes a drop.

---

## Model answers (for grading; not sent in chat)

**Q1.** Any 3-5 of:
- Batch vs streaming is *how* data is processed (bounded chunks vs an unbounded flow); incremental is *what* is processed (only what's new), and it works in either style.
- Incremental needs state / memory of what was done: a file list (`COPY INTO`), a checkpoint (Auto Loader / streaming), or a watermark column.
- Delivery semantics: at-most-once (no retries, can lose data), at-least-once (retries, can duplicate), exactly-once = at-least-once + idempotent or transactional sink.
- Idempotent = running twice gives the same result as once; that's what makes reruns, retries and backfills safe.
- `COPY INTO` records loaded files in the target table's metadata and skips them, so a rerun with no new files inserts 0 rows. It always appends; never overwrites.
- Options: `VALIDATE` is a dry run (loads nothing); `force` reloads files; `FILES` and `PATTERN` can't be combined; `mergeSchema` in `FORMAT_OPTIONS` = merge source schemas when reading, in `COPY_OPTIONS` = let the target table add columns.
- `COPY INTO` fits thousands of files and SQL-first teams; millions of files, continuous arrival or heavy schema drift → Auto Loader (in SQL: streaming table over `read_files`). `availableNow` = streaming code run as an incremental batch.

**Q2.** Retries are safe: `COPY INTO` tracks loaded files in the table's metadata and skips them, and each run is an all-or-nothing Delta commit, so a failed attempt leaves nothing half-written and a later attempt only loads files not yet committed. Duplicates can still appear because it is idempotent *per file*, not per row: two different files containing the same order both land, and so do rows reloaded with `'force' = 'true'`. Fix that downstream with dedup or `MERGE` on a business key into Silver.

**Q3.** `RESTORE TABLE main.bronze.orders TO VERSION AS OF <version before the force load>` (find the version with `DESCRIBE HISTORY`). RESTORE rolls an existing table back to an earlier version; `UNDROP` only brings back a *dropped* table, and this table was never dropped. (Links to the W1 P3 miss: RESTORE vs UNDROP, §7.)
