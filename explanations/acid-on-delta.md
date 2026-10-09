# ACID, explained simply, and how Delta Lake does it

## Like you're 5

Picture a box of LEGO that you and your friends share, plus a notebook next to it where every finished build gets written down. ACID is four promises about that box:

- **A is for All or nothing (Atomicity).** If you're building a castle and get called to dinner halfway through, nobody sees half a castle. Either the whole castle is in the box or none of it is.
- **C is for following the rules (Consistency).** The box has rules, like "only red bricks go in the red bin." If you try to put a blue brick there, it gets rejected, so the box never ends up in a broken state.
- **I is for not bumping into each other (Isolation).** When two kids build at the same time, each one sees the box as it was when they started and doesn't see the other's half-done work. If both want to change the same piece, one waits and tries again.
- **D is for it stays (Durability).** Once your castle is written in the notebook, it's there for good, even if the lights go out.

## How Delta does it

The notebook is the `_delta_log` folder, and each write adds the next numbered JSON file (`000…0.json`, `000…1.json`, and so on).

- **Atomicity.** Spark writes the new Parquet files first, where nobody can see them yet. The write becomes real only when the commit JSON file is created, and creating that file either succeeds completely or doesn't happen at all. If the job crashes before then, the orphan files are ignored and later cleaned up by `VACUUM`.
- **Consistency.** Before committing, Delta checks the schema (schema enforcement rejects mismatched columns) and any `NOT NULL` or `CHECK` constraints. A write that breaks them fails without changing the table.
- **Isolation.** Readers use a snapshot, meaning the latest committed version when the query starts, so they never see half-written data. Writers use optimistic concurrency: each one writes its files, then tries to claim the next version number. If someone else got there first, Delta checks whether the changes actually conflict. If they don't, it retries on top. If they do, the write fails with a concurrent-modification error. Writes are serializable, while reads get snapshot isolation.
- **Durability.** Data and log files live in cloud object storage (S3, ADLS, GCS), which is built not to lose them. Every committed version stays in the log, which is what makes `DESCRIBE HISTORY` and time travel (`VERSION AS OF`) possible until `VACUUM` removes old files.

## Exam detail

Delta's ACID guarantees cover **one table per transaction**. A job that writes two tables can succeed on one and fail on the other.
