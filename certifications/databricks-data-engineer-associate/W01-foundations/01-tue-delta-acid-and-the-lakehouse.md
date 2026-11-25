# W1 · P1 — Delta Lake, ACID and the lakehouse

**Tue Nov 17, 2026 · ~15–20 min · Exam Section 1 (Databricks Intelligence Platform), with links to Section 6 (optimization)**
⚙ Exam topic: Delta Lake transaction log, ACID, time travel
🧱 Fundamental: warehouse vs data lake vs lakehouse; what ACID really guarantees

---

## 0) Warm-up (answer before reading; answers in §5)

**W1.** A data lake stores Parquet files in cloud object storage. Two jobs write to the same folder at the same time and a reader sees half of one job's files. Which property was missing?
- A) Durability
- B) Atomicity / isolation
- C) Elasticity
- D) Columnar compression

**W2.** Where does a Delta table record which data files make up each version?
- A) In the Hive metastore only
- B) In the Parquet file footers
- C) In the `_delta_log` folder next to the data files
- D) In the cluster's driver memory

**W3.** You ran a bad `UPDATE` on `sales.orders` an hour ago. What is the quickest way to put the whole table back the way it was?
- A) `VACUUM sales.orders RETAIN 0 HOURS`
- B) `RESTORE TABLE sales.orders TO VERSION AS OF <version before the update>`
- C) `OPTIMIZE sales.orders`
- D) `DROP TABLE sales.orders` and re-run ingestion

**W4.** After `VACUUM sales.orders` with the default retention, which time-travel query is most likely to **fail**?
- A) Reading the current version
- B) Reading the version from 1 hour ago
- C) Reading the version from 3 days ago
- D) Reading the version from 15 days ago

---

## 1) The idea in one breath

A lakehouse is cheap, open files in object storage **plus** a transaction log that turns a pile of files into a real table: atomic commits, consistent snapshots, isolation between writers, durability, and a version history you can query (time travel). On Databricks that log is **Delta Lake**.

## 2) The fundamental (tool-agnostic)

| | Data warehouse | Data lake | Lakehouse |
|---|---|---|---|
| Storage | Proprietary, coupled to the engine | Open files (Parquet, JSON, CSV) in object storage | Open files in object storage |
| Transactions | Full ACID | None. "Table" = a folder | ACID through a table format's log (Delta, Iceberg, Hudi) |
| Schema | Schema-on-write | Usually schema-on-read | Schema enforced on write, can evolve |
| Workloads | BI / SQL | ML, raw data, cheap retention | BI + ML + streaming on one copy |
| Weak spot | Cost, lock-in, poor fit for unstructured data | "Data swamp": no quality, no consistency | Needs maintenance (compaction, cleanup) |

**What ACID actually guarantees** (and what it doesn't):
- **Atomicity** — a write fully lands or doesn't land at all. No half-written results visible.
- **Consistency** — every commit moves the table from one valid state to another (schema, constraints).
- **Isolation** — concurrent readers and writers don't see each other's partial work. Readers get a *snapshot*.
- **Durability** — once committed, it survives crashes (here: it's in object storage).

ACID does **not** mean "the data is correct". A perfectly atomic commit of wrong numbers is still wrong. Data quality is a separate layer (expectations and constraints, W3).

The core trick in every lakehouse format: **files are immutable; the log decides which files are "in" the table**. An update writes new files and a log entry that says "remove old file X, add new file Y". Readers follow the log, so they never see a half-done change.

## 3) How Databricks does it

- Each table folder has a `_delta_log/` with numbered JSON commits (`000…000.json`, `000…001.json`, …). Each commit lists `add`/`remove` file actions plus metadata. Periodic **checkpoint** files (Parquet) summarise the log so readers don't replay every JSON.
- Concurrent writers use **optimistic concurrency control**: each writer prepares its files, then tries to commit the next version number. If a conflicting commit got there first, Delta checks for a real conflict and either retries or fails with a concurrent-modification error.
- Reads use **snapshot isolation**: a query pins a version and reads exactly that set of files.
- Every commit is a **version** → time travel and restore.

```sql
-- See the versions (operation, user, timestamp, metrics)
DESCRIBE HISTORY sales.orders;

-- Time travel: read an older snapshot
SELECT * FROM sales.orders VERSION AS OF 12;
SELECT * FROM sales.orders TIMESTAMP AS OF '2026-11-17T08:00:00';

-- Undo a bad change (RESTORE itself becomes a new version)
RESTORE TABLE sales.orders TO VERSION AS OF 12;

-- Clean up data files no longer referenced by recent versions (default: older than 7 days)
VACUUM sales.orders;
```

Two retention knobs to keep apart:
- `delta.deletedFileRetentionDuration` (default **7 days**) — how long removed data files stay around before `VACUUM` may delete them. This is what limits time travel in practice.
- `delta.logRetentionDuration` (default **30 days**) — how long the history (log entries) is kept.

Your older notes go deeper on both: [`explanations/acid-on-delta.md`](../../../explanations/acid-on-delta.md) and [`explanations/delta-time-travel-and-retention.md`](../../../explanations/delta-time-travel-and-retention.md). For the platform overview: [`explanations/platform-delta-unity-catalog-compute.md`](../../../explanations/platform-delta-unity-catalog-compute.md).

## 4) Tradeoffs and exam traps

- **Trap: "VACUUM deletes old versions of the log."** No: it deletes *data files* that are no longer referenced and older than the retention. History rows can still appear in `DESCRIBE HISTORY`, but time travel to those versions fails because the files are gone.
- **Trap: `RETAIN 0 HOURS`.** Blocked by a safety check by default for a reason: it can break time travel and running readers/streams. Not a "fix".
- **Trap: RESTORE rewrites history.** No: it adds a *new* version whose content matches the old one. History stays intact.
- **Trap: "Delta is a file format."** Data files are Parquet; Delta is Parquet **plus** the transaction log (an open table format).
- **Trap: ACID = data quality.** ACID protects *how* data lands, not *whether it is right*.
- **Tradeoff:** many small commits (e.g. frequent tiny streaming writes) → many small files and log entries → maintenance needed (`OPTIMIZE`, predictive optimization, liquid clustering, W4).
- **Tradeoff:** longer retention = more time travel, but more storage cost.

## 5) Warm-up answers

**W1 → B (atomicity / isolation).** The reader saw a partial write (no atomicity) and concurrent writers interfered (no isolation). A) Durability is about surviving crashes after commit, not partial visibility. C) Elasticity is a compute scaling property. D) Compression affects size and speed, not correctness.

**W2 → C (`_delta_log`).** The log lives alongside the data files and is the source of truth for which files belong to each version. A) A metastore / Unity Catalog stores the table's name, location and permissions, not the per-version file list. B) Parquet footers hold per-file schema and stats only; a file doesn't know whether it's still "in" the table. D) Driver memory is temporary; the log has to be durable and shared.

**W3 → B (`RESTORE … VERSION AS OF`).** Brings the table back to the old snapshot in one command and records it as a new version. A) VACUUM with 0 hours removes old files, which *destroys* your ability to go back. C) OPTIMIZE compacts files; it doesn't change data. D) Works eventually, but slow, risky and loses any other history.

**W4 → D (15 days ago).** With the default 7-day `deletedFileRetentionDuration`, VACUUM may remove files only needed by versions older than 7 days. A–C are inside the retention window, so their files are kept.

## 6) Interview angle

*"Why would you pick a lakehouse over a warehouse or a plain data lake?"*
A strong 60-second answer: one copy of data in open formats on cheap storage (lake economics, no lock-in), with warehouse guarantees from a transaction log: ACID commits, schema enforcement, time travel for audits and rollbacks. That lets BI, streaming and ML share the same governed tables. Then name the cost honestly: you own maintenance (compaction, vacuum, retention policy), and ACID is per table, so cross-table consistency still needs pipeline design.

## 7) Optional hands-on (10 min, any SQL warehouse or serverless notebook)

```sql
CREATE TABLE main.default.p1_demo (id INT, status STRING);
INSERT INTO main.default.p1_demo VALUES (1,'new'),(2,'new');
UPDATE main.default.p1_demo SET status = 'oops';
DESCRIBE HISTORY main.default.p1_demo;          -- note the version numbers
SELECT * FROM main.default.p1_demo VERSION AS OF 1;
RESTORE TABLE main.default.p1_demo TO VERSION AS OF 1;
DESCRIBE HISTORY main.default.p1_demo;          -- RESTORE is a new version
DROP TABLE main.default.p1_demo;
```
(Swap `main.default` for a catalog/schema you can write to.)

## Official docs

- [What is Delta Lake?](https://docs.databricks.com/aws/en/delta/)
- [ACID guarantees on Databricks](https://docs.databricks.com/aws/en/lakehouse/acid)
- [Work with table history (time travel)](https://docs.databricks.com/aws/en/delta/history)
- [RESTORE](https://docs.databricks.com/aws/en/sql/language-manual/delta-restore)
- [VACUUM](https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum)
- [Isolation levels and write conflicts](https://docs.databricks.com/aws/en/optimizations/isolation-level)
- [Exam guide](https://www.databricks.com/learn/certification/data-engineer-associate)

---
*Reply with your warm-up answers (e.g. `W1 B, W2 C ...`) and any misses go into Monday's review.*
