# Evening recall · W2 Wed · Auto Loader, schema inference and schema evolution

**Wed Nov 25, 2026 · ~5 min · Exam Section 2 (Data Ingestion and Loading)**
Pill: [02-wed-auto-loader-and-schema-evolution.md](../02-wed-auto-loader-and-schema-evolution.md)

Answer from memory, without opening the pill. Rough or partial answers are perfect.

## Questions

**Q1. Brain dump.** Without looking, write down the 3-5 things you remember about Auto Loader and schema evolution.

**Q2. Explain or apply.** Your Python Auto Loader stream (`format("cloudFiles")`, JSON, `schemaLocation` set, no schema given, other options default) fails overnight with `UnknownFieldException` after the source team added a `coupon_code` field. The on-call engineer wants to rewrite the schema by hand. What's actually going on, and what would you do instead?

**Q3. Connect.** Yesterday's pill said `COPY INTO` is idempotent per file, not per row. Does switching that load to Auto Loader fix duplicate orders that arrive in two different files? Why or why not?

## Hints (only if stuck)

- Q1: Think how it finds files, what it remembers, what types it infers for JSON/CSV, the evolution modes, and where "unfit" values go.
- Q2: Which evolution mode is the default when you don't provide a schema, and what has already happened to the schema before the error?
- Q3: What does Auto Loader's checkpoint track: files or rows?

---

## Model answers (for grading; not sent in chat)

**Q1.** Any 3-5 of:
- Auto Loader is a streaming source (`cloudFiles`, or `read_files` in SQL) that finds new files and records them in a checkpoint (RocksDB), so each file is processed exactly once; works with `availableNow` as incremental batch.
- Schema inference samples up to 50 GB or 1,000 files and stores the schema (and its history) in `_schemas` under `cloudFiles.schemaLocation`; schema location ≠ checkpoint, one checkpoint per source.
- For JSON/CSV/XML the `cloudFiles` source infers every column as STRING unless `cloudFiles.inferColumnTypes = true`; `schemaHints` pins types for known columns; `read_files` infers real types by default.
- Evolution modes: `addNewColumns` (default with no schema; stops once with `UnknownFieldException`, restart continues), `rescue` (schema fixed, no failure, new data in `_rescued_data`), `failOnNewColumns` (fails until fixed), `none` (default with a provided schema; drops new fields unless `rescuedDataColumn` set). `addNewColumns` never changes existing column types and can't be used with a provided schema.
- `_rescued_data` holds unknown columns, type mismatches, case mismatches, plus source file path.
- Discovery: directory listing (default, cost grows with folder size) vs file notification; Databricks recommends file events (`cloudFiles.useManagedFileEvents`) for most workloads, especially large/growing locations.
- Design: evolve in Bronze, enforce in Silver/Gold; alert on `_rescued_data IS NOT NULL`.

**Q2.** Nothing is broken: with no schema provided the default is `addNewColumns`, so Auto Loader already merged `coupon_code` into the schema location and then stopped once by design. Just restart it (better: run it in a Lakeflow Job with retries, or in a pipeline, so the restart is automatic); `coupon_code` becomes a real column. Hand-editing isn't needed.

**Q3.** No. Auto Loader's checkpoint also guarantees exactly-once *per file*, so the same order in two different files still lands twice in Bronze. You still need dedup or `MERGE` on a business key into Silver. (Links to the Nov 24 recall partial miss: COPY INTO idempotent per file, not per row, §2.)
