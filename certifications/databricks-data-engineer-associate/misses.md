# Missed questions

Questions Mario missed, kept so later Monday reviews can bring them back. Six so far.

## Count by exam section

| Exam section | Misses |
|---|---|
| §1 Databricks Intelligence Platform | 1 |
| §2 Data Ingestion and Loading | 3 |
| §3 Data Transformation and Modeling | 1 |
| §4 Working with Lakeflow Jobs | 0 |
| §5 Implementing CI/CD | 0 |
| §6 Troubleshooting, Monitoring, and Optimization | 0 |
| §7 Governance (Unity Catalog) | 1 |
| **Total** | **6** |

## All misses

| Date | Piece | Question | Exam section | His answer | Correct | Key idea | Revisited in |
|---|---|---|---|---|---|---|---|
| 2026-11-16 | [W1 diagnostic](W01-foundations/00-mon-diagnostic.md) | Q6 Gold object for a light query that must be fresh | §3 Data Transformation and Modeling | Materialized view | Plain view | Light + must be fresh -> plain view (no lag, no copy) | [W2 Mon review Q10 (flip side)](W02-ingestion/00-mon-review.md) |
| 2026-11-18 | [W1 Wed pill P2](W01-foundations/02-wed-compute-and-storage-compute-separation.md) | Warm-up W4 compute for custom init scripts | §1 Databricks Intelligence Platform | D (any compute) | C (classic compute) | Serverless can't run custom init scripts | [W2 Mon review Q11](W02-ingestion/00-mon-review.md) |
| 2026-11-22 | [W1 P3](W01-foundations/03-thu-unity-catalog-managed-vs-external-tables.md) | Warm-up W3 recover a dropped managed table | §7 Governance (Unity Catalog) | RESTORE | UNDROP | RESTORE rolls back an existing table; UNDROP brings back a dropped one within 7 days | [W2 Mon review Q7 (flip side)](W02-ingestion/00-mon-review.md) |
| 2026-11-24 | [W2 Tue recall](W02-ingestion/recall/recall-01-tue-batch-streaming-incremental-copy-into.md) | Can `COPY INTO` retries create duplicates? (partial miss) | §2 Data Ingestion and Loading | No, retries can't duplicate | Yes, across files or with `force` | `COPY INTO` is idempotent per file, not per row: dedup or `MERGE` into Silver. Also: batch vs streaming = bounded vs unbounded data, not frequency | W3 Mon review (Nov 30), planned |
| 2026-11-26 | [W2 Wed pill P2 (Auto Loader)](W02-ingestion/02-wed-auto-loader-and-schema-evolution.md) | Warm-up W1: new JSON field, schemaLocation set, no schema given, default options | §2 Data Ingestion and Loading | C (lands in _rescued_data, keeps running) | B (stops once, restart loads new column) | Default with no schema is addNewColumns: schema updated, UnknownFieldException once, restart continues; rescue must be set explicitly | W3 Mon review (Nov 30) |
| 2026-11-26 | [W2 Wed pill P2 (Auto Loader)](W02-ingestion/02-wed-auto-loader-and-schema-evolution.md) | Warm-up W2: why amount is STRING in cloudFiles CSV stream | §2 Data Ingestion and Loading | D (only infers numerics from files over 1,000 rows) | A (CSV/JSON read as strings by default) | cloudFiles infers JSON/CSV/XML as strings unless cloudFiles.inferColumnTypes=true or schemaHints; 1,000 is the file sampling limit, not a row threshold | W3 Mon review (Nov 30) |
