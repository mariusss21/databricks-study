# Evening recall · W2 Thu · Lakeflow Connect, managed connectors and CDC

**Thu Nov 26, 2026 · ~5 min · Exam Section 2 (Data Ingestion and Loading)**
Pill: [03-thu-lakeflow-connect-managed-connectors.md](../03-thu-lakeflow-connect-managed-connectors.md)

Answer from memory, without opening the pill. Rough or partial answers are perfect.

## Questions

**Q1. Brain dump.** Without looking, write down the 3-5 things you remember about Lakeflow Connect and managed connectors.

**Q2. Explain or apply.** A Python Airflow task pulls a SQL Server `orders` table every night with `SELECT * FROM orders WHERE updated_at > :last_value` and appends the result to a Delta table. Orders that were deleted in SQL Server still show up in Databricks weeks later. Why does this happen, and what would you do instead?

**Q3. Connect.** On Tuesday you learned that `COPY INTO` is idempotent per file, not per row, so the same order in two nightly CSV exports lands twice. If a managed SQL Server connector (SCD type 1) loads that `orders` table instead, how many rows do you get for an order that changed status three times, and why?

## Hints (only if stuck)

- Q1: Think about the three layers (managed, standard, DIY), the building blocks (connection, pipeline, gateway), the history setting, and when *not* to copy the data at all.
- Q2: Can a `WHERE updated_at > ...` query ever see a row that no longer exists? Which kind of CDC reads the database's own log?
- Q3: Does the connector append what it reads, or apply each change to a row by its key?

---

## Model answers (for grading; not sent in chat)

**Q1.** Any 3-5 of:
- Lakeflow Connect has three layers, most managed first: managed connectors (SaaS like Salesforce/Workday, databases like SQL Server/MySQL/PostgreSQL), standard connectors (Auto Loader, `read_files`, `COPY INTO`, streaming from cloud storage/Kafka/SFTP), and DIY/partner tools. Rule: use the most managed option that supports the source.
- Building blocks: a Unity Catalog **connection** (credentials, governed by grants like `USE CONNECTION`), a serverless **ingestion pipeline** that writes **streaming tables**, and for **database connectors only** an **ingestion gateway** plus **staging volume**. Each schedule auto-creates a Lakeflow Job.
- First run loads everything; later runs are incremental (CDC, change tracking or a cursor column). Failures retry with exponential backoff and resume from the last cursor.
- History tracking: SCD type 1 is the default (overwrite); SCD type 2 keeps versions with `__START_AT` / `__END_AT`; a full refresh wipes SCD 2 history.
- Schema changes: new/dropped columns handled; type changes aren't; a rename needs a full refresh.
- Extraction fundamentals: full snapshot vs query-based cursor (misses hard deletes) vs log-based CDC (catches deletes, low source load, DBA must enable it).
- Ingest vs federate: "must not be copied" means Lakehouse Federation, not a connector. Local file upload UI is capped at 5 GB per file.

**Q2.** A query-based (cursor / high-watermark) extraction only sees rows that still exist, so hard deletes are invisible and the deleted orders never leave the Delta table (and appends also pile up versions). Replace it with a managed SQL Server connector using log-based CDC or change tracking (the DBA enables it), which captures inserts, updates and deletes and applies them to a streaming table; pick SCD type 1 or 2 depending on whether history is needed. That also removes the DIY ownership cost (retries, schema drift).

**Q3.** One current row (with SCD type 1), because the connector applies each change to the row by its primary key (an upsert from CDC) instead of appending whatever arrived, so later changes overwrite the earlier status. (With SCD type 2 you'd get one row per version, with only the latest having `__END_AT IS NULL`.) Links to the Nov 24 recall partial miss: COPY INTO idempotent per file, not per row (§2).
