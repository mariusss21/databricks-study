# W1 · P3 — Unity Catalog: managed vs external tables

**Thu Nov 19, 2026 · ~15–20 min · Exam Section 7 (Data Governance), with a nod to Section 1 (Databricks Intelligence Platform)**
⚙ Exam topic: Unity Catalog object model, managed vs external tables, external locations, DROP / UNDROP / SET MANAGED
🧱 Fundamental: metadata vs data, and who owns a table's lifecycle

---

## 0) Warm-up (answer before reading; answers in §5)

**W1.** `main.bronze.vendor_feed` was created with `LOCATION 's3://acme-landing/vendor_feed/'`. Someone runs `DROP TABLE main.bronze.vendor_feed`. What happens to the data files?
- A) They are deleted right away, along with the metadata
- B) They are kept for 7 days and then deleted
- C) They stay in S3; only the metadata goes away
- D) They are moved into the schema's managed storage

**W2.** An engineer can create managed tables in `main.bronze`, but `CREATE TABLE ... LOCATION 's3://acme-landing/x/'` fails with a permission error. What is most likely missing?
- A) MODIFY on the main.bronze schema and its tables
- B) USE CATALOG on main plus USE SCHEMA on bronze
- C) READ FILES granted directly on the storage credential
- D) CREATE EXTERNAL TABLE on the external location

**W3.** Yesterday someone dropped the managed table `main.silver.customers` by mistake. What's the quickest correct fix?
- A) `RESTORE TABLE` to the last version before the drop
- B) `UNDROP TABLE main.silver.customers`
- C) Recreate it with `CREATE TABLE ... LOCATION` on the old path
- D) Query it with `TIMESTAMP AS OF` yesterday and rewrite it

**W4.** You have an external Delta table that only Databricks jobs write to. You want predictive optimization on it and want to keep its name, grants and history. What should you do?
- A) Run `ALTER TABLE ... SET MANAGED` on it
- B) `DEEP CLONE` it into a new managed table
- C) Turn on predictive optimization for its schema
- D) Schedule a nightly `OPTIMIZE ... ZORDER` job

---

## 1) The idea in one breath

A table is two things: **metadata** (name, schema, grants, history pointer) and **data files** in object storage. Unity Catalog always owns the metadata. The only question is who owns the files: with a **managed** table, Unity Catalog owns them too (location, layout, cleanup); with an **external** table, you own them at a path you chose, and Unity Catalog just governs access.

## 2) The fundamental (tool-agnostic)

**Catalog vs storage.** Every analytical system keeps a catalog (a metastore) separate from the data: Hive Metastore, AWS Glue, Snowflake's metadata layer, Iceberg catalogs. The catalog answers *"what tables exist, what columns, who can read them, where are the files?"* The storage holds the bytes.

**Lifecycle ownership** is the real design choice:

| | System-owned ("managed") | User-owned ("external") |
|---|---|---|
| Who picks the path | The system | You |
| Who cleans up on delete | The system | You |
| Who can optimize the layout | The system, automatically | You, manually |
| Other engines writing files directly | Not supported (go through the catalog) | Possible, but bypasses governance |
| Typical use | New tables you build | Data that already lives somewhere, or is shared at the file level |

Rule of thumb from data architecture: **let the platform own what the platform creates.** Keep user-owned storage for data with an outside owner or outside writers.

**Namespaces.** Most modern catalogs use a hierarchy so you can grant and organize at any level. In Unity Catalog it's three levels: `catalog.schema.object` (for example `main.silver.customers`), all under one **metastore** per region.

## 3) How Databricks does it

**Object model (exam-friendly picture):**

```
metastore (one per region, attached to workspaces)
└── catalog            e.g. main, dev, prod
    └── schema         e.g. bronze, silver, gold
        ├── tables     managed or external
        ├── views      (plain, materialized)
        ├── volumes    governed files (W2 uses them for ingestion)
        └── functions
storage credential  →  external location (credential + cloud path)
```

- A **storage credential** wraps cloud access (an IAM role / managed identity).
- An **external location** = storage credential + a cloud path. Privileges on it (`CREATE EXTERNAL TABLE`, `READ FILES`, `WRITE FILES`) decide who can use that path.
- **Managed storage** can be set at metastore, catalog or schema level with `MANAGED LOCATION`. Managed tables land in the closest one.

**Managed vs external, side by side:**

| | Managed | External |
|---|---|---|
| How you create it | `CREATE TABLE` with **no** `LOCATION` (the default) | `CREATE TABLE ... LOCATION '<path>'` under an external location |
| Formats | Delta (Iceberg also supported) | Delta, Parquet, CSV, JSON, Avro, ORC, text |
| `DROP TABLE` | Metadata removed; data recoverable with `UNDROP` for **7 days** by default, then deleted | Metadata only; **files stay** |
| Predictive optimization, automatic liquid clustering | Yes | No |
| Databricks recommendation | **Default for most tables** | Existing data, or outside engines that need direct file access |

```sql
-- Managed storage for a catalog (optional; otherwise the metastore default is used)
CREATE CATALOG IF NOT EXISTS sales MANAGED LOCATION 's3://acme-uc/sales/';
CREATE SCHEMA IF NOT EXISTS sales.silver;

-- Managed table: no LOCATION
CREATE TABLE sales.silver.customers (
  customer_id BIGINT,
  email       STRING,
  country     STRING
);

-- External table: needs CREATE EXTERNAL TABLE on an external location covering this path
CREATE TABLE sales.bronze.vendor_feed (id BIGINT, payload STRING)
LOCATION 's3://acme-landing/vendor_feed/';

-- Which one is it?  Look at "Type" (MANAGED / EXTERNAL) and "Location"
DESCRIBE TABLE EXTENDED sales.bronze.vendor_feed;

-- Oops, dropped a managed table
DROP TABLE sales.silver.customers;
UNDROP TABLE sales.silver.customers;      -- within the recovery window (7 days by default)

-- Bring an external Delta table under UC management (keeps name, grants, history)
ALTER TABLE sales.bronze.vendor_feed SET MANAGED;
-- Changed your mind? Roll back within 14 days
ALTER TABLE sales.bronze.vendor_feed UNSET MANAGED;
```

In Python it's the same rule: `df.write.saveAsTable("sales.silver.customers")` makes a **managed** table; adding `.option("path", "s3://...")` makes it **external**.

Your older notes go deeper on the storage side: [`explanations/unity-catalog-volumes-and-external-locations.md`](../../../explanations/unity-catalog-volumes-and-external-locations.md) and [`explanations/platform-delta-unity-catalog-compute.md`](../../../explanations/platform-delta-unity-catalog-compute.md).

## 4) Tradeoffs and exam traps

- **Trap: "DROP deletes everything."** Only for managed tables (after the UNDROP window). External `DROP` removes metadata; you delete the files yourself.
- **Trap: "LOCATION is just a setting."** Adding `LOCATION` (or `option("path")`) is exactly what makes a table external.
- **Trap: permission errors on LOCATION.** Creating managed tables needs `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`. External ones also need **`CREATE EXTERNAL TABLE` on the external location**.
- **Trap: RESTORE vs UNDROP.** `RESTORE` rolls back the *data version* of a table that still exists. A dropped table needs `UNDROP`. And `RESTORE` never changes managed ↔ external; `UNSET MANAGED` does.
- **Trap: predictive optimization on external tables.** It runs on **managed** tables only.
- **Trap: SET MANAGED side effects.** Writers pause briefly during the switch, and running streams on that table must be restarted afterwards. It needs a recent runtime (17.3 LTS+) or serverless.
- **Tradeoff:** managed = less control over paths, but automatic optimization, simpler cleanup, safer drops. External = full control of files and outside access, but you own cleanup, layout and the risk of writes bypassing UC.

## 5) Warm-up answers

**W1 → C (files stay, metadata goes).** `LOCATION` made it external, so `DROP` only removes it from the catalog. A) Immediate file deletion doesn't happen for either type. B) The 7-day window is the `UNDROP` rule for *managed* tables. D) Nothing moves on a drop; moving data into managed storage is what `SET MANAGED` does.

**W2 → D (CREATE EXTERNAL TABLE on the external location).** They can already create managed tables, so the parent privileges are there; only the path permission is missing. A) MODIFY is for changing data in existing tables, not creating tables on a path. B) They already have these (otherwise managed tables would fail too). C) Grants for paths go on the *external location*, and `READ FILES` allows reading files, not registering a table.

**W3 → B (UNDROP).** A dropped managed table can be brought back with `UNDROP TABLE` inside the recovery window (7 days by default), with its data and history. A) `RESTORE` works on a table that still exists. C) Pointing `LOCATION` at managed storage paths isn't how you recover, and it would create an external table. D) Time travel also needs the table to exist.

**W4 → A (SET MANAGED).** It converts in place and keeps the name, grants and history, and managed tables get predictive optimization. B) A deep clone works but gives you a new table, so you'd redo grants and swap names. C) Predictive optimization skips external tables, whatever the schema setting. D) Manual `OPTIMIZE` helps layout but isn't predictive optimization and doesn't change the table type.

## 6) Interview angle

*"When would you choose an external table over a managed one?"*
Default to managed: the platform handles paths, cleanup and optimization, and drops are recoverable. Choose external when the data has an owner or writer outside Databricks (another engine writes the files, or a team must read raw files from the bucket), or when you're registering an existing data lake folder. Mention the migration path: register as external first, then `SET MANAGED` once only Databricks writes to it. End with the fundamental: the catalog always governs metadata; the choice is about who owns the files' lifecycle.

## 7) Optional hands-on (10 min)

1. In **Catalog Explorer**, open any catalog you can see and walk down to a schema. Check a table's **Details** tab for *Type* and *Storage location*.
2. In a sandbox schema you own, run:
   ```sql
   CREATE TABLE my_sandbox.p3_demo AS SELECT 1 AS id;
   DESCRIBE TABLE EXTENDED my_sandbox.p3_demo;   -- Type should be MANAGED
   DROP TABLE my_sandbox.p3_demo;
   UNDROP TABLE my_sandbox.p3_demo;
   SELECT * FROM my_sandbox.p3_demo;             -- it's back
   DROP TABLE my_sandbox.p3_demo;                -- clean up
   ```
   (Replace `my_sandbox` with a `catalog.schema` you own. Skip the external part unless you already have an external location.)
3. Run `SHOW EXTERNAL LOCATIONS;` to see whether your workspace has any. Just look; don't create one.

## Official docs

- [What is Unity Catalog?](https://docs.databricks.com/aws/en/data-governance/unity-catalog/)
- [Unity Catalog managed tables](https://docs.databricks.com/aws/en/tables/managed)
- [Work with external tables](https://docs.databricks.com/aws/en/tables/external)
- [External locations](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/external-locations)
- [UNDROP TABLE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-undrop-table)
- [Convert external tables to managed](https://docs.databricks.com/aws/en/tables/convert-external-managed)
- [Exam guide](https://www.databricks.com/learn/certification/data-engineer-associate)

*Optional further reading (not homework):* [kengio guide: Managed and External Tables](https://github.com/kengio/databricks-certification-study-guide/blob/main/certifications/data-engineer-associate/07-governance-and-security/02-managed-and-external-tables.md)

---
*Reply with your warm-up answers (e.g. `W1 B, W2 C ...`). Any misses go into Monday's review. Tomorrow: the Friday deep dive tying Week 1 together.*
