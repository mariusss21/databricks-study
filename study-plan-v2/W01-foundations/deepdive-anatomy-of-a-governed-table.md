# W1 · Friday deep dive — Anatomy of a governed table

**Fri Nov 20, 2026 · ~40–60 min · Ties together P1 (Delta/ACID), P2 (compute) and P3 (Unity Catalog)**
Exam sections touched: **§1 Databricks Intelligence Platform** · **§2 Data Ingestion and Loading** · **§3 Data Transformation and Modeling** · **§6 Troubleshooting, Monitoring, and Optimization** · **§7 Governance and Security**

> How to use this: read the scenario, then go step by step. Each step has a short "your call" prompt. Think of your answer before reading the explanation. The 6-question scenario check at the end has answers at the very bottom.

---

## The scenario

You join **Acme Retail** as a data engineer. A vendor drops daily **CSV order files** into an S3 bucket the vendor also writes to: `s3://acme-landing/vendor/orders/`. Your goal for the week:

1. Make those files reachable, with governance, from Databricks.
2. Load them into a **Bronze** Delta table, then a small **Gold** object for the analytics team.
3. Pick the right compute for each piece of work.
4. Survive a bad update without panic.
5. Know exactly what happens when things get dropped.

All code is SQL or Python. One table, followed from raw file to governed UC object.

```
s3://acme-landing/vendor/orders/*.csv        (vendor writes here)
        │   storage credential + external location
        ▼
/Volumes/main/landing/vendor_raw/orders/     (external volume, governed file access)
        │   read_files + CTAS / INSERT
        ▼
main.bronze.vendor_orders                    (managed Delta table)
        │   SQL
        ▼
main.gold.daily_vendor_revenue               (view for analysts)
```

---

## Step 1 — Make the files reachable, with governance `§7 Governance and Security` `§2 Ingestion`

**Your call:** the vendor keeps writing into their bucket. Do you copy the files somewhere, or govern them in place?

Govern them in place. The vendor is an **outside writer**, which is exactly the P3 rule for user-owned (external) storage. The chain is:

- **Storage credential**: wraps the IAM role that can read the bucket. Usually set up by an admin.
- **External location**: storage credential + a path (`s3://acme-landing/`). Privileges on it decide who can use the path.
- **External volume**: a UC object over a sub-path, so people work with `/Volumes/...` paths and UC privileges instead of raw S3 URIs.

```sql
-- Admin, once
CREATE EXTERNAL LOCATION IF NOT EXISTS acme_landing
  URL 's3://acme-landing/'
  WITH (STORAGE CREDENTIAL acme_landing_role);

-- Grant what the engineering group needs on that path
GRANT CREATE EXTERNAL VOLUME ON EXTERNAL LOCATION acme_landing TO `data-engineers`;

-- You
CREATE SCHEMA IF NOT EXISTS main.landing;
CREATE EXTERNAL VOLUME main.landing.vendor_raw
  LOCATION 's3://acme-landing/vendor/';

GRANT READ VOLUME ON VOLUME main.landing.vendor_raw TO `data-engineers`;

-- Look at the files
LIST '/Volumes/main/landing/vendor_raw/orders/';
```

In Python the same path works everywhere: `dbutils.fs.ls("/Volumes/main/landing/vendor_raw/orders/")` or `spark.read.csv("/Volumes/...")`.

**Why a volume and not a table here?** Volumes govern **files** (any format). Tables govern **tabular data**. You can't register volume files as a table. You *read* them into one (Step 3).

**Managed vs external volume** (same idea as tables):

| | Managed volume | External volume |
|---|---|---|
| Where files live | UC-managed storage for the schema | A path you chose, under an external location |
| `DROP VOLUME` | Files removed (7-day retention) | **Files stay** in cloud storage |
| Good for | Files your own pipelines create (e.g., exports, checkpoints) | Landing zones that outside systems write to |

Older note: [`explanations/unity-catalog-volumes-and-external-locations.md`](../../explanations/unity-catalog-volumes-and-external-locations.md).

---

## Step 2 — Pick compute for each job `§1 Databricks Intelligence Platform`

**Your call:** three pieces of work. Which compute for each?

| Work | Good fit | Why |
|---|---|---|
| Exploring the files in a notebook (Python + SQL) | **Serverless notebook compute** | Starts fast, no cluster to manage, you pay while it works |
| Analysts querying the Gold view from a BI tool / SQL editor | **SQL warehouse** (serverless) | Built for SQL and BI concurrency |
| Nightly load into Bronze | **Job on serverless compute** | Short, scheduled, no idle cost |
| A task that needs a **custom init script** or a **GPU** | **Classic compute** (jobs compute, dedicated access mode) | Serverless doesn't support custom init scripts or GPUs |

That last row is the one to remember. It's the same idea as Wednesday's W4: serverless limits are about **what you can configure** (init scripts, GPUs, many Spark configs), not about Python vs SQL. Python packages you need on serverless come from `%pip install` in the notebook or the environment settings.

And the P2 fundamental shows up here: storage (S3 + Delta) never moves. You can point **any** of these compute types at the same table because storage and compute are separate.

Older note: [`explanations/spark-parallelism-single-node-and-serverless.md`](../../explanations/spark-parallelism-single-node-and-serverless.md).

---

## Step 3 — Load Bronze as a managed Delta table `§2 Ingestion` `§3 Transformation`

**Your call:** the files are external. Should the Bronze table be external too?

No. The **files** have an outside owner. The **Bronze table** is something your pipeline creates, so let the platform own it: **managed** table (no `LOCATION`). You get predictive optimization, automatic cleanup, and `UNDROP` safety.

```sql
CREATE TABLE IF NOT EXISTS main.bronze.vendor_orders AS
SELECT
  *,
  current_timestamp() AS ingested_at
FROM read_files(
  '/Volumes/main/landing/vendor_raw/orders/',
  format => 'csv',
  header => true
);

DESCRIBE TABLE EXTENDED main.bronze.vendor_orders;   -- Type: MANAGED
```

Python equivalent:

```python
(spark.read.format("csv").option("header", True)
   .load("/Volumes/main/landing/vendor_raw/orders/")
   .withColumn("ingested_at", F.current_timestamp())
   .write.saveAsTable("main.bronze.vendor_orders"))   # no .option("path") → managed
```

A one-off CTAS is fine for a first load. Re-running it every night would re-read every file. Next week (W2) swaps this for **COPY INTO** and **Auto Loader**, which only pick up new files. Keep that in mind; it's the bridge to Monday.

---

## Step 4 — Delta keeps you safe when things go wrong `§1 Platform` `§6 Troubleshooting`

Three things happen on Wednesday:

**(a) The nightly job dies halfway through a write.** Analysts query the table at the same moment.
Nothing breaks. A Delta write only becomes visible when its commit lands in `_delta_log`. No commit → readers keep seeing the last good version. That's **atomicity + isolation** from P1.

**(b) A teammate runs a bad update:**

```sql
UPDATE main.bronze.vendor_orders SET amount = 0 WHERE vendor_id = 42;   -- oops, wrong vendor
```

**Your call:** how do you find it and fix it?

```sql
DESCRIBE HISTORY main.bronze.vendor_orders;          -- find the version before the UPDATE

SELECT * FROM main.bronze.vendor_orders VERSION AS OF 7
WHERE vendor_id = 42;                                -- check the old values

RESTORE TABLE main.bronze.vendor_orders TO VERSION AS OF 7;
```

`RESTORE` itself is a new commit, so history keeps the full story (and you could undo the restore).

**(c) Someone asks: "Can we time travel back 30 days?"**
Only if the old data files still exist. `VACUUM` removes unreferenced files older than the retention (default **7 days**). After that, older versions can't be read, even though they still show in `DESCRIBE HISTORY`.

Older notes: [`explanations/acid-on-delta.md`](../../explanations/acid-on-delta.md), [`explanations/delta-time-travel-and-retention.md`](../../explanations/delta-time-travel-and-retention.md).

---

## Step 5 — Give analysts a Gold object and the right grants `§3 Transformation` `§7 Governance`

The analytics team wants daily revenue per vendor. Freshness matters: they want to see today's numbers as soon as Bronze has them. The query is light.

**Your call:** plain view or materialized view?

A **plain view**. It stores no data and runs the query each time, so it's always as fresh as Bronze. A materialized view stores precomputed results and refreshes on a schedule or trigger, which helps for heavy queries but adds lag and a data copy. (This is your diagnostic Q6 coming back on purpose. 🙂)

```sql
CREATE SCHEMA IF NOT EXISTS main.gold;

CREATE OR REPLACE VIEW main.gold.daily_vendor_revenue AS
SELECT vendor_id,
       CAST(order_ts AS DATE) AS order_date,
       SUM(amount)            AS revenue
FROM main.bronze.vendor_orders
GROUP BY vendor_id, CAST(order_ts AS DATE);

-- Least privilege: analysts can see Gold, not Bronze
GRANT USE CATALOG ON CATALOG main          TO `analysts`;
GRANT USE SCHEMA  ON SCHEMA  main.gold     TO `analysts`;
GRANT SELECT      ON VIEW    main.gold.daily_vendor_revenue TO `analysts`;
```

Analysts need **USE CATALOG + USE SCHEMA + SELECT**. Missing either `USE` privilege blocks them even with `SELECT` on the object.

(In real life you'd clean Bronze into Silver first. That's Week 3; here we keep one table on purpose.)

---

## Step 6 — Lifecycle: what happens when things get dropped `§7 Governance`

Friday afternoon, a cleanup script runs. What happens in each case?

| Action | Result |
|---|---|
| `DROP VOLUME main.landing.vendor_raw` (external) | Volume metadata gone; **files stay** in `s3://acme-landing/vendor/` |
| `DROP TABLE main.bronze.vendor_orders` (managed) | Table gone, but `UNDROP TABLE` brings it back within **7 days** (data + history) |
| `DROP VIEW main.gold.daily_vendor_revenue` | Only the definition is lost; no data was stored |
| A legacy **external** Delta table that only Databricks writes to | `ALTER TABLE ... SET MANAGED` moves it under UC management and keeps name, grants, history |

```sql
UNDROP TABLE main.bronze.vendor_orders;
```

Remember: `RESTORE` fixes a table that still exists. `UNDROP` brings back a dropped one.

---

## What to remember (one screen)

- **Files with an outside owner** → external location + external volume. **Tables your pipeline creates** → managed.
- Volume path: `/Volumes/<catalog>/<schema>/<volume>/<path>`. Volumes govern files; they never act as tables.
- Serverless is the default; go classic when you need **custom init scripts, GPUs, or specific Spark configs**.
- Delta commit = visibility. Failed writes leave readers on the last good version.
- `DESCRIBE HISTORY` → `VERSION AS OF` → `RESTORE`. `VACUUM` (7 days by default) limits time travel.
- Plain view = always fresh, no copy. Materialized view = precomputed, refresh lag.
- Readers need `USE CATALOG` + `USE SCHEMA` + `SELECT`.
- Drop external = files stay. Drop managed = `UNDROP` for 7 days.

---

## Scenario check (6 questions, answers at the bottom)

**Q1.** `§7` The vendor will keep writing CSVs to their bucket. You want engineers to list and read those files by UC path, with UC privileges. What do you create?
- A) A managed volume in `main.landing`, then copy the files into it
- B) An external volume on a path under an external location
- C) An external table with `LOCATION` set to the vendor folder
- D) A storage credential granted directly to each engineer

**Q2.** `§1` A nightly Python task needs a custom init script that installs an OS-level package. Which compute fits?
- A) Serverless jobs compute with a larger environment
- B) A serverless SQL warehouse running the task as SQL
- C) Classic jobs compute with dedicated access mode
- D) Serverless notebook compute with `%pip install`

**Q3.** `§2` `§3` You load the vendor files with `CREATE TABLE main.bronze.vendor_orders AS SELECT ... FROM read_files('/Volumes/...')`. What kind of table is it?
- A) Managed, because no `LOCATION` was given
- B) External, because the volume is external
- C) External, because its source files sit in a volume
- D) Managed only after you run `SET MANAGED` on it

**Q4.** `§6` A job crashes halfway through an `INSERT` into a Delta table while analysts are querying it. What do analysts see?
- A) The last committed version, without the new rows
- B) The rows written before the crash, until a repair run
- C) An error until someone runs `FSCK REPAIR TABLE`
- D) A mix of old and new rows across the data files

**Q5.** `§3` Analysts want vendor revenue that always matches Bronze the moment they query it. The query is light. Which object fits best?
- A) A materialized view refreshed every hour
- B) A plain view over the Bronze table
- C) A managed table rebuilt by a nightly job
- D) A streaming table reading from Bronze

**Q6.** `§7` An analyst has `SELECT` on `main.gold.daily_vendor_revenue` but still gets a permission error. What's the most likely gap?
- A) `MODIFY` privilege granted on the view itself
- B) `READ VOLUME` on the vendor landing volume
- C) `USE CATALOG` and `USE SCHEMA` on its parents
- D) `CREATE TABLE` privilege on the `main.gold` schema

---

## Interview angle

*"Walk me through how you'd onboard a new vendor file feed in a lakehouse."*
Start with ownership: the vendor owns the files, so govern them in place (external location + volume) rather than copying. Load into a **managed** Bronze table so the platform handles layout, optimization and recovery. Use serverless for short jobs and SQL warehouses for BI; drop to classic only for specific needs like init scripts or GPUs. Expose a small Gold object with least-privilege grants. Close with safety: Delta gives atomic commits, history and `RESTORE`; managed tables give `UNDROP`; know that `VACUUM` bounds how far back you can go.

## Optional hands-on (15 min, sandbox only)

1. `LIST '/Volumes/<catalog>/<schema>/<volume>/';` on any volume you can already see (Catalog Explorer → a schema → Volumes). Just look.
2. In a schema you own:
   ```sql
   CREATE TABLE my_sandbox.dd_demo AS SELECT id, id * 10 AS amount FROM range(5);
   UPDATE my_sandbox.dd_demo SET amount = 0;
   DESCRIBE HISTORY my_sandbox.dd_demo;
   RESTORE TABLE my_sandbox.dd_demo TO VERSION AS OF 0;
   SELECT * FROM my_sandbox.dd_demo;          -- amounts are back
   CREATE VIEW my_sandbox.dd_view AS SELECT SUM(amount) AS total FROM my_sandbox.dd_demo;
   SELECT * FROM my_sandbox.dd_view;
   DROP VIEW my_sandbox.dd_view;
   DROP TABLE my_sandbox.dd_demo;
   ```
   Replace `my_sandbox` with a `catalog.schema` you own.

## Official docs

- [What are Unity Catalog volumes?](https://docs.databricks.com/aws/en/volumes/)
- [External locations](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/external-locations)
- [read_files table-valued function](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files)
- [Serverless compute limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)
- [Work with table history (time travel, RESTORE)](https://docs.databricks.com/aws/en/delta/history)
- [Unity Catalog privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges)
- [Exam guide](https://www.databricks.com/learn/certification/data-engineer-associate)

*Optional further reading (not homework):* [kengio guide: Data Engineer Associate topics](https://github.com/kengio/databricks-certification-study-guide/tree/main/certifications/data-engineer-associate)

---

## Scenario check answers

**Q1 → B (external volume).** The files have an outside writer, so govern them in place. A) A managed volume means copying, and the vendor would have to write into Databricks-managed storage. C) A table needs tabular data you query as a table, and doesn't give file-level access by `/Volumes` path. D) Storage credentials aren't granted to users for this; access goes through external locations and volumes.

**Q2 → C (classic, dedicated).** Serverless doesn't support custom init scripts, whatever the workload. A) A bigger environment doesn't add init scripts. B) A SQL warehouse runs SQL, not init scripts, and can't run this task. D) `%pip` installs Python packages, not OS-level setup from an init script.

**Q3 → A (managed).** No `LOCATION` (and no `.option("path")`) = managed. Where the source files came from doesn't matter. B) and C) mix up the source with the table's own storage. D) `SET MANAGED` is for converting tables that are already external.

**Q4 → A (last committed version).** Delta writes become visible only when the commit lands in the log. B) Partial writes are never visible. C) No repair is needed; the uncommitted files are simply ignored. D) Readers never mix versions; that's isolation.

**Q5 → B (plain view).** Always fresh, no data copy, fine for a light query. A) Hourly refresh means up to an hour of lag. C) Nightly rebuild means up to a day of lag. D) A streaming table is for incremental ingestion, and keeps its own copy of the data.

**Q6 → C (USE CATALOG + USE SCHEMA).** `SELECT` alone isn't enough; the parent containers need `USE` privileges. A) `MODIFY` is for changing data, not reading. B) The view reads a table, not the volume. D) `CREATE TABLE` is for making new objects.

---
*Reply with your answers (e.g. `Q1 B, Q2 C ...`). Misses go into Monday's Week 1 review, tagged by exam section. Have a restful weekend!*
