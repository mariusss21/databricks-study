# W2 · P3 — Lakeflow Connect: managed connectors, CDC and picking an ingestion path

**Thu Nov 26, 2026 · ~15–20 min · Exam Section 2 (Data Ingestion and Loading)**
⚙ Exam topic: Lakeflow Connect managed connectors (SaaS and databases), standard connectors, local file upload, and choosing an ingestion method
🧱 Fundamental: full vs incremental extraction, query-based vs log-based CDC, build vs buy, and ingest vs federate

---

## 0) Warm-up (answer before reading; answers in §5)

**W1.** Sales wants Salesforce objects landed as Unity Catalog tables every day, with the least code and upkeep possible. Which option fits?
- A) A managed Salesforce connector in Lakeflow Connect
- B) A Python notebook that pages through the REST API
- C) Auto Loader reading Salesforce CSV exports on a volume
- D) A Lakehouse Federation foreign catalog over Salesforce

**W2.** In the standard (gateway-based) setup of a SQL Server connector, which piece exists for database connectors but not for SaaS ones?
- A) A Unity Catalog connection that holds the credentials
- B) An ingestion gateway and its staging volume
- C) Streaming tables as the destination tables
- D) A Lakeflow Job created for each schedule

**W3.** A managed connector loads `customers` with history tracking **off**. A customer moves from Recife to Natal in the source. What does the next run do?
- A) It adds a new row and marks the old one inactive
- B) It keeps both and puts Natal in `_rescued_data`
- C) It updates the row in place, and Recife is gone
- D) It fails until someone runs a full refresh

**W4.** Analysts query an operational PostgreSQL database a few times a month. The data must **not** be copied into Databricks. What fits?
- A) A managed PostgreSQL connector run on a schedule
- B) Lakehouse Federation through a foreign catalog
- C) A JDBC read in a notebook saved to a Delta table
- D) Auto Loader over nightly `pg_dump` files on a volume

---

## 1) The idea in one breath

Tuesday and Wednesday were about **files** (COPY INTO, Auto Loader). Lakeflow Connect covers everything else: **apps and databases**. A managed connector is "someone else wrote and maintains the scraper": you give it a Unity Catalog connection, pick tables, and it runs a serverless pipeline that lands them as streaming tables, incrementally, with retries and schema changes handled for you.

## 2) The fundamental (tool-agnostic)

**How you pull data out of a source: three ways**

| Way | How it finds changes | Catches deletes? | Load on the source |
|---|---|---|---|
| Full snapshot | Doesn't; re-reads everything | Yes (by comparing) | High, grows with table size |
| Query-based (cursor / high-watermark) | `WHERE updated_at > last_value` | **No**, a deleted row just disappears | Medium, one query per run |
| Log-based CDC | Reads the database's transaction log | Yes | Low, reads the log, not the tables |

The cursor approach is what most hand-written extractors do (likely some of your Airflow DAGs). It only works if the cursor column is reliable, and it never sees hard deletes. **Log-based CDC** is the gold standard, but the DBA has to turn it on (CDC or change tracking in SQL Server, binlog in MySQL, logical replication in Postgres).

**Applying the changes: overwrite or keep history.** Once you have a change, you either overwrite the row (**SCD type 1**: latest value only) or close the old version and add a new one (**SCD type 2**: full history). Next week's deep dive goes deep on SCDs; today, just know the connector asks you to choose.

**Build vs buy.** Every connector you write yourself means owning auth, pagination, rate limits, retries, schema drift and API version changes forever. The rule of thumb: **use the most managed option that supports your source, and drop down a layer only when it can't.**

**Ingest vs federate.** Ingesting makes a copy (fast queries, but it can go stale and costs storage). Federation queries the source where it lives (no copy, but every query hits the source system). "Must not be copied" means federation, not a connector.

Related older notes: [Unity Catalog volumes and external locations](../../../explanations/unity-catalog-volumes-and-external-locations.md) (connections are another UC securable, like external locations), and [the medallion architecture](../../../explanations/medallion-architecture.md) (managed connectors feed Bronze).

## 3) How Databricks does it

**Three layers, most managed first:**

| Layer | Sources | You write |
|---|---|---|
| **Managed connectors** | SaaS (Salesforce, Workday, HubSpot, Jira…) and databases (SQL Server, MySQL, PostgreSQL, Oracle…) | Almost nothing: choose tables, schedule, SCD type |
| **Standard connectors** (SQL and Spark APIs) | Cloud object storage, Kafka/Kinesis/Pub/Sub, SFTP | Auto Loader, `read_files`, streaming tables, `COPY INTO`, Structured Streaming |
| **DIY / partners** | Anything else | Your own code (JDBC, REST), or a partner tool (Fivetran etc.) |

Plus **local files**: the *Add or upload data* UI uploads files to a volume or creates a managed table from them (UI limit **5 GB per file**; use the Python SDK for bigger ones).

**Building blocks of a managed connector**

- **Connection**: a Unity Catalog securable holding the endpoint and credentials. Governed with grants like any other object.
- **Ingestion pipeline**: a Lakeflow pipeline on **serverless** compute that writes **streaming tables** in the catalog and schema you choose.
- **Database connectors only**: an **ingestion gateway** that reads changes continuously (in its own job) into a **staging volume**, which the pipeline then applies. (SQL Server also has a newer "integrated CDC" mode that folds this into one pipeline, but it still stages data.)
- **Schedules**: each schedule you add auto-creates a **Lakeflow Job** with the pipeline as a task.

**How it behaves**

- **First run** loads everything you selected; later runs ingest only changes (CDC, change tracking, or a cursor column, depending on the source).
- **History tracking**: `SCD_TYPE_1` is the default (overwrite). `SCD_TYPE_2` keeps versions with `__START_AT` / `__END_AT`. A **full refresh** rebuilds the table and wipes that history.
- **Failures** retry automatically with exponential backoff. If you must step in (say, expired credentials), the connector keeps its last cursor position and resumes from there.
- **Schema changes** (SQL Server, for example): new and dropped columns are handled automatically; type changes aren't, and a column rename needs a full refresh.
- **Interfaces**: UI, Declarative Automation Bundles, API, SDK, CLI. Connectors whose only auth is browser OAuth (e.g. HubSpot, Slack) need a one-time interactive sign-in to create the connection.

**SQL you'd actually touch**

```sql
-- 1) Admin creates the connection once (secrets, never plain passwords)
CREATE CONNECTION sqlserver_erp TYPE sqlserver
OPTIONS (
  host 'erp-db.internal', port '1433',
  user secret('erp', 'user'), password secret('erp', 'password')
);
GRANT USE CONNECTION ON CONNECTION sqlserver_erp TO `data-eng`;

-- 2) The gateway + pipeline are created in the UI or a bundle (scd_type, schedule).

-- 3) Downstream, it's just a streaming table. With SCD type 2, current rows are:
SELECT * FROM main.bronze_erp.customers
WHERE __END_AT IS NULL;
```

The pipeline settings (tables, `scd_type: SCD_TYPE_2`, `sequence_by`) live in the UI or in bundle YAML, not in SQL, which is why exam questions on this topic are more about **choosing** and **components** than syntax.

## 4) Tradeoffs and exam traps

- **Trap: "least operational overhead" + SaaS or database source.** That's a managed connector. The same phrase + files in cloud storage = Auto Loader.
- **Trap: gateway everywhere.** The ingestion gateway and staging volume belong to **database** connectors. SaaS connectors call the app's API directly from serverless compute.
- **Trap: "must not copy the data".** Ingestion of any kind is wrong; use Lakehouse Federation (or sharing).
- **Trap: SCD 1 is the default.** History is off unless you turn it on. And a full refresh erases SCD 2 history.
- **Trap: deletes.** With SCD type 1, deleting a whole table or column in the source does **not** delete it from the destination.
- **Trap: source not supported.** Drop a layer: JDBC/Spark code or a partner tool. Don't force Auto Loader onto a database.
- **Trap: big local file.** UI upload caps at 5 GB per file; bigger means SDK or cloud storage plus Auto Loader.
- **Tradeoff:** managed connectors buy you CDC, retries and schema handling, but you get only the options the connector exposes (e.g. no row filtering for SQL Server via API, a cap of 250 tables per pipeline). Custom code gives full control and full ownership.

## 5) Warm-up answers

**W1 → A (managed Salesforce connector).** It's a supported SaaS source and you want minimal upkeep, so start at the most managed layer: auth, incremental cursors, retries and schema changes are handled. B) Works, but you own pagination, rate limits and API changes forever. C) Someone still has to produce the exports, and you lose incremental change tracking. D) Federation doesn't land tables; the question asks for daily copies in Unity Catalog.

**W2 → B (gateway + staging volume).** Database connectors read change logs continuously, so a gateway extracts changes into a staging volume and the pipeline applies them. A) Every managed connector uses a UC connection. C) All managed connectors write streaming tables. D) Every schedule auto-creates a Lakeflow Job, for SaaS and databases alike.

**W3 → C (update in place).** History tracking off means SCD type 1: the row is overwritten with the latest value. A) That's SCD type 2 (history on). B) `_rescued_data` is Auto Loader's place for values that don't fit the schema, not for updates. D) An ordinary update needs no full refresh.

**W4 → B (Lakehouse Federation).** It queries PostgreSQL in place through a foreign catalog, with no copy. Rare ad-hoc queries are its sweet spot. A) A connector copies the data, which is ruled out. C) Saving to Delta is also a copy. D) Dump files on a volume are a copy too, and stale by design.

## 6) Interview angle

*"When would you write your own connector instead of buying one?"*
Start with the rule: most managed layer first. Then make it concrete with your own experience: "I run about 300 Airflow DAGs, mostly scrapers and extractions, because those sources have no off-the-shelf connector. That's real ownership cost: auth, retries, pagination and schema drift are all on us. For a source like Salesforce or SQL Server, I'd use a managed connector with CDC and spend that time on modeling instead." Bonus points: mention that query-based extraction misses hard deletes, which is why log-based CDC matters for databases.

## 7) Optional hands-on (10 min)

Managed connectors need a real source and admin setup, so keep it to looking around plus one local file:

1. In your workspace, open the data ingestion page (*New → Add or upload data*) and look at which connectors are listed. Notice the split between files, databases and SaaS apps.
2. Upload a small CSV to a volume you own, then read it:

```sql
SELECT * FROM read_files(
  '/Volumes/my_cat/my_sch/landing/uploads/sample.csv',
  format => 'csv', header => true
);
```

3. Optional: run `SHOW CONNECTIONS;` to see whether your workspace already has any UC connections (you may see none, which is fine).

## Official docs

- [What is Lakeflow Connect?](https://docs.databricks.com/aws/en/ingestion/overview)
- [Lakeflow Connect connector concepts](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/)
- [Microsoft SQL Server ingestion connector](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-overview)
- [Enable history tracking (SCD type 2)](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/scd)
- [Work with files in Unity Catalog volumes (upload limits)](https://docs.databricks.com/aws/en/ingestion/file-upload/upload-to-volume)
- [Exam guide](https://www.databricks.com/learn/certification/data-engineer-associate)

*Optional further reading (not homework):* [kengio guide: Ingestion patterns and Lakeflow Connect](https://github.com/kengio/databricks-certification-study-guide/blob/main/certifications/data-engineer-associate/02-data-ingestion-and-loading/01-ingestion-patterns-and-lakeflow-connect.md)

---
*Reply with your warm-up answers (e.g. `W1 B, W2 C ...`). Any misses go into Monday's review. Tomorrow: the Week 2 deep dive, a realistic scenario that puts COPY INTO, Auto Loader and Lakeflow Connect side by side.*
