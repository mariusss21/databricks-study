# DE2: Defending medallion layer boundaries, data contracts, and idempotent Gold

- **Date:** Tue 2026-11-10
- **Week:** 2026-W46
- **Theme:** Gold objects and Lakeflow Jobs
- **Paired Databricks pill:** Gerald's DB2 for Tue 2026-11-10

---

## The idea in one breath

Bronze, Silver, and Gold are only useful if each layer makes a **promise** and keeps it. Senior interviews love to poke at this: "Why not just write straight to Gold?" or "What happens if the job runs twice?" A strong answer has three parts: clear layer boundaries, explicit contracts between layers, and Gold loads that are safe to re-run.

## 1. Layer boundaries: what each layer promises

| Layer | Promise | Who reads it |
|---|---|---|
| Bronze | Everything that arrived, as it arrived, append-only, replayable | Data engineers, audit |
| Silver | One validated, deduplicated, typed row per business entity or event, at a known grain | Engineers, analysts, data scientists |
| Gold | Business-shaped facts, dimensions, and aggregates, tuned for queries | BI, execs, ML, apps |

The boundary rules that keep it honest:
- **Business logic lives in Gold, cleaning lives in Silver.** If a revenue rule shows up in Silver, two Gold tables will end up computing it differently.
- **Gold never reads Bronze.** Skipping Silver means skipping dedup and quality gates.
- **Bronze stays raw** (strings, `VARIANT`, `_metadata.file_name`), so you can always replay.

## 2. Data contracts between layers

A data contract is the producer's written promise to consumers: schema, grain, keys, freshness, and quality rules, plus who owns it and how breaking changes are announced. In Databricks you enforce pieces of it in different places:

- **Schema and types:** Delta schema enforcement on write, explicit column lists in Silver.
- **Row rules:** Lakeflow pipeline **expectations** (`EXPECT ... ON VIOLATION DROP ROW` or `FAIL UPDATE`) or Delta `CHECK` / `NOT NULL` constraints on plain tables.
- **Keys and relationships:** `PRIMARY KEY` / `FOREIGN KEY` in Unity Catalog. They are informational (not enforced), so you still test uniqueness yourself.
- **Ownership and discovery:** Unity Catalog owners, comments, and tags on the table.

Rule of thumb: drop or quarantine bad rows at the Silver boundary, but **fail** the update at the Gold boundary, because a wrong number on a CFO dashboard is worse than a late one.

## 3. Idempotent, re-runnable Gold

Idempotent means running the same load twice for the same input gives the same result: no duplicates, no double counting. Lakeflow Jobs will retry tasks, and you will use **repair run** after a failure, so Gold must survive that.

Three patterns:
1. **Replace a slice** for a parameterized date (`INSERT INTO ... REPLACE WHERE`). Great for daily aggregates.
2. **MERGE on the business key** for facts and dimensions. Deterministic keys, never `monotonically_increasing_id()` or `uuid()` as the match key.
3. **Materialized view** in a Lakeflow pipeline, where the engine owns the refresh and you don't write load logic at all.

What breaks idempotency: plain `INSERT INTO` appends, `current_date()` inside the logic instead of a job parameter, and non-deterministic keys.

## Hands-on

```sql
-- Contract at the Gold boundary: hard rules fail the write
ALTER TABLE gold.daily_sales ADD CONSTRAINT non_negative_revenue CHECK (revenue >= 0);
ALTER TABLE gold.daily_sales ALTER COLUMN sale_date SET NOT NULL;

-- Idempotent daily load: :run_date comes from the Lakeflow Job parameter,
-- so a retry or a repair run for 2026-11-09 rewrites exactly that day.
INSERT INTO gold.daily_sales
REPLACE WHERE sale_date = :run_date
SELECT sale_date,
       store_id,
       SUM(amount)                 AS revenue,
       COUNT(DISTINCT customer_id) AS customers
FROM silver.sales
WHERE sale_date = :run_date
GROUP BY sale_date, store_id;
```

Run it twice for the same `run_date` and the row count stays the same. That's the test you'd show in an interview.

## Likely interview question

**"Your nightly Gold job failed halfway, and someone re-ran it. Now revenue is doubled for one day. How do you fix it and stop it from happening again?"**

Strong senior answer:
> "First I'd contain it: flag the affected dashboards and rebuild the bad day from Silver, which is the trusted source, so the fix is just re-running a corrected load for that date. Root cause is almost always a non-idempotent write, like a plain append. I'd change the load so each run owns a slice: a date parameter from the job, and either `REPLACE WHERE` for that date or a `MERGE` on the business key. Then I'd add a contract check at the Gold boundary, like a uniqueness test on the grain and a reconciliation of totals against Silver, that fails the task before bad numbers publish. Finally I'd make sure retries and repair runs in the Lakeflow Job are safe by design, and I'd write it up as a short postmortem so the team adopts the pattern everywhere, not just in this job."

Why it lands: it covers containment, root cause, prevention, and team process, which is what US interviewers listen for from a tech lead.

## English phrasing tip

Use **"owns"** and **"guarantees"** to sound precise: "Silver *guarantees* one row per order," "each run *owns* exactly one day." And say "safe to re-run" in plain words before you say "idempotent," so non-specialists on the panel follow you.

## Go deeper

- [Medallion lakehouse architecture (Databricks docs)](https://docs.databricks.com/aws/en/lakehouse/medallion)
- [Pipeline expectations for data quality (Databricks docs)](https://docs.databricks.com/aws/en/ldp/expectations)
- [Constraints on Databricks (Databricks docs)](https://docs.databricks.com/aws/en/tables/constraints)
