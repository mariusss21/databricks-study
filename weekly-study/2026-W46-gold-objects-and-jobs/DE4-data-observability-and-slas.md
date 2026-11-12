# DE4: Data observability and SLAs: freshness, volume, schema, distribution, and lineage

- **Date:** Thu 2026-11-12
- **Week:** 2026-W46, Gold objects and Lakeflow Jobs
- **Type:** Concept + interview story
- **Paired Databricks pill:** Gerald's DB4 for Thu 2026-11-12 (data quality in Silver and Gold: constraints, expectations, quarantine)

---

## Why this matters

Gerald's pill today covers the quality *gates*: constraints and expectations that stop bad rows. Observability is the other half. It answers "is the data I already published still healthy, and would I know before the CFO does?" US Senior DE postings keep asking for "data quality and observability", "SLAs/SLOs", and tools like Monte Carlo, Great Expectations, or dbt tests. The senior signal: you treat datasets like production services.

## Tests vs observability

- **Tests / expectations** check *known* rules inside the pipeline: `amount >= 0`, `order_id` not null. They run while the data is written.
- **Observability** watches *unknown unknowns* from the outside: the table didn't update, row count dropped 60%, a column went 40% null, a new country code appeared.

You need both. Expectations catch what you predicted. Observability catches what you didn't.

## The five pillars (the vocabulary interviewers use)

1. **Freshness:** when was the table last updated, and is that later than expected?
2. **Volume:** did we get roughly the usual number of rows?
3. **Schema:** did columns appear, disappear, or change type?
4. **Distribution:** null rate, value ranges, cardinality, drift vs a baseline.
5. **Lineage:** what feeds this table, and who breaks if it breaks?

## SLA, SLO, SLI in one breath

- **SLI** (indicator): what you measure, e.g. "minutes since `gold.daily_revenue` last committed".
- **SLO** (objective): your internal target, e.g. "fresh by 07:00 BRT on 99% of days".
- **SLA** (agreement): the promise to consumers, usually looser than the SLO, with an owner and an escalation path.

A good SLA names the table, the deadline, the owner, and what happens when it's missed.

## How it maps to Databricks

- **Data quality monitoring** (Unity Catalog). Two parts:
  - **Anomaly detection**: enable it once on a schema. It learns each table's commit history and row counts, then flags tables that are *stale* (freshness) or *incomplete* (volume). Results land in `system.data_quality_monitoring.table_results`, and you can alert on them.
  - **Data profiling** (formerly *Lakehouse Monitoring*): per-table metric tables with null fractions, distributions, and drift between time windows or against a baseline. This is your distribution pillar.
- **Schema:** Delta schema enforcement blocks surprise changes, and `DESCRIBE HISTORY` shows who changed what.
- **Lineage:** Unity Catalog captures lineage automatically, down to the column, including the jobs and dashboards that read a table. You can query it in `system.access.table_lineage` for impact analysis.
- **Pipeline SLAs:** Lakeflow Jobs health rules (for example a run-duration threshold) plus failure notifications, and Lakeflow Declarative Pipelines expectations metrics in the event log.
- **Alerting:** Databricks SQL alerts on any of the queries below.

## Hands-on: a tiny homemade freshness + volume check

```sql
-- Freshness SLI: minutes since the Gold table last got new data
-- (_loaded_at is the load timestamp your job writes on every row)
SELECT timestampdiff(MINUTE, max(_loaded_at), current_timestamp()) AS minutes_stale
FROM main.gold.daily_revenue;

-- Volume SLI: today's rows vs the trailing 7-day average
WITH daily AS (
  SELECT order_date, count(*) AS n
  FROM main.silver.orders
  WHERE order_date >= current_date() - INTERVAL 8 DAYS
  GROUP BY order_date
)
SELECT
  max(CASE WHEN order_date = current_date() - INTERVAL 1 DAY THEN n END) AS yesterday,
  avg(CASE WHEN order_date <  current_date() - INTERVAL 1 DAY THEN n END) AS avg_7d
FROM daily;
-- Put a SQL alert on: yesterday < 0.5 * avg_7d

-- Lineage: who reads from my Gold table? (blast radius before an incident note)
SELECT DISTINCT target_table_full_name, entity_type
FROM system.access.table_lineage
WHERE source_table_full_name = 'main.gold.daily_revenue'
  AND event_time >= current_date() - INTERVAL 30 DAYS;
```

In real life you'd turn on anomaly detection for the `gold` schema instead of hand-writing this for 200 tables. Hand-written checks stay useful for the 5 to 10 business-critical tables with hard SLAs.

## Likely interview question

**"Your revenue dashboard showed half the usual numbers this morning, and finance found it before you did. What do you change?"**

**Strong senior answer:**

"First I fix today: I use lineage to trace upstream from the Gold table, find where the volume dropped (it was a late Bronze file), backfill the affected slice idempotently, and tell finance what happened and when it's fixed.

Then I make sure we find out first next time. I'd set up three layers. One: gates inside the pipeline, with expectations that drop bad rows in Silver and fail the run in Gold. Two: observability outside it. On Databricks I'd enable anomaly detection on the Gold schema for freshness and completeness, and add profiling on the revenue fact for null rates and drift. Three: an explicit SLA for the dashboard, 'fresh by 7 AM, with an owner and an on-call path', backed by a job duration alert and a SQL alert on volume vs the 7-day average.

The goal is that a broken table pages us *before* the data reaches a human. I'd also tier the tables, because not every table deserves a pager."

## English phrasing tip

- "We **got paged before** finance noticed" (not "we were advised before").
- "That's the **blast radius**": who's affected downstream.
- Say "**freshness SLA**" and "**volume anomaly**". Avoid "the data was late of 2 hours". Say "**the data landed two hours late**."

## Go deeper

- [Data quality monitoring (Databricks)](https://docs.databricks.com/aws/en/data-quality-monitoring/)
- [Anomaly detection (Databricks)](https://docs.databricks.com/aws/en/data-quality-monitoring/anomaly-detection/)
- [Lineage in Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage)
