# DE7: What does this Gold table cost? FinOps for Lakeflow Jobs

**Date:** Sun 2026-11-15 · **Week:** 2026-W46 · **Theme:** Gold objects and Lakeflow Jobs · **Paired Databricks pill:** Gerald's DB7 (Sunday) · **Type:** light weekend pill, interview prep (cost optimization shows up in about 35% of the US postings in the backlog, 45% of Senior DE ones)

---

This week you made Gold correct (DE2), fast (DE1), watched (DE4) and scheduled (DE5). Senior interviews add one more question: **"What does it cost, and how would you cut it?"** A senior engineer treats cost as one more SLA, just like freshness or correctness.

## 1. The mental model: cost = compute type × runtime × how often × waste

- **Compute type.** For scheduled work, use **jobs compute** or **serverless jobs**, never an all-purpose cluster (it has a higher DBU rate and often sits idle). Serverless jobs run in **performance-optimized mode** by default. **Standard mode** uses fewer DBUs if you can accept a typical 4 to 6 minute startup, which fits a nightly Gold job with a 7 AM SLA well.
- **Runtime.** Less data scanned means less time. Use liquid clustering so queries skip files, incremental processing instead of full rebuilds (`REPLACE WHERE` for one day, MERGE, materialized views that refresh incrementally), and Photon for heavy SQL.
- **Frequency.** Does finance really need this every 15 minutes? A trigger on table update (DE5) runs only when upstream data changed. That's cheaper than a cron schedule that runs on nothing.
- **Waste.** Oversized clusters, long auto-termination, retries that redo successful work (use Repair run instead), and dev jobs left on a schedule.

## 2. You can't cut what you can't attribute

Databricks puts every DBU in **`system.billing.usage`**. Each row has `usage_metadata.job_id` / `job_run_id`, `custom_tags`, `billing_origin_product` (for example `JOBS`), and `identity_metadata.run_as`. Join it with **`system.billing.list_prices`** to get dollars at list price, and with **`system.lakeflow.jobs`** to get job names.

How to get attribution in place:
- **Tags** on jobs and clusters (`team`, `domain`, `env`). On classic compute, **cluster policies** can enforce them.
- On serverless, **serverless usage policies** (Public Preview) add attribution tags to the billing records, which you then use for **budgets**.

## 3. Hands-on: top Gold jobs by list cost, this week vs last week

```sql
WITH cost AS (
  SELECT
    u.usage_metadata.job_id                     AS job_id,
    u.custom_tags['team']                       AS team,
    CASE WHEN u.usage_date >= current_date() - INTERVAL 7 DAYS
         THEN 'this_week' ELSE 'last_week' END  AS period,
    SUM(u.usage_quantity * p.pricing.default)   AS list_cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices p
    ON u.cloud = p.cloud
   AND u.sku_name = p.sku_name
   AND u.usage_start_time >= p.price_start_time
   AND (u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL)
  WHERE u.billing_origin_product = 'JOBS'
    AND u.usage_date >= current_date() - INTERVAL 14 DAYS
  GROUP BY ALL
),
jobs AS (
  SELECT job_id, name
  FROM system.lakeflow.jobs
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
)
SELECT j.name, c.team,
       SUM(IF(period = 'this_week', list_cost_usd, 0)) AS this_week,
       SUM(IF(period = 'last_week', list_cost_usd, 0)) AS last_week,
       try_divide(SUM(IF(period = 'this_week', list_cost_usd, 0)),
                  SUM(IF(period = 'last_week', list_cost_usd, 0))) - 1 AS wow_change
FROM cost c LEFT JOIN jobs j USING (job_id)
GROUP BY ALL
ORDER BY this_week DESC
LIMIT 20;
```

Schedule this as a SQL alert on `wow_change > 0.3` and you have cost observability, the same way DE4 gave you data observability. Note: these queries only cover jobs on jobs compute and serverless. Jobs that run on all-purpose clusters or SQL warehouses aren't billed as jobs, which is one more reason to move them.

## 4. Interview question

**"Our Databricks bill went up 40% last month. The CFO wants an answer by Friday. What do you do?"**

**Strong senior answer (short version):**
> "First I'd find where the money went, before I change anything. I'd query `system.billing.usage` joined with list prices, broken down by product, workspace, job and tag, month over month. Usually two or three workloads explain most of the increase. For each one, I'd check whether the growth came from more data, more runs, or a change: a schedule moved from daily to every 15 minutes, a job running on an all-purpose cluster, a full rebuild replacing an incremental load, or a missing clustering key that made scans much bigger.
>
> Then I'd fix it in order of savings versus risk. The quick wins are moving scheduled work to jobs compute or serverless standard mode where the SLA allows it, switching cron schedules to table-update triggers, and right-sizing clusters. The structural fixes are incremental Gold with `REPLACE WHERE` or MERGE and liquid clustering on the filter columns.
>
> To keep it from happening again, I'd make tags mandatory through cluster policies and serverless usage policies, set budgets per team, and alert on week-over-week cost change. And I'd report back in dollars per data product, like 'the revenue mart costs $X a day', because that's a number the CFO can actually make a decision with."

Why it lands: you measure before you optimize, you rank fixes by savings versus risk, and you put guardrails in place so it doesn't happen again. Those are tech-lead behaviors. If you have a real story from Stack about cutting AWS cost (Glue, EMR, Lambda), tell it here and map it to Databricks.

## 5. English phrasing tip

- Say **"the bill went up 40%"** or **"spend increased 40%"**, not "the cost raised". *Raise* needs an object ("they raised prices"), while *rise* doesn't ("costs rose").
- A good senior line: **"I'd rather attribute before I optimize."**
- "Quick wins" vs "structural fixes" is a clean way to frame trade-offs out loud.

## Go deeper (official docs)

- [Monitor job costs & performance with system tables](https://docs.databricks.com/aws/en/admin/system-tables/jobs-cost)
- [Billable usage system table reference](https://docs.databricks.com/aws/en/admin/system-tables/billing)
- [Run your Lakeflow Jobs with serverless compute (performance modes, usage policies)](https://docs.databricks.com/aws/en/jobs/run-serverless-jobs)
- [Cost optimization best practices](https://docs.databricks.com/aws/en/lakehouse-architecture/cost-optimization/best-practices)
