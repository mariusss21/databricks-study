# DE6: One definition of "revenue": semantic layers and Unity Catalog metric views

- **Date:** Sat 2026-11-14
- **Week:** 2026-W46, Gold objects and Lakeflow Jobs
- **Paired Databricks pill:** none on Saturday (week pairs: DB3 Gold layer objects, DB5 Lakeflow Jobs basics)
- **Type:** light weekend pill, concept + tech gap (dbt Semantic Layer, Snowflake semantic views)

---

## The problem in one sentence

You built a clean, idempotent Gold table this week. Then Finance, Marketing and the CEO dashboard each write their own `SUM(...)` on top of it, and you get three different "revenue" numbers in the same meeting.

A **semantic layer** fixes that: you define each metric **once**, next to the data (name, formula, the right aggregation, allowed dimensions, owner), and every consumer asks for the metric by name: BI, notebooks, alerts, and AI agents.

In US job postings this appears as "semantic layer", "metrics layer", or "single source of truth for KPIs". It shows up more in AI Data Engineer roles, because a text-to-SQL agent is only as accurate as the metric definitions it reads.

## Why a view or a Gold table isn't enough

- A normal view **freezes the grouping**. `revenue_by_month` can't answer "revenue by region" without a new view.
- Ratios and distinct counts are **not additive**. If you pre-aggregate `revenue_per_customer` by day and then average the days, the number is wrong. You have to recompute `SUM(revenue) / COUNT(DISTINCT customer)` at whatever grain the user asks for.
- A semantic layer stores the **formula**, not the result, and the engine builds the right SQL for the grouping the user asks for.

## The Databricks piece: Unity Catalog metric views

A metric view is a Unity Catalog object defined in YAML. It has a `source` (your Gold fact table), optional star or snowflake `joins` to dimensions, `fields` (dimensions to group or filter by), and `measures` (aggregate formulas). You query measures with `MEASURE()`, and you can group by any field you defined.

Because it lives in Unity Catalog, you get grants, lineage and comments for free. The same definition serves AI/BI dashboards, Genie, SQL alerts, notebooks and external BI tools. Optional `materialization` pre-computes hot aggregations, and the engine rewrites queries to use them.

### Hands-on (runs on `samples.tpch`)

```sql
CREATE OR REPLACE VIEW main.gold.orders_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Order KPIs. Owner: data-eng. Revenue = gross order price."
source: samples.tpch.orders
joins:
  - name: customer
    source: samples.tpch.customer
    'on': source.o_custkey = customer.c_custkey
    rely:
      at_most_one_match: true
fields:
  - name: order_month
    expr: DATE_TRUNC('MONTH', source.o_orderdate)
  - name: market_segment
    expr: customer.c_mktsegment
measures:
  - name: order_count
    expr: COUNT(1)
  - name: total_revenue
    expr: SUM(source.o_totalprice)
  - name: revenue_per_customer
    expr: SUM(source.o_totalprice) / COUNT(DISTINCT source.o_custkey)
$$;

-- Same definition, any grouping, ratio recomputed correctly at each grain
SELECT market_segment,
       MEASURE(total_revenue)        AS revenue,
       MEASURE(revenue_per_customer) AS rev_per_customer
FROM main.gold.orders_metrics
GROUP BY market_segment;

SELECT order_month, MEASURE(revenue_per_customer)
FROM main.gold.orders_metrics
GROUP BY order_month
ORDER BY order_month;
```

Where it sits in this week's story: the **Lakeflow Job** keeps the Gold fact fresh and idempotent (DE2, DE5), **observability** watches it (DE4), and the **metric view** is the contract consumers query. Store the YAML in Git and deploy it with the job, so a metric change goes through code review.

## Tech gap: what US teams use instead

| | Databricks | dbt | Snowflake |
|---|---|---|---|
| Object | UC **metric view** (YAML in a `CREATE VIEW ... WITH METRICS`) | **dbt Semantic Layer**, powered by MetricFlow (semantic models + metrics in YAML in the dbt project) | **Semantic view** (`CREATE SEMANTIC VIEW`, schema-level object) |
| Building blocks | source, joins, fields, measures | semantic models (entities, dimensions, measures), metrics | logical tables, relationships, facts, dimensions, metrics |
| Who queries it | SQL `MEASURE()`, AI/BI dashboards, Genie, alerts, BI tools | Semantic Layer APIs, BI integrations, dbt MCP server for AI tools | `SELECT` on the semantic view, Cortex Analyst, Cortex Agents |
| Governance | Unity Catalog grants and lineage | dbt platform permissions (needs a paid dbt plan) | Snowflake RBAC |
| Watch out | Databricks-only object | Extra service in the query path | Snowflake-only object |

How to say it in an interview: "They solve the same problem. dbt's is warehouse-agnostic but adds a hosted service. Metric views and Snowflake semantic views live inside the platform, next to governance and the AI assistant. If a company is already on dbt across warehouses, I'd keep dbt metrics. If it's all-in on Databricks, I'd use metric views so Genie and dashboards share one definition."

## Likely interview question

> "Leadership complains that revenue on the exec dashboard doesn't match Finance's report. Both read from your Gold layer. What do you do?"

**Strong senior answer (about 60 seconds):**

1. **Diagnose before fixing.** "I'd put the two queries side by side. In my experience it's almost never the pipeline. It's the definition: gross vs net of refunds, order date vs ship date, time zone, or one side averaging a ratio that was already aggregated."
2. **Agree on the definition with the owner.** "Finance owns revenue, so I get them to sign off on one written definition, including the edge cases."
3. **Encode it once.** "I'd move it into a semantic layer: on Databricks, a Unity Catalog metric view on the Gold fact, with the measure as a formula so ratios are recomputed at every grain. I'd put a named owner and a comment on it, and keep the YAML in Git with code review."
4. **Point consumers at it and retire the copies.** "The dashboards, Genie space and alerts query the metric view. The ad hoc SQL copies get deprecated, and lineage shows me who still reads the raw table."
5. **Prevent regressions.** "I'd add a reconciliation check in the Lakeflow Job that compares the metric to Finance's ledger total for the day and alerts if they drift past a threshold."

Why it lands: it's a people problem plus a technical fix. That's what a tech lead is hired for.

## English phrasing tip

- Say **"a single source of truth for metrics"** or **"one governed definition"**, not "a unique definition" (that sounds like a translation of *definição única*).
- **"The numbers don't tie out"** is natural US finance/data English for "they don't match". Example: "The dashboard didn't tie out to the ledger, so we traced it to a refund filter."
- **"Metric drift"** = the same metric slowly diverging across tools.

## Go deeper

- Databricks: [Unity Catalog metric views](https://docs.databricks.com/aws/en/metric-views/) and the [YAML syntax reference](https://docs.databricks.com/aws/en/metric-views/data-modeling/syntax)
- dbt: [dbt Semantic Layer (MetricFlow)](https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl)
- Snowflake: [Overview of semantic views](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
