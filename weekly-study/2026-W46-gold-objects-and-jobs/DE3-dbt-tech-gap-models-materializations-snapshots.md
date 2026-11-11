# DE3: dbt for a Databricks engineer: models, ref, materializations, and snapshots

- **Date:** Wed 2026-11-11
- **Week:** 2026-W46, Gold objects and Lakeflow Jobs
- **Type:** Tech gap (dbt)
- **Paired Databricks pill:** Gerald's DB3 for Wed 2026-11-11

---

## Why this matters

dbt shows up in a huge share of US Senior DE postings, often as "SQL + dbt + Snowflake/Databricks". You don't need years of dbt. You need to show you understand what it does and how it maps to what you already know in Databricks.

## What dbt is, in one paragraph

dbt (data build tool) is the "T" in ELT. You write each transformation as a `SELECT` in a `.sql` file (a **model**). dbt compiles it, works out the dependency graph, and runs it in the warehouse as a view or table. It adds what analysts never had with plain SQL scripts: version control, tests, docs, lineage, and environments (dev/prod). It doesn't move data and has no compute of its own. On Databricks it runs through the `dbt-databricks` adapter on a SQL warehouse.

US companies like it because analytics engineers who know SQL can own the Gold layer with software practices (pull requests, CI, tests) without writing Spark.

## The four ideas you must know

**1. Models and `ref()`**

```sql
-- models/gold/fct_daily_revenue.sql
select
  o.order_date,
  c.region,
  sum(o.amount) as revenue
from {{ ref('silver_orders') }} o
join {{ ref('dim_customer') }} c using (customer_id)
group by all
```

`ref()` does two things: it resolves the right schema per environment (dev vs prod), and it builds the DAG, so dbt knows `silver_orders` must run first. `source()` does the same for raw tables that dbt doesn't build.

**2. Materializations** (how a model lands in the warehouse)

| dbt | What it does | Closest Databricks thing |
|---|---|---|
| `view` | `CREATE VIEW`, nothing stored | UC view |
| `table` | full rebuild every run (`CREATE OR REPLACE TABLE AS`) | CTAS / full-recompute Gold |
| `incremental` | only processes new or changed rows | `MERGE INTO`, `INSERT ... REPLACE WHERE` |
| `ephemeral` | inlined as a CTE, no object | a CTE |
| `materialized_view` / `streaming_table` (dbt-databricks) | creates the real Databricks object | Databricks MV / streaming table |

**3. Incremental models**

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id'
) }}

select * from {{ ref('silver_orders') }}
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

On Databricks, the `merge` strategy compiles to a Delta `MERGE INTO`. The adapter also supports `append`, `insert_overwrite`, `replace_where` (yesterday's `REPLACE WHERE` idea), and `microbatch`. The high-water mark filter is your responsibility, and it's the classic place for late-arriving data bugs, so seniors add a lookback window (for example `max(updated_at) - interval 3 days`).

**4. Snapshots = SCD Type 2 for free**

```yaml
# snapshots/customers_snapshot.yml
snapshots:
  - name: customers_snapshot
    relation: ref('silver_customers')
    config:
      unique_key: customer_id
      strategy: timestamp      # or: check, with check_cols
      updated_at: updated_at
```

dbt adds `dbt_valid_from`, `dbt_valid_to` (null for the current row), `dbt_scd_id`, and `dbt_updated_at`, and runs a `MERGE` to close old versions. The Databricks equivalent is `AUTO CDC ... STORED AS SCD TYPE 2` in Lakeflow Declarative Pipelines. Key difference: a snapshot only sees the state at the moment it runs, so changes that happen between two runs are lost. AUTO CDC consumes a change feed, so it captures every change and handles out-of-order events with `SEQUENCE BY`.

## dbt vs Lakeflow Declarative Pipelines, honestly

- **Both** are declarative, build a DAG, and support tests/expectations.
- **dbt** is SQL-first, warehouse-agnostic (Snowflake, BigQuery, Databricks), batch, and has a huge ecosystem (packages like `dbt_utils`, docs site, CI with `state:modified`).
- **Lakeflow Declarative Pipelines** is Databricks-only, handles streaming natively, has AUTO CDC, expectations with drop/fail, and incremental refresh of materialized views managed for you.
- **Common real pattern:** ingestion and Bronze-to-Silver in Lakeflow (streaming, CDC), Silver-to-Gold business models in dbt, orchestrated by a **Lakeflow Job with a dbt task**. That ties straight into this week's theme.

## Interview question

> "We're on Databricks. Should we adopt dbt for our Gold layer, or stay with Lakeflow Declarative Pipelines?"

**Strong senior answer:**
"It depends on who owns Gold and what the data looks like. If analysts and analytics engineers own the business models, and the work is batch SQL, dbt is a great fit: they get PR reviews, tests, docs, and lineage with only SQL, and the skills are portable across warehouses. If the Gold tables need streaming freshness, CDC with out-of-order events, or tight quality enforcement, I'd keep that in Lakeflow, because AUTO CDC and expectations do that natively and dbt snapshots miss intermediate changes. In practice I'd split by layer: Lakeflow for ingestion and Silver, dbt for Gold marts, one Lakeflow Job orchestrating both so we have a single schedule, retries, and alerting. The thing I'd avoid is two tools building the same table, because then nobody owns the contract."

## English phrasing tip

When asked about a tool you haven't used in production, don't say "I never used it." Say:
"I haven't run dbt in production, but I've built the same patterns in Databricks: incremental MERGE models, SCD2, and tested Gold tables. dbt is mostly a different way to package them, so I'd be productive quickly."
It's honest and turns a gap into transferable experience.

## Try it (10 minutes)

On a Databricks SQL warehouse, read a dbt incremental model as the SQL it becomes:

```sql
MERGE INTO gold.fct_orders AS t
USING (
  SELECT * FROM silver.orders
  WHERE updated_at > (SELECT max(updated_at) - INTERVAL 3 DAYS FROM gold.fct_orders)
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

That is literally what `incremental_strategy='merge'` with a lookback runs for you.

## Go deeper

- [dbt materializations](https://docs.getdbt.com/docs/build/materializations)
- [dbt snapshots](https://docs.getdbt.com/docs/build/snapshots)
- [dbt incremental strategies](https://docs.getdbt.com/docs/build/incremental-strategy)
- [Connect to dbt Core on Databricks](https://docs.databricks.com/aws/en/partners/prep/dbt)
- [dbt task in Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/dbt)
