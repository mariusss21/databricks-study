# DE7: System design capstone: near-real-time CDC from Postgres to Gold dashboards

- **Date:** Sun 2026-11-22
- **Week:** 2026-W47
- **Theme:** Lakeflow pipelines and Jobs in depth
- **Paired Databricks pill (Gerald):** DB7: Recap: Connect to Declarative Pipelines to Jobs as one Lakeflow path
- **Format:** weekend system design capstone (ties together DE1 to DE6)

---

Good morning, Mario! Sunday capstone. Gerald recaps the Lakeflow path (Connect, Declarative Pipelines, Jobs). This pill turns the whole week into **one 45-minute system design answer**, the kind US senior interviews love: *"Our ops team wants dashboards on orders data from Postgres, fresh within 5 minutes. Design it."*

## Step 0: clarify before you draw (2-3 minutes)

Ask, then write the answers down as your requirements:

- **Freshness SLA:** 5 minutes end to end, or "every 15 is fine"? (DE1: this decides streaming vs triggered.)
- **Volume:** e.g. 40 tables, ~2k changes/sec at peak, 5 TB history.
- **History:** do they need "as of" reporting (SCD2) or just the current state (SCD1)?
- **Deletes:** hard deletes in Postgres? (DE3: you need log-based CDC to see them.)
- **Correctness vs speed:** is a slightly late number OK, or is a wrong number worse? (It almost always is.)

## The design, layer by layer

| Layer | Choice | Why (the trade-off you say out loud) |
|---|---|---|
| **Capture** | Log-based CDC from the Postgres WAL: **Lakeflow Connect PostgreSQL connector** (managed), or Debezium → Kafka if Kafka already exists | Sees deletes and ordering (LSN). Buy before build (DE6); Debezium if other consumers also need the stream |
| **Bronze** | Append-only streaming table of raw change events (op, before/after, LSN, ingest time) | Replayable source of truth. If Silver breaks, you rebuild from here |
| **Silver** | **AUTO CDC** in a Lakeflow Declarative Pipeline: SCD1 for current state, SCD2 where history matters, `SEQUENCE BY` LSN, deletes applied | Ordering, duplicates and late events handled by the engine (DE3), one config-driven flow per table (DE4) |
| **Quality gates** | Expectations: **fail** on null keys, **drop/quarantine** malformed rows, warn on soft rules | Bad data stops at Silver, never reaches the exec dashboard |
| **Gold** | Materialized views (daily/hourly KPIs) on serverless, which can refresh incrementally | Dashboard queries hit small, pre-aggregated tables |
| **Run** | One **Lakeflow Job**: ingestion → transform pipeline, triggered every few minutes; continuous only if the SLA truly needs it | Triggered is far cheaper than always-on compute (DE1, W46 FinOps) |
| **Operate** | Freshness SLO alert, job duration thresholds, runbook, CI tests (DE2, DE5) | You own it at 3 AM, so design for that |

## Hands-on: the two pieces interviewers ask you to sketch

Silver apply, one table (Python, Lakeflow Declarative Pipelines):

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

dp.create_streaming_table(
    "silver_orders",
    expect_all_or_fail={"valid_key": "order_id IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    target="silver_orders",
    source="bronze_orders_changes",
    keys=["order_id"],
    sequence_by=col("lsn"),
    apply_as_deletes=expr("op = 'd'"),
    except_column_list=["op", "lsn"],
    stored_as_scd_type=1,
)
```

Freshness SLO check (Databricks SQL, wire it to a SQL alert that pages when `lag_min > 5`):

```sql
SELECT
  timestampdiff(MINUTE, max(_ingested_at), current_timestamp()) AS lag_min,
  count(*)                                                       AS rows_now
FROM main.ops.silver_orders;
```

Pair it with a nightly **row-count reconciliation** against Postgres so you catch silent loss, not just lateness.

## Likely interview question

**"Walk me through what happens when this pipeline breaks at 2 AM."**

Strong senior answer:

> "First, I'd know before the business does: a freshness alert fires when Silver lag passes 5 minutes, and the job has a duration threshold. The dashboard shows a 'data as of' timestamp, so users see stale data, not wrong data. Then I triage by layer. If capture is behind, I check replication lag and the slot on Postgres, because a stuck slot can fill the source disk, and that's the real emergency. If a Silver expectation failed, the bad batch never reached Gold. I fix the rule or the data and rerun, which is safe because AUTO CDC is idempotent on key and LSN. If Silver got corrupted, I rebuild from Bronze, because Bronze is append-only and replayable. Afterwards I run a blameless postmortem and update the runbook. The design goal is that every failure is either *late* or *loud*, never *silently wrong*."

That last line is the one they'll remember.

## English phrasing tip

When you state trade-offs, use **"I'd optimize for X at the cost of Y"**: *"I'd optimize for correctness at the cost of a few minutes of latency, because a wrong revenue number in front of the COO is worse than a late one."* It sounds senior because it names what you give up.

## Go deeper

- [PostgreSQL ingestion connector (Lakeflow Connect)](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/postgresql)
- [The AUTO CDC APIs](https://docs.databricks.com/aws/en/ldp/cdc)
- [Automate jobs with schedules and triggers](https://docs.databricks.com/aws/en/jobs/triggers)

That closes W47. Great week, Mario. Rest well and enjoy the planning Sunday! 🌎💵
