# DE1: Streaming vs batch as a design decision (and when NOT to stream)

- **Date:** Mon 2026-11-16
- **Week:** 2026-W47
- **Theme:** Lakeflow pipelines and Jobs in depth
- **Paired Databricks pill (Gerald):** DB1: Lakeflow Spark Declarative Pipelines: streaming tables vs materialized views, flows, triggered vs continuous updates

---

Good morning, Mario. This week we go deeper into Lakeflow pipelines and Jobs. Today's pill is the question behind all of it: should this pipeline stream at all? In October we covered delivery semantics (exactly-once, checkpoints). Today is the design side: latency, cost, state, and the courage to say "batch is fine."

## 1. The key reframe: streaming is an *engine*, latency is a *schedule*

Senior engineers separate two things juniors mix up:

| Question | Options |
|---|---|
| **How is data processed?** (engine) | Full recompute (batch) vs incremental (streaming: only new data, tracked by a checkpoint) |
| **How often does it run?** (trigger) | Once a day, every 15 min, always-on, sub-second |

On Databricks you can use the **streaming engine on a batch schedule**: `trigger(availableNow=True)` processes everything new since the last checkpoint, then stops. You get incremental, exactly-once processing, and you only pay while the job runs. That's the default answer for most "near real time" asks.

The latency ladder on Databricks:

| Need | Pattern | Cost profile |
|---|---|---|
| Hours / daily | Lakeflow Job on schedule; `availableNow` stream or MV refresh | Pay per run |
| ~5–30 min | Same, more often, or a **triggered** pipeline on a table-update / file-arrival trigger | Pay per run, more runs |
| Seconds | **Continuous** pipeline, or default micro-batch trigger (~3–5 s) | Cluster always on |
| Sub-second (fraud, personalization) | **Real-time mode**: `.trigger(realTime='5 minutes')`, also in Lakeflow pipelines | Dedicated, always-on compute |

Note: on serverless notebooks and jobs, only `availableNow` / `Once` triggers are supported. For always-on on serverless, use a pipeline in continuous mode.

## 2. The Lakeflow link (pairs with Gerald's DB1)

In Lakeflow Spark Declarative Pipelines:
- **Streaming table** = incremental, append-style processing with a checkpoint per flow (good for ingest, append-only facts, CDC).
- **Materialized view** = "always correct result of this query"; Databricks refreshes it incrementally when that's cheaper, else recomputes (good for joins and aggregates that change retroactively).
- **Pipeline mode**: *triggered* (run, update, stop) vs *continuous* (keep updating). Same code; the mode is a config choice. That's the whole point: **latency becomes a knob you turn, not a rewrite.**

## 3. What streaming really costs you (the "when NOT to stream" list)

1. **Always-on compute** for a dashboard someone opens at 9 AM.
2. **State**: aggregations, dedup and stream-stream joins keep state. Without a **watermark** the state grows forever. With one, late data past the threshold gets dropped. You trade completeness for bounded memory.
3. **Retroactive changes**: a dimension updated yesterday doesn't re-join old stream rows. MVs or batch recompute handle that naturally.
4. **Operations**: checkpoints to protect, schema changes that need restarts, replays after failure, harder backfills.
5. **Small files** from frequent commits (mitigated by optimized writes / predictive optimization, but still real).

Stream when: the **business acts on the data within minutes** (fraud, ops alerts, inventory), the source is naturally a log (Kafka, CDC), or incremental processing is cheaper than recomputing a huge table. Batch when: humans read it daily, the logic needs full-history recompute, or the team can't operate 24/7 pipelines.

## 4. Hands-on: same code, two latencies

```python
# Incremental Bronze ingest with Auto Loader, run as a scheduled job
(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")
   .load("/Volumes/main/raw/orders/")
   .writeStream
   .option("checkpointLocation", "/Volumes/main/chk/orders_bronze")
   .trigger(availableNow=True)          # batch schedule, streaming engine
   .toTable("main.bronze.orders"))
# Want ~5 s latency instead? Remove the trigger line (classic compute)
# or put the logic in a continuous pipeline. The code stays the same.
```

```sql
-- Lakeflow pipeline (SQL): streaming table + windowed aggregate with a watermark
CREATE OR REFRESH STREAMING TABLE orders_bronze
AS SELECT * FROM STREAM read_files('/Volumes/main/raw/orders/', format => 'json');

CREATE OR REFRESH STREAMING TABLE orders_per_5min
AS SELECT window(order_ts, '5 minutes') AS w, count(*) AS orders
FROM STREAM(orders_bronze) WATERMARK order_ts DELAY OF INTERVAL 10 MINUTES
GROUP BY window(order_ts, '5 minutes');
```

## 5. Interview question

**"The product team wants the sales dashboard in real time. How do you design it?"**

Strong senior answer (shape):
> "First I'd ask what decision 'real time' supports and what happens if the data is 15 minutes old. Most dashboards are read by humans a few times a day, so the real requirement is usually 'fresh within 15–30 minutes, reliably.' For that I'd use the streaming engine with an incremental trigger: Auto Loader or a CDC feed into streaming tables, with `availableNow` or a triggered Lakeflow pipeline run by a Job on a file-arrival or table-update trigger. We get incremental, exactly-once processing and pay only while it runs. Because the logic is the same for triggered and continuous mode, we can move to continuous later without a rewrite if the SLA tightens. I'd only go always-on, or to real-time mode for sub-second, when a system acts on the data automatically, like fraud blocking or stock alerts. Then I'd budget for 24/7 compute, watermarks for state, and on-call. I'd put the SLA and the cost of each option side by side so the business picks the trade-off with the numbers in front of them."

Why it's senior: it pushes back on the requirement, names a cost/latency trade-off, keeps a migration path open, and shows you understand the operational burden, not just the API.

## 6. English phrasing tip

When you push back on a requirement, sound curious, not negative:
- "What decision does this data drive, and how stale can it be before that decision is wrong?"
- "We can get you to 15 minutes cheaply. Going below a minute is a different cost tier. Is that worth it for this use case?"

Avoid "we don't need streaming." Say "**we don't need always-on compute for this SLA**."

## Go deeper (official docs)

- Structured Streaming trigger intervals (availableNow, processingTime, real-time mode): https://docs.databricks.com/aws/en/structured-streaming/triggers
- Real-time mode concepts (when to use it vs micro-batch): https://docs.databricks.com/aws/en/structured-streaming/real-time
- Watermarks and stateful processing: https://docs.databricks.com/aws/en/structured-streaming/watermarks
- Run a pipeline update (refresh vs full refresh semantics): https://docs.databricks.com/aws/en/ldp/updates
