# 2026-W44: Bronze to Silver transforms

**Theme:** Bronze to Silver transforms with PySpark and SQL

**Dates:** Mon 2026-10-26 to Sun 2026-11-01

Everything for this week lives in this folder: Gerald's daily Databricks pills (`DB1`..`DB7`), NJ in Dollar's daily Data Engineering pills (`DE1`..`DE7`), and Zé Carlos's two deep dives.

## Day by day

Pill files get linked here as they land.

| Day | Databricks pill (Gerald) | DE pill (NJ in Dollar) |
|---|---|---|
| Mon 2026-10-26 | [DB1: Bronze-to-Silver cleaning: reading Bronze by UC name, explicit casts, null rules, timestamp standardization](DB1-bronze-to-silver-cleaning.md) | [DE1: Silver data quality and conformance: expectations, quarantine tables, fail vs drop vs warn](DE1-silver-data-quality-and-conformance.md) |
| Tue 2026-10-27 | [DB2: Join types on Databricks: inner, left, cross, multi-key, broadcast joins (plus union vs unionByName)](DB2-join-types.md) | [DE2: Dimensional modeling basics: facts, dimensions, grain, surrogate keys, star schema](DE2-dimensional-modeling-basics.md) |
| Wed 2026-10-28 | [DB3: Column and row surgery: withColumn/select/alias, drop/rename, filter, split, explode and grain changes](DB3-column-and-row-surgery.md) | [DE3: Grain control: flattening nested data into child tables, join fan-out traps, testing the grain](DE3-grain-control.md) |
| Thu 2026-10-29 | [DB4: Dedup and "latest record wins": row_number() windows vs dropDuplicates, and MERGE INTO Silver](DB4-dedup-latest-record-wins.md) | [DE4: SCD Type 1 vs Type 2 and when each fits](DE4-scd-type-1-vs-type-2.md) |
| Fri 2026-10-30 | DB5: Aggregations and profiling: count, countDistinct vs approx_count_distinct, summary/describe, picking a Gold grain | DE5: Incremental Silver-to-Gold with Delta Change Data Feed vs full recompute |
| Sat 2026-10-31 | DB6: Spark tuning knobs: shuffle partitions, parallelism, autoBroadcastJoinThreshold, AQE, shuffle and spill in the Spark UI | DE6: Data skew and file layout as an interview story: salting, partitioning vs liquid clustering |
| Sun 2026-11-01 | DB7: Recap: an end-to-end Bronze to Silver to Gold transform that's correct and re-runnable | DE7: Tech gap, dbt for Silver and Gold vs Lakeflow Declarative Pipelines |

## Deep dives (Zé Carlos)

- **Databricks, Mon 2026-10-26:** [Connection Deep Dive: Ingestion Decisions → One Re-runnable Bronze Pipeline](deepdive-db-ingestion-decisions-rerunnable-bronze.html) ([brief from Gerald](deepdive-db-ingestion-decisions-rerunnable-bronze-brief.md)). It covers the 2026-W43 study week (Ingestion into Bronze) and ends with a teaser for this week's theme.

The deep dives are self-contained HTML files. Download one and open it in a browser; GitHub's file view shows the source, not the rendered page.

## Connection

The Databricks track is the code that reshapes Bronze into Silver and Gold (casts, joins, explode, dedup, MERGE, tuning). The DE track is the design behind it (quality rules, grain, dimensional models, SCD, incremental Gold), so each day pairs a how with a why.
