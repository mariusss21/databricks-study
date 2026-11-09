# Databricks deep-dive brief for Monday 2026-11-09

From Gerald, for Zé Carlos. This is the first lesson after Mario's house-move pause (Oct 31 to Nov 8). There's no weekly plan for Nov 9–15, so this brief closes the theme from the last active week, W44 (Oct 26 – Nov 1, "Bronze to Silver transforms"). Mario got the DB1–DB5 pills that week. The Sat pill (Spark tuning knobs) and the Sun recap never went out, and they'll be the daily pills on Mon and Tue this week. Sources: /home/box/agent-data/dea/weekly-plans/2026-10-26.md and the DB1–DB5 files in mariusss21/databricks-study under weekly-study/2026-W44-bronze-to-silver-transforms/.

## Scope notes
- Mario has had a week off and is catching up on a work backlog and McKinsey Forward. Aim for the short end, about 45–60 minutes, and treat the lesson as a re-entry review instead of new ground.
- Recap first, then connect. Don't go deep on tuning, because the Mon pill covers the knobs. A short bridge is enough.
- He may move the exam from Dec 18, 2026 to January 2027 (not confirmed). Either way, a full-pipeline review now is good timing.

## Topics (all from W44)
1. Bronze-to-Silver cleaning (DB1): casts vs `try_cast`, `coalesce`, `na.fill` / `na.drop`, and parsing timestamps. Bad values become nulls, not failed jobs.
2. Joins and unions (DB2): the join types (inner, left, semi, anti), broadcast joins for small dimensions, and `union` (by position) vs `unionByName` (by name).
3. Column and row surgery (DB3): `select` / `withColumn`, `split`, `explode` vs `explode_outer`, and how exploding changes the table's grain (one row per what?).
4. Dedup and upsert (DB4): latest record wins with `row_number()` over a window or `QUALIFY`, vs `dropDuplicates`, then `MERGE INTO` into Silver so re-runs are idempotent.
5. Aggregations and profiling into Gold (DB5): `count(*)` vs `count(col)`, `countDistinct` vs `approx_count_distinct`, `describe` / `summary`, and declaring Gold's grain before you aggregate.

## How they connect
Together these five topics are one pipeline: Bronze (raw, append-only, from the previous week) goes to clean, typed Silver (1), gets enriched (2) and reshaped (3), is deduplicated and merged so it's safe to re-run (4), and is then aggregated into Gold at a declared grain (5). Grain ties it all together: explode multiplies rows, joins can fan them out, dedup restores one row per key, and Gold picks a new grain on purpose. Profiling counts (5) are how you check every step didn't silently change the grain. The tuning bridge: broadcast (2) and shuffles from window functions, joins and aggregations (4, 5) are where AQE and shuffle partitions matter. That's Monday's pill.

## Suggested shape
- Recap: one card per topic with the single rule to remember.
- Connection map: Bronze to Silver to Gold, with each topic placed on the arrow where it happens and a grain label at each stage.
- End-to-end walkthrough: one small dataset (for example orders with a nested items array plus a customers dimension), run through every step, with a row count after each one.
- Hands-on (short): Mario runs it in a notebook. Run the MERGE twice to show idempotency, and compare the counts before and after explode and dedup.
- Exam-style questions, 5–6, SQL-first like the exam: union vs unionByName, explode vs explode_outer, ROW_NUMBER/QUALIFY vs dropDuplicates, MERGE semantics, count(*) vs count(col).

## Official sources for going deeper
- [Medallion architecture](https://docs.databricks.com/aws/en/lakehouse/medallion)
- [try_cast](https://docs.databricks.com/aws/en/sql/language-manual/functions/try_cast)
- [explode_outer](https://docs.databricks.com/aws/en/sql/language-manual/functions/explode_outer)
- [QUALIFY clause](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-qualify)
- [MERGE INTO](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into)
- [approx_count_distinct](https://docs.databricks.com/aws/en/sql/language-manual/functions/approx_count_distinct)
- [Adaptive query execution](https://docs.databricks.com/aws/en/optimizations/aqe) (bridge to tuning only)
- [Spark SQL performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html) (bridge to tuning only)

## Repo filename
Save it in mariusss21/databricks-study as `weekly-study/2026-W44-bronze-to-silver-transforms/deepdive-db-bronze-to-silver-to-gold-recap.html`, since it closes W44, and add it to that week's README.
