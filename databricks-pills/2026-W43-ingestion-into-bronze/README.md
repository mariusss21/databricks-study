# 2026-W43: Ingestion into Bronze

**Theme:** Ingestion into Bronze

**Dates:** Mon 2026-10-19 to Sun 2026-10-25

| Day | Databricks topic (Gerald) | Paired DE topic (NJ in Dollar) |
|---|---|---|
| Mon 2026-10-19 | [Lakeflow Connect, managed vs standard connectors](01-mon-2026-10-19-lakeflow-connect.md) | CDC fundamentals: log-based vs query-based, Debezium-style events, what managed connectors do for you |
| Tue 2026-10-20 | [Auto Loader checkpoints, exactly-once, and trigger modes (availableNow vs continuous)](02-tue-2026-10-20-auto-loader-checkpoints-exactly-once-trigger-modes.md) | Delivery semantics: at-least-once vs exactly-once, idempotency, batch vs streaming vs micro-batch |
| Wed 2026-10-21 | Landing nested or semi-structured JSON in Bronze (struct and VARIANT) | Schema evolution and drift: schema-on-read vs schema-on-write, data contracts, Snowflake VARIANT vs Databricks VARIANT |
| Thu 2026-10-22 | JDBC, ODBC, and REST reads from notebooks, scheduled as Lakeflow Jobs | Incremental extraction: high-water marks, API pagination and rate limits, backfills |
| Fri 2026-10-23 | Choosing an ingestion tool: Connect vs Auto Loader vs COPY INTO vs JDBC | US modern data stack ingestion layer (Fivetran/Airbyte, Snowpipe/COPY INTO, dbt sources) mapped to Databricks |
| Sat 2026-10-24 | Bronze table design: append-only, _metadata file columns, ingest timestamps, rescued data | Dedup and replay: business keys, late/duplicate data, append-only Bronze vs MERGE on ingest |
| Sun 2026-10-25 | Recap: an end-to-end Bronze pipeline that's safe to re-run | System design: ingestion platform for 200+ sources |

Topics without a link don't have a pill file yet.

**Connection:** Each day, NJ's general DE concept is the 'why' behind the Databricks tool Gerald covers that day (CDC -> Lakeflow Connect, exactly-once -> Auto Loader checkpoints, drift -> VARIANT/rescued data, incremental extraction -> JDBC/REST jobs, MDS mapping -> tool choice, dedup -> Bronze design, system design -> end-to-end recap).
