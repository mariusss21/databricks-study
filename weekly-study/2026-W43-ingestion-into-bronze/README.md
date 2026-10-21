# 2026-W43: Ingestion into Bronze

**Theme:** Ingestion into Bronze

**Dates:** Mon 2026-10-19 to Sun 2026-10-25

Everything for this week lives in this folder: Gerald's daily Databricks pills (`DB1`..`DB7`), NJ in Dollar's daily Data Engineering pills (`DE1`..`DE7`), and Zé Carlos's two deep dives.

## Day by day

| Day | Databricks pill (Gerald) | DE pill (NJ in Dollar) |
|---|---|---|
| Mon 2026-10-19 | [DB1: Lakeflow Connect, managed vs standard connectors](DB1-lakeflow-connect.md) | [DE1: CDC fundamentals: log-based vs query-based, Debezium-style events, what managed connectors do for you](DE1-cdc-fundamentals.md) |
| Tue 2026-10-20 | [DB2: Auto Loader checkpoints, exactly-once, and trigger modes (availableNow vs continuous)](DB2-auto-loader-checkpoints-exactly-once-trigger-modes.md) | [DE2: Delivery semantics: at-least-once vs exactly-once, idempotency, batch vs streaming vs micro-batch](DE2-delivery-semantics-idempotency.md) |
| Wed 2026-10-21 | Landing nested or semi-structured JSON in Bronze (struct and VARIANT) | [DE3: Schema evolution and drift: schema-on-read vs schema-on-write, data contracts, Snowflake VARIANT vs Databricks VARIANT](DE3-schema-evolution-drift-variant.md) |
| Thu 2026-10-22 | JDBC, ODBC, and REST reads from notebooks, scheduled as Lakeflow Jobs | Incremental extraction: high-water marks, API pagination and rate limits, backfills |
| Fri 2026-10-23 | Choosing an ingestion tool: Connect vs Auto Loader vs COPY INTO vs JDBC | US modern data stack ingestion layer (Fivetran/Airbyte, Snowpipe/COPY INTO, dbt sources) mapped to Databricks |
| Sat 2026-10-24 | Bronze table design: append-only, _metadata file columns, ingest timestamps, rescued data | Dedup and replay: business keys, late/duplicate data, append-only Bronze vs MERGE on ingest |
| Sun 2026-10-25 | Recap: an end-to-end Bronze pipeline that's safe to re-run | System design: ingestion platform for 200+ sources |

Topics without a link don't have a pill file yet.

## Deep dives (Zé Carlos)

- **Databricks, Mon 2026-10-19:** [Connection Deep Dive: Governed Ingestion to a Trustworthy Bronze Table](deepdive-db-governed-ingestion-to-bronze.html) ([brief from Gerald](deepdive-db-governed-ingestion-to-bronze-brief.md))
- **Data engineering, Fri 2026-10-23:** not published yet.

The deep dives are self-contained HTML files. Download one and open it in a browser; GitHub's file view shows the source, not the rendered page.

## Connection

Each day, NJ's general DE concept is the 'why' behind the Databricks tool Gerald covers that day (CDC -> Lakeflow Connect, exactly-once -> Auto Loader checkpoints, drift -> VARIANT/rescued data, incremental extraction -> JDBC/REST jobs, MDS mapping -> tool choice, dedup -> Bronze design, system design -> end-to-end recap).
