# 2026-W47: Lakeflow pipelines and Jobs in depth

**Theme:** Lakeflow pipelines and Jobs in depth (second Lakeflow week)

**Dates:** Mon 2026-11-16 to Sun 2026-11-22

Everything for this week lives in this folder: Gerald's daily Databricks pills (`DB1`..`DB7`), NJ in Dollar's daily Data Engineering pills (`DE1`..`DE7`), and Zé Carlos's deep dives.

## Day by day

Pill files get linked here as they land.

| Day | Databricks pill (Gerald) | DE pill (NJ in Dollar) |
|---|---|---|
| Mon 2026-11-16 | [DB1: Lakeflow Spark Declarative Pipelines: streaming tables vs materialized views, flows, triggered vs continuous](DB1-sdp-streaming-tables-vs-mvs.md) | [DE1: Streaming vs batch as a design decision (and when NOT to stream)](DE1-streaming-vs-batch-design-decision.md) |
| Tue 2026-11-17 | [DB2: Pipeline expectations in practice and the pipeline event log](DB2-expectations-and-event-log.md) | [DE2: Testing data pipelines as code (unit, contract, integration, and CI)](DE2-testing-pipelines-as-code.md) |
| Wed 2026-11-18 | [DB3: AUTO CDC for SCD Type 1 and Type 2 inside pipelines](DB3-auto-cdc-scd1-scd2.md) | [DE3: CDC end to end (capture, ordering, deletes, idempotent apply)](DE3-cdc-end-to-end.md) |
| Thu 2026-11-19 | [DB4: Lakeflow Jobs deeper: parameters, task values, for-each, if/else, pipeline task](DB4-jobs-parameters-task-values-for-each.md) | [DE4: Metadata-driven pipelines (config-driven frameworks, and when they're over-engineering)](DE4-metadata-driven-pipelines.md) |
| Fri 2026-11-20 | [DB5: Monitoring and troubleshooting Lakeflow Jobs](DB5-monitoring-and-troubleshooting-jobs.md) | [DE5: Incident and on-call stories (detect, mitigate, communicate, learn)](DE5-incident-and-on-call-stories.md) · [Deep dive: 40 Postgres tables, one config: CDC to SCD2 Silver with a config-driven Lakeflow pipeline, tested and orchestrated](deepdive-de-config-driven-cdc-pipeline.html) |
| Sat 2026-11-21 | Lakeflow Connect: managed connectors | [DE6: Ingestion patterns (full vs incremental, high-water marks, APIs, and build vs buy)](DE6-ingestion-patterns-high-water-marks.md) |
| Sun 2026-11-22 | Recap: Connect to Declarative Pipelines to Jobs | System design capstone: near-real-time CDC Postgres to Gold |

## Deep dives (Zé Carlos)

Linked here as they land.

- **Data engineering, Fri 2026-11-20:** [Connection Deep Dive: 40 Postgres tables, one config: CDC to SCD2 Silver with a config-driven Lakeflow pipeline, tested and orchestrated](deepdive-de-config-driven-cdc-pipeline.html) ([brief from NJ in Dollar](deepdive-de-config-driven-cdc-pipeline-brief.md)). It connects this week's DE pills DE1–DE4 (CDC end to end, metadata-driven pipelines, testing pipelines as code, and streaming vs batch) into one system: ~40 Postgres tables via Debezium into a config-driven Lakeflow Declarative Pipeline (AUTO CDC SCD2 with expectations), run by a Lakeflow Job with If/else and For each, guarded by pytest and bundle CI, and feeding Gold through Silver's change data feed. Hands-on: an in-browser YAML config checker and code generator, a CDC ordering puzzle, and a Databricks Free Edition lab.

The deep dives are self-contained HTML files. Download one and open it in a browser; GitHub's file view shows the source, not the rendered page.

## Weekly review

- **Weekly review, Fri 2026-11-20:** [Cheat sheet](cheatsheet.html) (one A4 landscape page; open it in a browser and print, or use the PDF sent in chat) · [Definitions quiz](definitions-quiz.html) (24 multiple-choice definitions across Databricks, AI Engineering and Data Engineering).
