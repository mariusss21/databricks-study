# Friday DE deep dive brief: 2026-11-20 (W47, "Lakeflow pipelines and Jobs in depth")

**For:** Zé Carlos | **From:** NJ in Dollar | **Learner:** Mario (Senior/AI Data Engineer interviews, US market)
**Format:** single-file, self-paced interactive HTML (no LMS), 45-90 min: recap, connection map, end-to-end walkthrough, hands-on, interview questions.
**Repo destination:** `mariusss21/databricks-study` → `weekly-study/2026-W47-lakeflow-pipelines-and-jobs/deepdive-de-config-driven-cdc-pipeline.html`, linked from that week's README (Fri row, DE column).

## Chosen topics (from this week's DE pills)
1. **CDC end to end** (DE3, Wed): log-based vs query-based capture, ordering by LSN/sequence, duplicates and idempotent apply, deletes/tombstones, late events.
2. **Metadata-driven pipelines** (DE4, Thu): one config (YAML in Git or a UC control table) drives N sources; pipeline metaprogramming loop vs Job For-each fan-out; when a framework becomes over-engineering.
3. **Testing pipelines as code** (DE2, Tue): pure DataFrame functions + pytest unit tests, contract/schema tests on the config, integration in a dev workspace, runtime expectations.
4. *Light thread:* **Streaming vs batch** (DE1, Mon): the same pipeline run as `availableNow` on a schedule vs continuous, and how to justify the choice.

**Why together:** they are one real system. "We have 40 Postgres tables to land as SCD2 Silver" is a CDC problem (correctness), solved at scale with metadata (one pattern, many tables), made safe with tests (a config bug breaks 40 tables at once), and run at the right latency/cost (streaming vs batch).

## Through-line scenario
A mid-size US e-commerce company replicates its Postgres OLTP (orders, customers, products, ... ~40 tables) via Debezium to Kafka/cloud storage. Build:
1. **Config:** `sources.yml` (table, keys, sequence column, SCD type 1/2, delete marker, columns to exclude, freshness SLA). Validated by a contract test (schema of the config itself).
2. **Bronze:** Auto Loader / Kafka streaming tables, append-only raw change events.
3. **Silver:** a Python loop in a Lakeflow Declarative Pipeline creates, per config entry, a target streaming table + `create_auto_cdc_flow(keys=..., sequence_by=lsn, apply_as_deletes=op='d', stored_as_scd_type=2)`. Show the closure gotcha (bind loop vars via function args).
4. **Quality:** expectations on Bronze (non-null keys, valid op codes), surfaced in the event log.
5. **Orchestration:** a Lakeflow Job: pipeline task (triggered, availableNow-style) → For-each over "Gold models" using task values → if/else on freshness check. Discuss when to switch the pipeline to continuous.
6. **Tests + CI:** pytest on the pure transform functions (dedupe by key keeping max LSN, delete handling) with `assertDataFrameEqual`; bundle `validate` + deploy to dev + integration run in CI.
7. **Downstream:** Change Data Feed on Silver feeds incremental Gold.

Hands-on idea: an in-browser exercise where Mario edits a small YAML config (add a table, mark SCD2, choose a sequence column) and the lesson "generates" the pipeline code and flags errors (missing key, non-monotonic sequence column, SCD2 without sequence). Plus an ordering puzzle: shuffled CDC events (insert/update/delete, one late, one duplicate) → predict the final SCD2 table.

## Databricks angle per topic (ties to Gerald's DB1-DB4 this week)
- CDC → `AUTO CDC` / `create_auto_cdc_flow` (formerly APPLY CHANGES) with SEQUENCE BY, APPLY AS DELETE, SCD1/2 (Gerald DB3); `MERGE INTO` with an LSN guard as the hand-rolled alternative; Delta Change Data Feed downstream.
- Metadata-driven → Python metaprogramming inside Declarative Pipelines; Jobs parameters, task values, For-each, if/else (Gerald DB4); Unity Catalog table as control table.
- Testing → expectations + event log (Gerald DB2) as runtime checks; Declarative Automation Bundles for deploy/CI.
- Streaming vs batch → streaming tables vs materialized views, triggered vs continuous (Gerald DB1).

## Tool gaps (Snowflake / dbt sidebars)
- **Snowflake:** Streams (change tracking on a table) + Tasks (scheduled MERGE) or Dynamic Tables (declarative, target lag) ≈ CDF + AUTO CDC + triggered pipelines. Metadata-driven in Snowflake often = stored procedures/Snowpark reading a config table.
- **dbt:** snapshots (`strategy: timestamp/check`) ≈ SCD2; incremental models with `unique_key` + merge ≈ MERGE INTO; dbt is itself a metadata-driven framework (YAML sources/models, Jinja macros, `dbt test`, CI with `state:modified`). Good contrast for "don't build your own framework if dbt/Lakeflow already does it".

## Go deeper (verified to load)
- AUTO CDC in Lakeflow Declarative Pipelines: https://docs.databricks.com/aws/en/ldp/cdc
- Jobs For each task: https://docs.databricks.com/aws/en/jobs/tasks/for-each
- Delta Change Data Feed: https://docs.databricks.com/aws/en/tables/features/change-data-feed
- Snowflake Streams: https://docs.snowflake.com/en/user-guide/streams-intro
- (Optional) Declarative Automation Bundles: https://docs.databricks.com/aws/en/dev-tools/bundles/

## Interview angle (US senior DE)
- **Q1 (system design):** "Design ingestion of 50 OLTP tables into a lakehouse with full history, deletes, and a 15-minute freshness SLA." Strong answer: log-based CDC (why not query-based: misses deletes, load on source), ordering by LSN not ingest time, idempotent apply (dedupe + sequence guard), SCD2 only where history is needed, config-driven pattern with per-table overrides, triggered every 10-15 min instead of continuous (cost), expectations + freshness alerts, backfill/re-snapshot plan, schema evolution policy.
- **Q2 (trade-off):** "When is a metadata-driven framework a bad idea?" Strong answer: few heterogeneous sources, logic that doesn't fit config (inner-platform effect), debugging/onboarding cost; keep it thin (80/20), escape hatches for custom code, test the config with contracts, prefer platform features (AUTO CDC, dbt) over custom engines.
- Tech-lead framing: Mario can tie this to his own experience (Python/AWS pipelines at Stack Tecnologias, the Airflow-based code factory) with a short STAR example; add one English phrasing tip box ("The trade-off here is...", "I'd start with X and only move to Y if...").

## Notes for Zé Carlos
- Synthesis, not re-teaching: recap each pill in a few lines, spend the time on the connections and the scenario.
- Saturday/Sunday pills (ingestion patterns, CDC capstone system design) will build on this, so end with a short "what's next" teaser.
