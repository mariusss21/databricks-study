# Databricks Certified Data Engineer Associate — study plan v2

A lighter 8-week plan for Mario Carvalho. Exam window: **Tue–Thu, January 12–14, 2027**.

This folder is additive. Nothing in `weekly-study/`, `explanations/`, or `courses/` is modified or removed. The older pills and explainers stay as review material and are linked from the weeks below.

Official sources (exam published May 4, 2026; guide updated August 6, 2026):

- [Certification page](https://www.databricks.com/learn/certification/data-engineer-associate)
- [Exam guide PDF (August 2026)](https://www.databricks.com/sites/default/files/2026-10/Databricks-Certified-Data-Engineer-Associate-Exam-Guide-August-2026.pdf)

The exam is 45 scored multiple-choice questions, 90 minutes, no aids. Code is in SQL when possible, otherwise Python.

## Naming convention

Each week lives in a folder `W0N-topic/`. Files are named by weekday so a week reads in order:

| File | What it is |
|---|---|
| `00-mon-review.md` | Monday review of the previous week |
| `00-mon-diagnostic.md` | Week 1 only. Monday was a baseline diagnostic, because there was no previous week to review |
| `01-tue-<slug>.md` | Tuesday pill |
| `02-wed-<slug>.md` | Wednesday pill |
| `03-thu-<slug>.md` | Thursday pill |
| `04-fri-deepdive-<slug>.md` | Friday deep dive, Markdown |
| `04-fri-deepdive-<slug>.html` | The same deep dive as HTML |
| `recall/recall-*.md` | Evening recall answers, when that week has any |
| `assets/` | That week's images |
| `README.md` | Kept when a week has its own readme |

`<slug>` is the short kebab-case topic taken from the piece. Missed questions are logged in [`misses.md`](misses.md).

## Index

Link text is each file's title. Weeks 3–8 are on the schedule further down and do not have files yet.

### Week 1 — Foundations

- [Week 1 kickoff — baseline diagnostic](W01-foundations/00-mon-diagnostic.md)
- [W1 · P1 — Delta Lake, ACID and the lakehouse](W01-foundations/01-tue-delta-acid-and-the-lakehouse.md)
- [W1 · P2 — Compute choices and separating storage from compute](W01-foundations/02-wed-compute-and-storage-compute-separation.md)
- [W1 · P3 — Unity Catalog: managed vs external tables](W01-foundations/03-thu-unity-catalog-managed-vs-external-tables.md)
- [W1 · Friday deep dive: Anatomy of a governed table (v2)](W01-foundations/04-fri-deepdive-anatomy-of-a-governed-table.md)
- [Anatomy of a governed table](W01-foundations/04-fri-deepdive-anatomy-of-a-governed-table.html) (HTML)

### Week 2 — Ingestion

- [Monday review — Week 1: Foundations](W02-ingestion/00-mon-review.md)

[Missed questions](misses.md) — three so far, with a count by exam section.

## Weekly rhythm

Times are America/Fortaleza (UTC-3). Weekends are off.

| Day | What | Time |
|---|---|---|
| Monday | Review: 10-question quiz + one-page cheat sheet + 1 carried-over missed question (spaced repetition) | ~15–20 min |
| Tuesday, Wednesday, Thursday | One pill that blends an exam topic with a fundamental (architecture, modeling, or data engineering in general) | 15–20 min |
| Friday | Scenario deep dive, with steps tagged by official exam section | 40–60 min |
| Saturday, Sunday | Off | — |

**Week 1 is the exception on Monday:** there is no previous week to review, so Monday is a short baseline diagnostic instead of the usual review.

**Weeks 6 and 7 are light** (Christmas and New Year): Monday quiz only, two pills, no Friday deep dive, Thursday–Sunday off.

## Official exam sections

| # | Section | Weight | ~Questions |
|---|---|---|---|
| 1 | Databricks Intelligence Platform | 6% | ~3 |
| 2 | Data Ingestion and Loading | 21% | ~9–10 |
| 3 | Data Transformation and Modeling | 22% | ~10 |
| 4 | Working with Lakeflow Jobs | 16% | ~7 |
| 5 | Implementing CI/CD | 10% | ~4–5 |
| 6 | Troubleshooting, Monitoring, and Optimization | 10% | ~4–5 |
| 7 | Governance and Security | 15% | ~7 |

## The 8 weeks

### Week 1 — Nov 16–22, 2026 — Foundations

Delta Lake, ACID, and the lakehouse; compute choices and storage/compute separation; Unity Catalog managed vs external tables.

- **Mon 16:** Baseline diagnostic ([Week 1 kickoff — baseline diagnostic](W01-foundations/00-mon-diagnostic.md)) — 10 questions across all 7 exam sections. Reply with answers like `1B 2C ...` so misses can be tracked in [`misses.md`](misses.md).
- **Tue:** Delta Lake: transaction log, ACID, time travel, paired with warehouse vs data lake vs lakehouse (what ACID actually guarantees).
- **Wed:** Compute: serverless, classic jobs/all-purpose, SQL warehouses; limits and cost, paired with storage/compute separation and elasticity.
- **Thu:** Unity Catalog: metastore → catalog → schema → object; managed vs external tables (create, alter, drop, convert), paired with metadata, ownership, and lifecycle.
- **Fri deep dive:** "Anatomy of a governed table" — one table from file to UC object, including volumes and external locations.
- **Sat–Sun:** Off.

Older material: [ACID on Delta](../explanations/acid-on-delta.md), [Delta time travel and retention](../explanations/delta-time-travel-and-retention.md), [Platform wrap: Delta, Unity Catalog, and compute](../explanations/platform-delta-unity-catalog-compute.md), [Spark parallelism on single-node and serverless](../explanations/spark-parallelism-single-node-and-serverless.md), [Unity Catalog volumes, storage credentials, and external locations](../explanations/unity-catalog-volumes-and-external-locations.md), [Medallion architecture](../explanations/medallion-architecture.md).

### Week 2 — Nov 23–29, 2026 — Ingestion

COPY INTO, Auto Loader, and Lakeflow Connect. Mostly review of work already in the repo.

- **Mon:** Review of Week 1 ([Monday review — Week 1: Foundations](W02-ingestion/00-mon-review.md)).
- **Tue:** Batch vs streaming vs incremental; COPY INTO, paired with idempotent loads and delivery semantics.
- **Wed:** Auto Loader: schema inference, enforcement, evolution, rescued data, directory listing vs file notification, paired with schema-on-read vs schema-on-write and data contracts.
- **Thu:** Lakeflow Connect standard vs managed connectors; JDBC/ODBC/REST from notebooks scheduled by Jobs, paired with CDC and API constraints.
- **Fri deep dive:** "Pick the right door" — a decision matrix (Auto Loader / COPY INTO / Connect / partner / JDBC) plus semi-structured JSON → struct/VARIANT.
- **Sat–Sun:** Off.

Older material: [W43 Ingestion into Bronze](../weekly-study/2026-W43-ingestion-into-bronze/README.md), especially [COPY INTO / tool choice](../weekly-study/2026-W43-ingestion-into-bronze/DB5-choosing-an-ingestion-tool.md), [Auto Loader](../weekly-study/2026-W43-ingestion-into-bronze/DB2-auto-loader-checkpoints-exactly-once-trigger-modes.md), [Lakeflow Connect](../weekly-study/2026-W43-ingestion-into-bronze/DB1-lakeflow-connect.md), [JDBC/ODBC/REST](../weekly-study/2026-W43-ingestion-into-bronze/DB4-jdbc-odbc-rest-reads-lakeflow-jobs.md), [nested JSON / VARIANT](../weekly-study/2026-W43-ingestion-into-bronze/DB3-nested-json-struct-variant.md), plus [schema enforcement vs evolution](../explanations/schema-enforcement-and-evolution.md) and [CDC fundamentals](../weekly-study/2026-W43-ingestion-into-bronze/DE1-cdc-fundamentals.md).

### Week 3 — Nov 30–Dec 6, 2026 — Bronze → Silver

Data quality, joins, normalization. Friday goes deep on star schema and SCDs.

- **Mon:** Review of Week 2.
- **Tue:** Cleaning: read Bronze, nulls, casts, timestamps, write Silver, paired with data quality dimensions (completeness, validity, consistency).
- **Wed:** Joins (inner, left, cross, multi-key, broadcast) and union vs union all, paired with cardinality, fan-out, and grain.
- **Thu:** Column and row operations (add, drop, rename, split, filter, explode), paired with normalization vs denormalization.
- **Fri deep dive:** "From Silver to a star schema" — dedup, aggregations, dimensional modeling, business keys, SCD Type 1 vs Type 2.
- **Sat–Sun:** Off.

Older material: [W44 Bronze to Silver](../weekly-study/2026-W44-bronze-to-silver-transforms/README.md), especially [cleaning](../weekly-study/2026-W44-bronze-to-silver-transforms/DB1-bronze-to-silver-cleaning.md), [join types](../weekly-study/2026-W44-bronze-to-silver-transforms/DB2-join-types.md), [column and row surgery](../weekly-study/2026-W44-bronze-to-silver-transforms/DB3-column-and-row-surgery.md), [dedup](../weekly-study/2026-W44-bronze-to-silver-transforms/DB4-dedup-latest-record-wins.md), [aggregations](../weekly-study/2026-W44-bronze-to-silver-transforms/DB5-aggregations-and-profiling.md), [dimensional modeling](../weekly-study/2026-W44-bronze-to-silver-transforms/DE2-dimensional-modeling-basics.md), and [SCD Type 1 vs Type 2](../weekly-study/2026-W44-bronze-to-silver-transforms/DE4-scd-type-1-vs-type-2.md).

### Week 4 — Dec 7–13, 2026 — Gold and Lakeflow Declarative Pipelines

Gold objects and quality checks during the week; Friday is Spark UI and tuning.

- **Mon:** Review of Week 3.
- **Tue:** Gold objects in Unity Catalog: table vs view vs materialized view vs streaming table, paired with materialization trade-offs.
- **Wed:** Data quality checks: constraints, expectations (warn / drop / fail), quarantine, paired with contracts and observability.
- **Thu:** Lakeflow Spark Declarative Pipelines: flows, streaming tables vs materialized views, triggered vs continuous, event log, paired with declarative vs imperative pipelines.
- **Fri deep dive:** "Why is it slow?" — tuning parameters (`spark.sql.shuffle.partitions`, `spark.default.parallelism`, executor/driver memory, `spark.sql.autoBroadcastJoinThreshold`) and re-measuring; skew, shuffle, and spill in the Spark UI; liquid clustering and predictive optimization.
- **Sat–Sun:** Off.

Older material: [W46 Gold objects and Jobs](../weekly-study/2026-W46-gold-objects-and-jobs/README.md), especially [Gold layer objects](../weekly-study/2026-W46-gold-objects-and-jobs/DB3-gold-layer-objects.md), [data quality expectations](../weekly-study/2026-W46-gold-objects-and-jobs/DB4-data-quality-expectations-quarantine.md), [Spark tuning knobs](../weekly-study/2026-W46-gold-objects-and-jobs/DB1-spark-tuning-knobs.md), [skew, salting, and liquid clustering](../weekly-study/2026-W46-gold-objects-and-jobs/DE1-data-skew-salting-and-liquid-clustering.md), [W47 streaming tables vs materialized views](../weekly-study/2026-W47-lakeflow-pipelines-and-jobs/DB1-sdp-streaming-tables-vs-mvs.md), plus [AQE](../explanations/adaptive-query-execution.md) and [columnar storage, updates, and clustering](../explanations/columnar-storage-updates-and-clustering.md).

### Week 5 — Dec 14–20, 2026 — Lakeflow Jobs

Jobs building blocks during the week; Friday is troubleshooting a failing job.

- **Mon:** Review of Week 4.
- **Tue:** Task types (notebook, SQL query, dashboard, pipeline) and DAG dependencies, paired with orchestration fundamentals (and Airflow parallels).
- **Wed:** Control flow: retries, if/else, for-each, task values, parameters, paired with failure handling, safe-to-retry design, and backfills.
- **Thu:** Triggers: scheduled vs file arrival vs table update; time-based vs data-driven, paired with event-driven scheduling and SLAs.
- **Fri deep dive:** "On call for a Job" — run history vs baseline, Jobs UI statuses, upstream blockers, repair runs; cluster startup failures, library conflicts, OOM.
- **Sat–Sun:** Off.

Older material: [Lakeflow Jobs basics](../weekly-study/2026-W46-gold-objects-and-jobs/DB5-lakeflow-jobs-basics.md), [job triggers and repair runs](../weekly-study/2026-W46-gold-objects-and-jobs/DB6-job-triggers-and-repair-runs.md), and [Airflow vs Lakeflow Jobs](../weekly-study/2026-W46-gold-objects-and-jobs/DE5-orchestration-airflow-vs-lakeflow-jobs.md). Looping, if/else, dashboard/SQL tasks, run-history diagnosis, and cluster startup issues were planned in W47 but never written — this week fills that gap.

### Week 6 — Dec 21–27, 2026 — Governance (light)

Christmas week: two pills, no deep dive.

- **Mon 21:** Short review (quiz only).
- **Tue 22:** Privileges: GRANT / REVOKE / DENY to users, groups, and service principals; inheritance through the UC hierarchy, paired with least privilege and RBAC.
- **Wed 23:** Row filters, column masks, and Unity Catalog ABAC policies, paired with RBAC vs ABAC and PII handling.
- **Thu–Sun:** Off. Optional, only if you feel like it: a governed medallion pass with grants, masks, and ABAC.

Older material is thin here on purpose — Governance is a known gap. The useful background is [Unity Catalog volumes and external locations](../explanations/unity-catalog-volumes-and-external-locations.md) and the platform wrap above.

### Week 7 — Dec 28, 2026–Jan 3, 2027 — CI/CD (light)

New Year week: two pills, no deep dive. This section was barely covered in the old plan.

- **Mon 28:** Short review (quiz only).
- **Tue 29:** Git folders (formerly Repos): branches, commit/push, pull requests in the workspace, paired with branching strategies and code review.
- **Wed 30:** Declarative Automation Bundles (formerly Databricks Asset Bundles): `databricks.yml`, variables, targets and overrides (dev/test/prod), and `databricks bundle validate` / `deploy` / `run` with the CLI, paired with infrastructure as code and promoting through environments.
- **Thu–Sun:** Off. Optional, only if you feel like it: ship a pipeline to prod (bundle + GitHub Actions).

### Week 8 — Jan 4–10, 2027 — Mocks and gap-filling

- **Mon:** Review of Weeks 6–7 plus a one-page "everything" cheat sheet.
- **Tue:** Mock exam #1 (45 questions / 90 minutes, weighted by official section), then a mistakes review.
- **Wed:** One targeted gap pill built from the two weakest sections in Mock #1.
- **Thu:** Mock exam #2 (new questions, same weights).
- **Fri:** Review of every miss, plus a "renamed products" trap list (Declarative Automation Bundles, Git folders, Lakeflow Declarative Pipelines, Lakeflow Jobs).
- **Sat–Sun:** Off. Optional light quiz only if you want it.

**Exam:** book it for **Tue–Thu, January 12–14, 2027**. Monday Jan 11 is a short warm-up quiz only — no cramming.

## Week-to-section map

How each week lines up with the official outline. "Primary" is where the new pills spend their time; "review" means the older repo material is the main source.

| Week | Dates | Theme | Sec 1 (6%) | Sec 2 (21%) | Sec 3 (22%) | Sec 4 (16%) | Sec 5 (10%) | Sec 6 (10%) | Sec 7 (15%) |
|---|---|---|---|---|---|---|---|---|---|
| W1 | Nov 16–22 | Foundations | **Primary** | — | — | — | — | — | **Primary** (managed vs external) |
| W2 | Nov 23–29 | Ingestion | — | **Primary** (review of W43) | — | — | — | — | — |
| W3 | Nov 30–Dec 6 | Bronze → Silver | — | — | **Primary** | — | — | — | — |
| W4 | Dec 7–13 | Gold and Declarative Pipelines | — | — | **Primary** | — | — | **Deep dive** (Spark UI, tuning) | — |
| W5 | Dec 14–20 | Lakeflow Jobs | — | — | — | **Primary** | — | **Deep dive** (failing job) | — |
| W6 | Dec 21–27 | Governance (light) | — | — | — | — | — | — | **Primary** (privileges, masks, ABAC) |
| W7 | Dec 28–Jan 3 | CI/CD (light) | — | — | — | — | **Primary** | — | — |
| W8 | Jan 4–10 | Mocks | Weighted practice across all seven sections | | | | | | |

Sections 2 and 3 are already well covered in the old weekly folders, so those weeks are review-heavy. New effort is concentrated on the known gaps: Governance (15%), CI/CD (10%), Troubleshooting (10%), and the rest of Jobs (control flow, trigger choice, and incident diagnosis).

## Folder convention for this plan

```
study-plan-v2/
  README.md                          this file
  misses.md                          missed questions, by exam section
  W01-foundations/
    00-mon-diagnostic.md             Week 1 Monday (diagnostic, not a review)
    01-tue-<slug>.md                 Tuesday pill
    02-wed-<slug>.md                 Wednesday pill
    03-thu-<slug>.md                 Thursday pill
    04-fri-deepdive-<slug>.md        Friday scenario (Markdown)
    04-fri-deepdive-<slug>.html      Friday scenario (HTML)
    assets/                          this week's images
    recall/                          evening recall answers, when any exist
  W02-ingestion/
    00-mon-review.md                 Monday review of the previous week
  ...
```

Pills and deep dives are added week by week. You do not need a full folder on day one. The [index](#index) lists every piece that exists, and [Naming convention](#naming-convention) says what each filename means.
