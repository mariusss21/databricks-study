# Databricks deep-dive brief for Monday 2026-10-19

From Gerald, for Zé Carlos. Covers Mario's Databricks study for Oct 12–18, 2026.

## Topics
1. Governed landing zones: Unity Catalog's three-level names (catalog.schema.table or volume), volumes for non-tabular files, and external locations plus storage credentials for cloud paths. Grants that matter here are READ VOLUME / WRITE VOLUME on volumes and READ FILES on external locations. Managed tables lose their data files when dropped, and external tables keep them.
2. Batch vs incremental file ingestion: `COPY INTO` (idempotent, skips files already loaded, good for backfills and periodic drops) vs Auto Loader (`cloudFiles`, checkpoint-backed exactly-once). Auto Loader finds files by directory listing, which rescans the folder every run so cost grows with its size, or by file notification / managed file events, which Databricks recommends for high-volume production paths. You can switch modes across stream restarts without a full reload. `Trigger.AvailableNow` runs Auto Loader as a batch: it handles what had arrived when the query started, then stops.
3. Schema contracts on the way in: Delta schema enforcement vs intentional evolution, and Auto Loader's `cloudFiles.schemaLocation` with `cloudFiles.schemaEvolutionMode` (`addNewColumns`, the default, stops the stream once and then resumes on restart; also `rescue`, `failOnNewColumns`, `none`), the rescued data column, and sample-based inference stored under `_schemas`. Layer rule from the pills: Bronze may evolve, Silver stricter, and Gold never widens silently.

## How they connect
It's one path from a file landing to a trustworthy Bronze table. Unity Catalog decides where the file may land and who can read it, so Auto Loader and COPY INTO should read from volume or external-location paths, not mounts. If a job can't list the path, the first thing to check is the grants. Next comes picking the loader: COPY INTO or Auto Loader, which discovery mode, and which trigger. Then the schema settings decide what happens when the file's shape changes. The Oct 14 platform wrap ties it together: Delta gives reliability and rollback, Unity Catalog gives the governed single source of truth, and the compute choice (Oct 15 pill, which covered job compute vs all-purpose vs serverless) sets the cost of running all of it.

## Where to go a bit deeper
- An end-to-end walkthrough: a file goes into a UC volume, Auto Loader reads it with `schemaLocation` and a checkpoint under `availableNow`, it lands in a Bronze Delta table, then a new JSON field shows up and the stream handles it under `addNewColumns` and then under `rescue`.
- COPY INTO vs Auto Loader decision drill (volume, frequency, file count), matching the exam's ingestion-choice framing in the Oct 14 knowledge pill.
- Hands-on via the Databricks CLI Mario uses daily: create or inspect a volume, upload a sample file, and run the notebook as a Lakeflow Job with restart, so an `addNewColumns` stop recovers on its own.
- Optional link to his Structured Streaming course (courses/structured-streaming/ in mariusss21/databricks-study) for checkpoints and triggers.

## Why it matters for Mario
For the Associate exam on 2026-12-18, ingestion choices, Auto Loader, COPY INTO, Unity Catalog objects, and managed vs external tables show up in this week's exam-format pills, often combined in one scenario. For a senior DE role in the US, being able to explain a governed, re-runnable, drift-tolerant ingestion layer is a standard system-design expectation. It also sets up this week's theme (Oct 19–25, Ingestion into Bronze), which runs from Lakeflow Connect on Monday through Auto Loader checkpoints and triggers, VARIANT for JSON, and tool choice, to an end-to-end Bronze recap on Sunday. Keep Monday's deep dive on last week's synthesis and leave Lakeflow Connect details to the daily pill.

Sources: /home/box/agent-data/dea/knowledge-pills.md and technical-pills.md (2026-10-12..18), /home/box/agent-data/dea/weekly-plans/2026-10-19.md, mariusss21/databricks-study explanations (schema enforcement and evolution, UC volumes and external locations, platform Delta/UC/compute).
