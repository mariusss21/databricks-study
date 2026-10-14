# Schema enforcement vs. schema evolution on Delta

Schema enforcement and schema evolution are how Delta decides what to do with a write whose columns don't match the table: reject it, or accept it by widening the table. Enforcement is the default. Every append, overwrite, or merge is checked against the table schema, so a bad or drifted payload can't quietly land and break downstream readers. Evolution is the deliberate escape hatch for when you *want* new data to add columns.

## How it works under the hood

Delta stores the table schema in the transaction log's metadata. On every write, Spark compares the incoming DataFrame (or `MERGE` source) to that committed schema. Columns that can be safely cast are cast. If a required target column is missing, a type can't be cast, or there are extra columns while evolution is off, the write fails before any commit is created. When evolution is on for an additive write, Delta updates the schema in the same commit that adds the new Parquet files. Readers of the earlier version still see the old schema, and readers of the new version see the new columns, with nulls for older rows.

## The settings

- **Schema enforcement (the default):** Writes that don't match fail. This applies to Delta tables, not to loose CSV or JSON folders in cloud storage.
- **`mergeSchema` / `INSERT WITH SCHEMA EVOLUTION`:** Turns on additive evolution for batch or streaming writes, so new columns get appended to the schema. It doesn't drop columns or make arbitrary type changes.
- **`MERGE WITH SCHEMA EVOLUTION` (or `.withSchemaEvolution()`):** Does the same for `MERGE`, so new source columns can be inserted or updated into the target.
- **`overwriteSchema`:** Used with an overwrite to replace the table schema on purpose. It throws away the old schema, so treat it as a migration, not an everyday switch.
- **`spark.databricks.delta.schema.autoMerge.enabled`:** The legacy session-wide auto-merge. Prefer per-write `mergeSchema` or `WITH SCHEMA EVOLUTION` so evolution only affects the job that meant it.
- **Auto Loader `cloudFiles.schemaEvolutionMode`:** Controls evolution during file ingest, separately from Delta writes. The modes are `addNewColumns` (the usual default when inferring), `rescue`, `failOnNewColumns`, and `none`. Pair it with `cloudFiles.schemaLocation` so the inferred schema survives restarts.

## Example

Bronze JSON starts carrying a new `discount_code` field. With Auto Loader in `addNewColumns` mode, the first micro-batch that sees the field updates the schema location and stops once with `UnknownFieldException`. When the job restarts, it writes rows that include `discount_code`. On the Bronze Delta table, an append with `.option("mergeSchema", "true")` (or `INSERT WITH SCHEMA EVOLUTION`) commits the new files and the wider schema together. Silver and Gold stay on enforcement: a write still on the old contract either leaves out the new column on purpose or fails until you run an explicit `ALTER TABLE` and update the transformation.

## Gotchas

- **Evolution only adds.** Dropping or renaming columns needs column mapping, explicit DDL, or a controlled rewrite, not `mergeSchema`.
- **Type widening has its own rules** and table properties, so don't assume every type change is free.
- **Evolving Gold quietly breaks consumers.** BI dashboards and saved queries that expect a fixed set of columns can stop working. Bronze can be loose, but Silver and especially Gold should fail fast so you can review the field and migrate on purpose.
- **Session-wide autoMerge leaks.** It can surprise other notebooks running on the same cluster.
- **Auto Loader's stop-then-restart looks like an outage** if the job isn't set to retry automatically.

## For the exam

Delta enforces schema on write by default. `mergeSchema` and `WITH SCHEMA EVOLUTION` allow additive changes, `overwriteSchema` replaces the schema during an overwrite, and Auto Loader's `cloudFiles.schemaEvolutionMode` handles the same question at file ingest. Match how strict you are to the medallion layer instead of evolving everything blindly.
