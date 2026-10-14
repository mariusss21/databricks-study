# Unity Catalog volumes, storage credentials, and external locations

A volume is a Unity Catalog object for files that aren't tables, like raw landing drops, CSV exports, images, or ML artifacts. It lives in the same `catalog.schema` hierarchy as tables, so file access gets the same grants, auditing, and lineage instead of relying on cloud keys hidden in notebooks.

## How it works

Three objects work together. A **storage credential** wraps a long-lived cloud identity: an IAM role on AWS, an access connector (managed identity) on Azure, or a service account on GCP. An **external location** pairs that credential with one cloud path, such as `s3://acme-landing/`, so Unity Catalog can reach anything under that prefix. A **volume** then exposes a directory as `/Volumes/<catalog>/<schema>/<volume>/...`. When a notebook or job reads that path, Unity Catalog checks the user's privileges and hands out short-lived credentials for the storage. Nobody sees the underlying key.

## Variants and privileges

- **Managed volume:** files sit in the schema's or catalog's managed storage. Dropping it deletes the files, just like dropping a managed table.
- **External volume:** registered on a directory inside an external location. Dropping it removes only the catalog entry, and the files stay put.
- **Volume privileges:** `READ VOLUME` and `WRITE VOLUME`, plus `USE CATALOG` and `USE SCHEMA` on the parents. Creating one needs `CREATE VOLUME` on the schema, and an external one also needs `CREATE EXTERNAL VOLUME` on the external location.
- **External location privileges:** `READ FILES`, `WRITE FILES`, `CREATE EXTERNAL TABLE`, and `CREATE EXTERNAL VOLUME`. Databricks recommends granting broad `READ FILES` sparingly and giving users volumes instead.

## Example

```sql
CREATE EXTERNAL LOCATION landing
  URL 's3://acme-landing/'
  WITH (STORAGE CREDENTIAL landing_cred);

CREATE EXTERNAL VOLUME main.raw.orders_landing
  LOCATION 's3://acme-landing/orders/';

GRANT READ VOLUME ON VOLUME main.raw.orders_landing TO `data-eng`;
```

Auto Loader then reads `/Volumes/main/raw/orders_landing/` with `format("cloudFiles")`. You can also put its schema location and checkpoint in a managed volume. If someone in `data-eng` gets `PERMISSION_DENIED`, check `USE SCHEMA` and the volume grant before you touch any Spark config. A missing grant can't be fixed by tuning.

## Tradeoffs and gotchas

- A volume holds files, not tables. You can't query it with `SELECT * FROM`. You read its files with `read_files()`, `spark.read`, or `dbutils.fs`, and you can't use a volume path as a table's location.
- Overlapping paths are blocked. A volume can't sit on a directory that's already a table's location, and vice versa.
- DBFS mounts are the legacy approach and bypass Unity Catalog governance. New pipelines should use volumes or external locations.
- Volume paths need Unity Catalog-enabled compute. Old no-isolation clusters won't resolve `/Volumes`.
- The managed vs. external drop behavior catches people out. Dropping a managed volume really does delete your landing files.

## For the exam

Know that volumes govern non-tabular files, that external locations plus storage credentials replace embedded keys and mounts, and that a permissions failure on a path is a Unity Catalog grant problem. Remember too that managed vs. external drop behavior is the same for volumes as for tables.
