# Landing nested or semi-structured JSON in Bronze (struct and VARIANT)

Date: Wed 2026-10-21

## Exam pill

Nested JSON is in scope. It still has to land in a Unity Catalog Delta table, not only as raw files.

## Deep dive

**Landing nested or semi-structured JSON in Bronze: struct vs VARIANT**

Real sources rarely send flat rows. API payloads and event logs arrive as nested JSON with objects inside objects, arrays, and fields that come and go. Bronze has to keep all of it, queryable, in a Unity Catalog Delta table, without breaking every time the producer adds a key.

**How it works under the hood**

You have two ways to store a nested record. A **struct** gives the nested object a fixed schema: Spark knows every field name and type, writes each leaf as its own column in Parquet, and you read it with dot notation (`payload.customer.id`). A **VARIANT** stores the whole value in a compact binary encoding with no fixed schema. You read it with colon paths (`payload:customer.id`) and cast the result (`payload:customer.id::bigint`). Struct means you decide the schema at write time; VARIANT means you decide at read time.

With Auto Loader, the order is: it samples files, infers a schema, saves it under `cloudFiles.schemaLocation`, and reads with that schema. Nested objects become structs and JSON arrays become arrays. When a new key shows up, schema evolution kicks in.

**Main options**

- **`cloudFiles.inferColumnTypes`**: JSON leaves are inferred as strings unless you turn this on.
- **`cloudFiles.schemaEvolutionMode`**: `addNewColumns` (the default) stops the stream on a new column, then the restart picks it up. `rescue` keeps the schema and sends surprises to `_rescued_data`. `failOnNewColumns` and `none` are the strict options.
- **`cloudFiles.schemaHints`**: pins types for known fields, for example `'payload VARIANT'` to keep one messy subtree as VARIANT while the rest stays typed.
- **`singleVariantColumn`**: loads each whole record into one VARIANT column, so there's no inference and no evolution to manage.
- **`multiLine`**: off by default, which means JSON Lines (one record per line). Turn it on for pretty-printed files.
- **`parse_json` / `from_json`**: turn a JSON string column into VARIANT, or into a struct with an explicit schema.

**Example**

```sql
CREATE OR REPLACE TABLE bronze.events_raw AS
SELECT *, current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file
FROM read_files('/Volumes/main/landing/events/', format => 'json',
                singleVariantColumn => 'raw');

SELECT raw:order.id::bigint AS order_id,
       raw:items[0].sku::string AS first_sku
FROM bronze.events_raw;
```

A key that's missing comes back as NULL, not an error. That's why VARIANT holds up so well when the source drifts. Going to Silver, `explode()` turns an `items` array into one row per item.

**Tradeoffs and gotchas**

- Struct reads are the fastest, and the schema documents itself. The cost is that drift has to be handled through evolution or `_rescued_data`.
- With VARIANT you have to cast on read. A wrong path quietly returns NULL instead of failing, and a bad `::` cast raises an error (use `try_cast` if you want NULL instead). Cast before you group, sort, or cluster on a value.
- VARIANT needs a recent runtime (DBR 15.3+ or serverless), and older readers can't open the table.
- Keeping the whole payload as a raw string works too, but every query then re-parses it. Prefer VARIANT.
- Don't flatten in Bronze. Keep the payload whole and flatten in Silver.

For the exam: know how to land nested JSON into a governed Delta table (Auto Loader or `read_files`), how to reach nested fields (dot notation for structs, colon paths and casts for VARIANT), how `explode` handles arrays, and how schema evolution modes and `_rescued_data` deal with drift.
