# Schema evolution, drift, and data contracts

**Date:** Wed 2026-10-21 · **Week:** 2026-W43 · Ingestion into Bronze · **Track:** Data Engineering (NJ in Dollar)
**Pairs with Gerald's Databricks pill:** Landing nested or semi-structured JSON in Bronze (struct and VARIANT)

## The core idea

Sources change without asking you. A new column shows up, a field goes from int to string, a nested object gets a new key. That's **schema drift**. How you react to it is **schema evolution**.

- **Schema-on-write:** the table enforces a schema, and bad or unexpected data is rejected. It's safe, but brittle.
- **Schema-on-read:** store the raw payload and apply structure when you query it. It's flexible, but you push the problem downstream.
- **Data contract:** an agreement, owned by the producer, about schema, meaning, and SLAs (freshness, nullability, allowed values). It's versioned, and breaking changes need notice. It turns drift from a 3 AM surprise into a planned change.

The senior pattern is to be **tolerant in Bronze and strict in Silver**. Bronze should never lose data because of a surprise field, and Silver is where the contract gets enforced.

## How Databricks does it

- **Auto Loader schema evolution** (`cloudFiles.schemaEvolutionMode`): `addNewColumns` (the default: the stream stops on a new column, updates the schema, and continues after a restart), `rescue` (the schema stays fixed and unexpected data goes into `_rescued_data`), `failOnNewColumns`, and `none`.
- **`_rescued_data` column:** fields that don't match the schema (type mismatch, case mismatch, new keys) are kept as JSON instead of being dropped.
- **Delta schema enforcement** rejects mismatched writes by default. `mergeSchema` opts in to adding columns, and **type widening** allows safe changes such as int to bigint without rewriting the table.
- **VARIANT type:** stores semi-structured data in an efficient binary format. Query it with `:` paths and `::` casts. It's much faster than JSON strings and doesn't force you to fix a schema up front, which makes it a natural fit for Bronze.
- **Contract enforcement:** expectations in Lakeflow Declarative Pipelines (`EXPECT ... ON VIOLATION DROP ROW / FAIL UPDATE`), Delta `CHECK` constraints, and Unity Catalog for ownership and lineage.

## Snowflake VARIANT vs Databricks VARIANT

Snowflake has had `VARIANT` for years, and it's a big reason US teams like Snowflake for JSON. Databricks VARIANT is the lakehouse answer, and the query syntax is almost the same (`col:path.to.field::type`). The differences: Snowflake loads JSON into VARIANT with `COPY INTO` or Snowpipe, while Databricks uses Auto Loader, `read_files`, or `COPY INTO`. Databricks VARIANT lives in open Delta tables. Both auto-optimize common paths behind the scenes (Databricks calls it shredding). In an interview, "I've used VARIANT on Databricks, and it maps directly to Snowflake's VARIANT" is a good bridge.

## Hands-on (SQL)

```sql
-- Bronze: land raw JSON as VARIANT, no schema decisions yet
CREATE OR REPLACE TABLE main.bronze.events_raw AS
SELECT parse_json(value) AS raw, current_timestamp() AS _ingested_at
FROM VALUES
  ('{"id": 1, "customer": {"id": "C9", "tier": "gold"}, "amount": 10}'),
  ('{"id": 2, "customer": {"id": "C7"}, "amount": "12.50", "coupon": "FALL"}') AS t(value);

-- Silver: apply the contract when you read it
SELECT
  raw:id::bigint              AS event_id,
  raw:customer.id::string     AS customer_id,
  raw:customer.tier::string   AS tier,      -- missing -> NULL, no failure
  try_cast(raw:amount AS decimal(10,2)) AS amount,  -- drifted type handled
  raw:coupon::string          AS coupon     -- new field, zero ALTERs
FROM main.bronze.events_raw;
```

Try this: add a third row with a brand-new nested key and query it right away. Then do the same thing with a strict `STRUCT` column and see what happens.

## Likely interview question

*"An upstream team renamed a column and changed a type, and your pipeline broke overnight. How do you design so this doesn't happen again?"*

**Strong senior answer:** "I'd fix it on two levels: technical and organizational. Technically, Bronze is tolerant. We land raw payloads with Auto Loader into VARIANT, or with rescue mode, so a surprise never loses data or stops ingestion. Silver is strict. We enforce a contract with expectations and constraints, and a breaking change fails loudly there, with an alert, instead of quietly corrupting a dashboard. Organizationally, I'd put a data contract in place with the producing team: a versioned schema, an owner, and a rule that breaking changes ship as a new version with a deprecation window. I'd also add schema checks to their CI. Additive changes flow through automatically. Breaking ones are negotiated. As a tech lead, I've seen that most drift incidents are communication problems, so the contract matters as much as the code."

## English tip

Say "the pipeline **broke loudly**" or "it **failed silently**." Those are natural, very common phrases in US interviews. And "**schema drift**" is said as one unit, not "drift of schema."

## Go deeper

- [VARIANT data type for semi-structured data (Databricks)](https://docs.databricks.com/aws/en/semi-structured/variant)
- [Auto Loader schema inference and evolution (Databricks)](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
- [Semi-structured data types, VARIANT (Snowflake)](https://docs.snowflake.com/en/sql-reference/data-types-semistructured)
