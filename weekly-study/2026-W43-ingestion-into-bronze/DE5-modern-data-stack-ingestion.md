# The US "modern data stack" ingestion layer, mapped to Databricks

**Date:** Fri 2026-10-23 · **Week:** 2026-W43 · Ingestion into Bronze · **Track:** Data Engineering (NJ in Dollar)
**Pairs with Gerald's Databricks pill:** Choosing an ingestion tool: Connect vs Auto Loader vs COPY INTO vs JDBC

## Why this matters for your interviews

A lot of US job posts say "Fivetran, Snowflake, dbt" in the same line. That trio is the classic **modern data stack** (MDS): a SaaS tool extracts and loads raw data, the warehouse stores it, and dbt transforms it with SQL. You don't need years in these tools. You need to speak the vocabulary and show you know the Databricks equivalent of each piece.

## The three pieces

**1. EL tools: Fivetran and Airbyte.** They do the "E" and "L" of ELT: hundreds of prebuilt connectors (Salesforce, Postgres, Stripe, HubSpot...) that copy source data into raw tables in your warehouse, including CDC from databases and schema-change handling.
- **Fivetran** is fully managed SaaS. You click, it syncs. It adds metadata columns like `_fivetran_synced` and `_fivetran_deleted`. Pricing is based on monthly active rows, so high-churn tables get expensive.
- **Airbyte** is open source (self-host) or Airbyte Cloud. Cheaper and more flexible, but you own more operations when you self-host.
- Why US companies use them: a small team can't hand-write and maintain 80 SaaS API connectors. They buy the long tail.

**2. Snowflake loading: stages, COPY INTO, Snowpipe.**
- A **stage** points at files (for example in S3).
- **COPY INTO** is a batch load from the stage into a table. Snowflake remembers which files it loaded (load metadata for 64 days), so re-running it doesn't duplicate data.
- **Snowpipe** is the same COPY statement wrapped in a **pipe** that fires automatically when new files land (S3 event notifications). It's serverless and near real time, and it keeps its own load history (14 days).
- **Snowpipe Streaming** writes rows directly (for example from Kafka) without files.

**3. dbt sources.** dbt does **not** load data. A `sources.yml` file declares the raw tables that Fivetran or Snowpipe landed, so models can use `{{ source('shop', 'orders') }}`. That gives you lineage, tests on raw data, and `dbt source freshness`, which fails or warns when the loader stops delivering.

## The Databricks mapping

| MDS piece | Databricks equivalent | Note |
|---|---|---|
| Fivetran / Airbyte connectors | **Lakeflow Connect** managed connectors (SaaS apps and databases like SQL Server, Salesforce, Workday) | Fivetran also writes into Databricks directly (Partner Connect). Many shops keep Fivetran for the long tail. |
| Snowflake COPY INTO | Databricks **COPY INTO** | Same idea: idempotent, skips already-loaded files. Good for thousands of files, scheduled batch. |
| Snowpipe (auto-ingest) | **Auto Loader** (`cloudFiles` / `read_files` in a streaming table), with file notification mode | Scales to millions of files, schema evolution and `_rescued_data` built in. |
| Snowpipe Streaming | Structured Streaming from Kafka/Kinesis into Delta | Row-level, no files. |
| dbt sources + freshness | Unity Catalog tables + lineage, pipeline expectations, data quality monitoring | And dbt runs on Databricks too, through the `dbt-databricks` adapter, with sources pointing at UC catalogs. |

The big difference: in Snowflake, loading and transforming are separate products (pipe plus dbt). In Databricks you can do both in one **Lakeflow Declarative Pipeline**: Auto Loader streaming tables for Bronze and materialized views for Silver/Gold, or you can keep dbt for transforms if the team already knows it.

## Hands-on: the same load in both worlds

```sql
-- Snowflake: batch load, then make it automatic with a pipe
CREATE STAGE raw.orders_stage
  URL = 's3://acme-raw/orders/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = (TYPE = JSON);

COPY INTO raw.orders_raw (v)
  FROM (SELECT $1 FROM @raw.orders_stage);

CREATE PIPE raw.orders_pipe AUTO_INGEST = TRUE AS
  COPY INTO raw.orders_raw (v)
  FROM (SELECT $1 FROM @raw.orders_stage);
```

```sql
-- Databricks: batch load with COPY INTO (idempotent, re-runnable)
CREATE TABLE IF NOT EXISTS main.bronze.orders_raw;

COPY INTO main.bronze.orders_raw
  FROM 's3://acme-raw/orders/'
  FILEFORMAT = JSON
  FORMAT_OPTIONS ('inferSchema' = 'true')
  COPY_OPTIONS ('mergeSchema' = 'true');

-- Databricks: continuous version, Auto Loader inside a declarative pipeline
CREATE OR REFRESH STREAMING TABLE bronze.orders_raw AS
SELECT *, _metadata.file_path AS source_file, current_timestamp() AS ingested_at
FROM STREAM read_files('s3://acme-raw/orders/', format => 'json');
```

```yaml
# dbt: declare the raw table and alert if the loader goes quiet
version: 2
sources:
  - name: shop
    database: raw          # on Databricks this is the UC catalog
    schema: shop
    loaded_at_field: _fivetran_synced
    freshness:
      warn_after: {count: 6, period: hour}
      error_after: {count: 24, period: hour}
    tables:
      - name: orders
```

Run `dbt source freshness` in the scheduler before `dbt build`. (Newer dbt versions also accept `freshness` and `loaded_at_field` under a `config:` block.)

## Likely interview question

**"Our stack is Fivetran, Snowflake, and dbt. If we moved ingestion to Databricks, what would you keep and what would you replace?"**

Strong senior answer:
"I'd start from a source inventory, not from the tool. For the long tail of SaaS sources with low volume, Fivetran is usually cheaper than engineering time, so I'd keep it, or move a source to Lakeflow Connect where a managed connector exists and the cost or governance story is better, since it lands straight into Unity Catalog. For the few high-volume databases, monthly-active-row pricing hurts, so I'd look at Lakeflow Connect's database CDC or a Debezium-to-Kafka path we own. File drops become Auto Loader streaming tables, which replace Snowpipe, and one-off or scheduled batch loads can use COPY INTO. dbt I'd keep if the team is productive in it, because dbt-databricks works well, and I'd keep source freshness checks as the contract with the loaders. Then I'd migrate one domain end to end, compare cost, latency, and on-call load, and only then scale it. The decision is buy versus build per source, not one tool for everything."

## English phrasing tip

Say **"buy versus build"**, **"the long tail of sources"**, and **"we own the connector"**. A good line: *"We chose to buy for the long tail of SaaS sources and build for the two high-volume databases, because that's where the pricing and the latency mattered."* It sounds like a tech lead who made a real trade-off.

## Go deeper

- [Snowpipe introduction (Snowflake docs)](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro)
- [dbt sources and freshness (dbt docs)](https://docs.getdbt.com/docs/build/sources)
- [Ingest data into a Databricks lakehouse (Lakeflow Connect, Auto Loader, COPY INTO)](https://docs.databricks.com/aws/en/ingestion/)
- [Connect to Fivetran from Databricks](https://docs.databricks.com/aws/en/partners/ingestion/fivetran)
