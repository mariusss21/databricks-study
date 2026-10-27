# DE2: Dimensional modeling basics: facts, dimensions, grain, surrogate keys, star schema

- **Date:** Tue 2026-10-27
- **Week:** 2026-W44, Bronze to Silver transforms
- **Paired Databricks pill:** DB2, join types on Databricks (inner, left, cross, multi-key, broadcast joins)
- **Track:** Data Engineering interview pill (NJ in Dollar)

---

Yesterday we made Silver trustworthy. Today is about the shape of what comes next. Once Silver is clean, Gold usually becomes a dimensional model, and almost every US Senior DE loop has a "model this business for me" question. Kimball's star schema is still the shared language for it, including on a lakehouse.

## The four ideas

**1. Grain first.** The grain is one sentence that says what one row of the fact table means, for example "one row per order line" or "one row per account per day". You declare it before you pick any columns. Most modeling bugs, like double counting or numbers that don't tie out, come from a grain nobody wrote down.

**2. Facts are the events you measure.** A fact table holds numbers (quantity, amount, duration) plus foreign keys to the dimensions. It's tall and narrow and grows fast. There are three common kinds:
- *Transaction* facts have one row per event, like an order line.
- *Periodic snapshot* facts have one row per entity per period, like an account balance per day.
- *Accumulating snapshot* facts have one row per process instance that gets updated as it moves through milestones, like an order going from placed to shipped to delivered.

Measures can be additive (amount), semi-additive (balance, which you can't sum across time), or non-additive (ratios, so you store the numerator and denominator instead).

**3. Dimensions are the context you slice by.** Who, what, where, and when: customer, product, store, date. They're wide, descriptive, and fairly small. You denormalize them on purpose (category and subcategory go on the product row) so analysts join once and don't walk a snowflake of lookup tables.

**4. Surrogate keys.** A dimension gets its own meaningless integer key (`customer_sk`) instead of reusing the source's business key (`customer_id`). This matters because:
- it's the only way to keep history. In SCD Type 2 (Thursday's pill), one customer has several rows, so the business key can't be unique.
- it protects you from source systems that reuse, change, or collide IDs when you merge two CRMs.
- it gives you a home for an "unknown" member (`-1`), so a late or missing dimension never drops fact rows in an inner join.

## How it maps to Databricks

- **Medallion layers.** Bronze is raw, Silver is cleaned and conformed entities, and Gold is where the star schema lives, ready for Databricks SQL, AI/BI dashboards, and Genie.
- **Surrogate keys.** Use Delta identity columns, `BIGINT GENERATED ALWAYS AS IDENTITY`. The values are unique and increasing but not gap-free, and that's fine for a surrogate key. Some teams use a hash of the business key and version instead, which is deterministic across environments. Know both and the trade-off.
- **Keys as metadata.** Unity Catalog supports `PRIMARY KEY` and `FOREIGN KEY` constraints. They're informational, meaning Databricks doesn't enforce them, but they document the model, show up in Catalog Explorer, and with `RELY` the optimizer may use them to skip unnecessary joins or aggregations. Uniqueness is still your pipeline's job, through MERGE and expectations.
- **Today's join pill (DB2).** A star query is one big fact joined to small dimensions, which is the textbook case for a broadcast join. That's why star schemas run fast on Spark.
- **Layout.** Put liquid clustering on the fact table using the columns people filter on most, usually the date key and one or two hot dimension keys. Dimensions are small, so they rarely need it.

## Hands-on (Databricks SQL)

```sql
-- Dimension with a surrogate key and an "unknown" member
CREATE TABLE gold.dim_customer (
  customer_sk   BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id   STRING NOT NULL,          -- business key from the source
  customer_name STRING,
  segment       STRING,
  country       STRING,
  CONSTRAINT dim_customer_pk PRIMARY KEY (customer_sk) RELY
);

-- Fact at a declared grain: one row per order line
CREATE TABLE gold.fact_order_line (
  order_id      STRING NOT NULL,
  line_number   INT    NOT NULL,
  order_date_sk INT    NOT NULL,          -- e.g. 20261027
  customer_sk   BIGINT NOT NULL,
  product_sk    BIGINT NOT NULL,
  quantity      INT,
  net_amount    DECIMAL(18,2),
  CONSTRAINT fol_customer_fk FOREIGN KEY (customer_sk)
    REFERENCES gold.dim_customer(customer_sk)
)
CLUSTER BY (order_date_sk, customer_sk);

-- Load facts: look up surrogate keys, fall back to the unknown member (-1)
INSERT INTO gold.fact_order_line
SELECT o.order_id, o.line_number,
       CAST(date_format(o.order_ts, 'yyyyMMdd') AS INT),
       COALESCE(c.customer_sk, -1),
       COALESCE(p.product_sk, -1),
       o.quantity, o.net_amount
FROM silver.order_lines o
LEFT JOIN gold.dim_customer c ON o.customer_id = c.customer_id
LEFT JOIN gold.dim_product  p ON o.product_id  = p.product_id;

-- Test the grain: this must return zero rows
SELECT order_id, line_number, COUNT(*)
FROM gold.fact_order_line
GROUP BY ALL HAVING COUNT(*) > 1;
```

Note: an identity column won't accept the `-1` row through a normal insert, so seed the unknown member by creating the column as `GENERATED BY DEFAULT AS IDENTITY`, or keep unknowns in a small separate seed. That's a good detail to mention if an interviewer asks how you handle it.

## Likely interview question

**"We're an e-commerce company. Design the core analytics model for orders."**

Strong senior answer, in this order:

1. *Ask about the questions the business needs answered first.* For example revenue by product and region, conversion, and returns. The model serves those questions.
2. *Declare the grain.* "My main fact is one row per order line, because that's the most atomic level and you can always roll up but never drill down below your grain." Then add a separate fact for returns or shipments, since those are different business processes with different grains. Don't force them into one table.
3. *Name the dimensions.* Date, customer, product, store or channel, promotion. They're conformed, so the same `dim_customer` serves orders, returns, and support tickets. That's what makes metrics comparable across teams.
4. *Keys and history.* Use surrogate keys on every dimension, SCD Type 2 on customer and product where history matters (a segment change shouldn't rewrite last year's revenue), and an unknown member for late-arriving dimensions.
5. *Make it run on the platform.* On Databricks, Gold is Delta tables with identity keys, informational PK/FK in Unity Catalog, liquid clustering on the fact by date, and a grain test and FK-orphan check in the pipeline. If needed, add a pre-aggregated daily table or a materialized view for the dashboards.
6. *Trade-off.* "I'd consider a One Big Table for a single heavy dashboard, but the star stays the source of truth, because OBTs duplicate dimension attributes and get painful when they change."

Bonus tech-lead angle: say who owns conformed dimensions. "On my team, I'd make one squad own `dim_customer` so we don't end up with three definitions of an active customer." That shows you've lived the organizational side.

## English phrasing tip

Say "**What's the grain of this table?**" and "**This fact is at the order-line grain.**" Americans often say "**roll up**" for aggregating to a coarser level and "**fan out**" for the row explosion you get from joining at the wrong grain (that's tomorrow's pill). Also, "dimension" is the table and "attribute" is a column in it, so say "the segment attribute on the customer dimension."

## Go deeper

- [Kimball dimensional modeling techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/) is the original glossary for grain, fact types, and surrogate keys.
- [Data modeling on Databricks](https://docs.databricks.com/aws/en/transform/data-modeling) covers how Databricks recommends modeling on the lakehouse.
- [Constraints on Databricks](https://docs.databricks.com/aws/en/tables/constraints) explains enforced NOT NULL/CHECK and informational primary and foreign keys with RELY.
- [CREATE TABLE reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using) has the identity column syntax.
