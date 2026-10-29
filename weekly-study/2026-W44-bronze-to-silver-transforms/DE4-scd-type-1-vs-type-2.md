# DE4: SCD Type 1 vs Type 2 and when each fits

- **Date:** Thu 2026-10-29
- **Week:** 2026-W44
- **Theme:** Bronze to Silver transforms
- **Paired Databricks pill:** DB4 (Gerald): Dedup and "latest record wins": row_number() windows vs dropDuplicates, and MERGE INTO Silver

Gerald's DB4 is the *how* (get the latest row per key, then MERGE it). This pill is the *why*: once a dimension attribute changes, do you overwrite it or keep the history? That choice is SCD (slowly changing dimension) type.

## The idea in 60 seconds

- **SCD Type 1, overwrite.** One row per business key. A change replaces the old value. Simple and small, but history is gone, so old facts get re-labeled with today's value.
- **SCD Type 2, add a version.** A change closes the current row (sets an end timestamp, `is_current = false`) and inserts a new row. Each version gets its own surrogate key, and facts point at the version that was true *when the event happened*.
- **Mixed is normal.** Per column, ask: "If this changes, should last year's report change too?" Fix a typo in a name, yes, so Type 1. Customer moves from SMB to Enterprise segment, no, so Type 2. That mix is a hybrid dimension; in practice you track history only on the columns that matter.

| | Type 1 | Type 2 |
|---|---|---|
| Rows per key | 1 | 1 per version |
| Answers | "What is it now?" | "What was it at time T?" |
| Fact join | on business key | on surrogate key, or key + date range |
| Cost | cheap | grows with change rate; needs `is_current` filters |
| Typical use | corrections, contact info, operational Silver | segment, region, price tier, owner, anything reported historically |

**Trap from yesterday's pill:** joining facts to a Type 2 table on the business key alone, without `is_current` or a date range, fans out your rows.

## Databricks connection

**Option A: hand-written MERGE (Type 2)** — the classic "staged union" trick so one MERGE both closes the old row and inserts the new one:

```sql
MERGE INTO silver.dim_customer t
USING (
  -- rows that changed: one copy to close the old version (merge_key = id)...
  SELECT c.customer_id AS merge_key, c.* FROM updates c
  UNION ALL
  -- ...and one copy with a NULL key so it falls into NOT MATCHED and inserts
  SELECT NULL AS merge_key, c.* FROM updates c
  JOIN silver.dim_customer d
    ON d.customer_id = c.customer_id AND d.is_current
   AND d.segment <> c.segment          -- only tracked columns
) s
ON t.customer_id = s.merge_key AND t.is_current
WHEN MATCHED AND t.segment <> s.segment THEN
  UPDATE SET is_current = false, end_at = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, segment, start_at, end_at, is_current)
  VALUES (s.customer_id, s.name, s.segment, s.updated_at, NULL, true);
```

It works, but you own the edge cases: out-of-order events, deletes, duplicates in the batch (Gerald's DB4 dedup must run first, or MERGE fails on multiple source matches).

**Option B: AUTO CDC in Lakeflow Declarative Pipelines** — declare it, and Databricks handles ordering and versioning:

```sql
CREATE OR REFRESH STREAMING TABLE dim_customer;

CREATE FLOW customer_scd2 AS AUTO CDC INTO dim_customer
FROM stream(bronze.customer_changes)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY lsn
COLUMNS * EXCEPT (op, lsn)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (email, phone);  -- email/phone behave like Type 1
```

- `SEQUENCE BY` fixes out-of-order arrival (the CDC/dedup pills from last week pay off here).
- Type 2 adds `__START_AT` and `__END_AT`; current rows have `__END_AT IS NULL`.
- `TRACK HISTORY ON ... EXCEPT` gives you the hybrid in one line.
- Swap to `STORED AS SCD TYPE 1` and you get an upsert-style current table instead.
- Python twin: `dp.create_auto_cdc_flow(..., stored_as_scd_type=2, track_history_except_column_list=[...])`. Source is a snapshot, not a change feed? Use `create_auto_cdc_from_snapshot_flow`.

## Hands-on (10 minutes)

Run the AUTO CDC block above on three change events for one customer: segment SMB at seq 1, email change at seq 2, segment Enterprise at seq 3, and feed seq 3 *before* seq 2. Then check:

```sql
SELECT customer_id, segment, email, __START_AT, __END_AT
FROM dim_customer ORDER BY __START_AT;
```

You should see two versions (SMB, Enterprise), not three, and the email change folded in without a new row.

## Interview question

**"A product manager says revenue by customer segment for last quarter changed after we re-ran the pipeline. What happened, and how would you design it?"**

Strong answer: "Almost certainly the segment dimension is Type 1, so when customers moved segments we re-labeled history. First I'd confirm with the business which attributes need point-in-time reporting; segment usually does, contact info doesn't. I'd make the dimension Type 2 on the tracked columns only, so it doesn't explode in size, and give each version a surrogate key. The fact table stores the surrogate key that was current at event time, so old revenue stays with the old segment. On Databricks I'd build it with AUTO CDC, `STORED AS SCD TYPE 2` with `TRACK HISTORY ON` the tracked columns, sequenced by the source LSN so late events land in the right version. And I'd offer a Type 1 'current view' too, because some teams do want 'revenue by today's segment', and that's a different, valid question."

**Why it lands:** you diagnose before you design, scope history per column, and name the trade-off (two valid questions, two views).

## English phrasing tip

Say **"point-in-time accurate"** or **"as-was vs as-is reporting"**: "Type 2 gives us as-was reporting; a current view gives us as-is." US interviewers use these exact phrases.

## Go deeper

- [AUTO CDC APIs: simplify change data capture (Databricks docs)](https://docs.databricks.com/aws/en/ldp/cdc)
- [Upsert into a Delta table using MERGE (Databricks docs)](https://docs.databricks.com/aws/en/delta/merge)
- [Kimball: Type 2 slowly changing dimension](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/)
