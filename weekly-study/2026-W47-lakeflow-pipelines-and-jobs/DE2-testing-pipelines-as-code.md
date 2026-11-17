# DE2: Testing data pipelines as code (unit, contract, integration, and CI)

- **Date:** Tue 2026-11-17
- **Week:** 2026-W47
- **Theme:** Lakeflow pipelines and Jobs in depth
- **Paired Databricks pill (Gerald):** DB2: Pipeline expectations in practice plus the pipeline event log

---

Good morning, Mario. Gerald's pill today is about **expectations**, which check the *data* while the pipeline runs. Today's DE pill is the other half: checking the *code* before it ever reaches production. US interviewers love this one because it separates "writes notebooks" from "engineers pipelines."

## 1. The key reframe: test the code vs test the data

| | Tests the **code** (before deploy) | Tests the **data** (at runtime) |
|---|---|---|
| Question | "Is my logic right?" | "Is today's input sane?" |
| Input | Small, hand-built DataFrames | Real production data |
| When | On every PR, in CI | On every pipeline run |
| Databricks tools | pytest + `pyspark.testing`, Databricks Connect, bundles in CI | Expectations, Delta CHECK constraints, data quality monitoring |

Expectations can't tell you that your dedup logic keeps the *oldest* row instead of the newest. A unit test can. A unit test can't tell you the source sent 0 rows today. An expectation can. **You need both.**

## 2. The test pyramid for data pipelines

1. **Unit tests (many, fast):** pure transformation functions on 3–10 row DataFrames. Edge cases: NULLs, duplicates, late events, empty input, timezones.
2. **Contract / schema tests (some):** the output schema and keys match what downstream consumers expect. `assertSchemaEqual`, or a dbt model contract.
3. **Integration tests (few, slower):** run the real pipeline end to end on a small fixture dataset in a dev/staging workspace, then assert on the target tables (row counts, key uniqueness, a few golden values).
4. **Runtime checks (always on):** expectations and constraints in production (Gerald's lane today).

The design trick that makes all of this possible: **keep business logic in plain Python functions that take and return DataFrames**, in a module, not inline in a notebook cell or a pipeline decorator. The Lakeflow pipeline file then just wires the functions together.

## 3. Hands-on: a unit test with pytest + PySpark testing utilities

```python
# src/transforms.py — pure logic, no reads/writes
from pyspark.sql import DataFrame, functions as F, Window

def latest_per_key(df: DataFrame, key: str, ts: str) -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col(ts).desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1").drop("_rn"))
```

```python
# tests/test_transforms.py
from pyspark.testing import assertDataFrameEqual
from src.transforms import latest_per_key

def test_keeps_newest_row_per_key(spark):          # spark = pytest fixture
    src = spark.createDataFrame(
        [(1, "2026-11-01", "old"), (1, "2026-11-02", "new"), (2, "2026-11-01", "only")],
        "id INT, ts STRING, v STRING")
    expected = spark.createDataFrame(
        [(1, "2026-11-02", "new"), (2, "2026-11-01", "only")],
        "id INT, ts STRING, v STRING")
    assertDataFrameEqual(latest_per_key(src, "id", "ts"), expected)  # row order ignored
```

```python
# pipeline file (Lakeflow Spark Declarative Pipelines) — just wiring
from pyspark import pipelines as dp
from src.transforms import latest_per_key

@dp.materialized_view
@dp.expect_or_fail("id_not_null", "id IS NOT NULL")   # runtime data check
def customers_latest():
    return latest_per_key(spark.read.table("bronze.customers"), "id", "ts")
```

The `spark` fixture can be a local SparkSession (fast, free) or a **Databricks Connect** session pointing at serverless compute, so tests run against the real runtime from your laptop or CI.

## 4. Running it in CI (preview of next week)

A typical GitHub Actions flow on every PR:
1. `pytest tests/unit` (local Spark, seconds).
2. `databricks bundle validate` and `databricks bundle deploy -t dev`.
3. `databricks bundle run integration_test_job -t dev`: runs the pipeline on fixture data and asserts on the outputs.
4. Merge, then deploy to prod with the same bundle and a different target.

We'll go deep on bundles and CI/CD in W48, so keep this as the shape for now.

## 5. Tech-gap sidebar: how dbt does it

dbt (very common in US Snowflake and Databricks shops) has testing built in, which is one reason teams love it:
- **Data tests** (`unique`, `not_null`, `relationships`, `accepted_values`) run against real tables. These are like expectations.
- **Unit tests** (since dbt 1.8): you give mock input rows and expected output rows in YAML, and dbt checks the model's SQL logic. These are like pytest on small DataFrames.
- **Model contracts** enforce column names and types. These are like schema tests.

If an interviewer asks "how do you test dbt models?", map it straight to the same pyramid.

## 6. Interview question

**"How do you test your data pipelines?"**

Strong senior answer (shape):
> "I split it into testing the code and testing the data. For the code, I keep transformations as pure functions that take and return DataFrames, so I can unit test them with pytest on a handful of rows. That's where I cover the edge cases that break pipelines: duplicates, nulls, late or out-of-order events, empty inputs. I add schema and contract tests on outputs other teams depend on, and a small number of integration tests that deploy the pipeline to a dev workspace with a bundle and run it on fixture data. All of that runs in CI on every pull request, so nothing reaches prod untested. For the data, I use expectations and table constraints at runtime, with a clear policy for warn, drop, quarantine, or fail, and I monitor the results from the pipeline event log. As a tech lead, I also made tests part of the definition of done: a bug fix comes with the test that would have caught it."

Why it's senior: two layers clearly separated, testability designed into the code structure, CI automation, and a team practice, not just a tool name.

## 7. English phrasing tip

Use "**catch**" and "**shift left**" naturally:
- "Unit tests **catch** logic bugs; expectations **catch** bad data."
- "We **shifted** testing **left**, so most issues are caught in the PR, not by finance on Monday."
- Say "**fixture data**" (small, controlled test inputs), not "fake data."

## Go deeper (official docs)

- PySpark testing (`assertDataFrameEqual`, `assertSchemaEqual`): https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html
- Unit testing for notebooks on Databricks: https://docs.databricks.com/aws/en/notebooks/testing
- Run tests with pytest via Databricks Connect: https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/testing
- dbt unit tests: https://docs.getdbt.com/docs/build/unit-tests
