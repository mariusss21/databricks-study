# The Spark Driver, explained

Context: follow-up to Introduction to Apache Spark, Spark Runtime Architecture

The driver is the one process that runs your code and does all the thinking. Executors only do the work it hands them. When your notebook runs `df.filter(...).groupBy(...).count()`, that Python or SQL runs on the driver. On Databricks, the driver is a single node of your cluster, and the executors run on the worker nodes.

Here's what each bullet actually means, in the order it happens.

**1. It creates the SparkSession.** The `spark` object is the driver's connection to the cluster. Through it, the driver asks for executors and keeps track of them. On Databricks you never create it yourself, because the notebook already has `spark` ready.

**2. It builds the plan, and here's where laziness comes in.** Transformations like `select`, `filter`, `join`, and `groupBy` don't run anything. The driver just records them as a logical plan. Nothing happens until you call an action, such as `count()`, `collect()`, `write`, or `display()`. At that point the driver:
- runs the plan through the Catalyst optimizer, which pushes filters down, prunes columns, and picks join strategies such as broadcast versus sort-merge;
- turns it into a physical plan;
- splits that plan into stages at every shuffle boundary. A narrow transformation (filter, select, withColumn) stays inside one stage, while a wide one (join, groupBy, repartition) needs data moved between executors, so it starts a new stage.

That chain of stages is the DAG (directed acyclic graph). You can see it in the Spark UI under a job's stages, and `df.explain()` prints the plan.

**3. It schedules tasks.** Each stage becomes one task per data partition. If a stage reads 200 partitions, the driver creates 200 tasks and hands them to free executor cores, trying to send each task to where its data already sits. A stage can't start until the stage before it has finished writing its shuffle output.

**4. It monitors and handles failures.** Executors report task status back to the driver. If a task fails, the driver retries it, by default up to 4 times. If an executor dies, the driver reschedules that executor's tasks elsewhere and can recompute lost partitions from the plan, because it knows how each partition was built. The driver itself has no such safety net. If the driver dies, the whole application fails.

**5. It returns results to the client.** Only actions like `collect()`, `toPandas()`, `take()`, `show()`, or `display()` bring rows back to the driver. A `write` doesn't. Executors write straight to storage, and only the commit bookkeeping goes through the driver.

**Why this matters in practice and on the exam**
- **Driver out-of-memory.** It almost always comes from pulling too much data back, with `collect()` or `toPandas()` on a large DataFrame, or from broadcasting a table that isn't really small, since a broadcast table is built on the driver first. A bigger cluster won't fix it. The fix is a bigger driver, or simply not collecting.
- **Plain Python runs on the driver only.** A pandas loop or a `for` loop over rows uses one machine while the executors sit idle. If a job is slow and the Spark UI shows no active tasks, this is usually why.
- **Driver versus executor problems.** Driver issues show up in the driver logs as out-of-memory, garbage-collection pauses, or a hung notebook. Executor issues show up per task in the Spark UI as spill, skew, or failed tasks.

To sum up: the driver plans and coordinates, the executors process the data, and the only rows that travel back to the driver are the ones you explicitly ask for.
