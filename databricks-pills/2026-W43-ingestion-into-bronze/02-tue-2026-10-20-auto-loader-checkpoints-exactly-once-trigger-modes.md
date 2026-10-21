# Auto Loader checkpoints, exactly-once, and trigger modes

**Date:** Tue 2026-10-20

## Exam pill

A checkpoint lets Auto Loader remember which files it already processed, so a restart does not load them again.

## Deep dive

**Auto Loader checkpoints, exactly-once, and trigger modes**

An Auto Loader stream is only as reliable as its checkpoint. The checkpoint is a directory where the stream records what it has already read and committed, so a crash, a restart, or a scheduled re-run picks up exactly where the last one stopped instead of reloading the whole landing folder.

**How it works under the hood**

Each micro-batch runs in a fixed order:
1. Auto Loader discovers new files, either by listing the directory or from file events.
2. It writes the planned batch into the checkpoint's `offsets/` folder, which works as a write-ahead log.
3. Spark processes those files and commits them to the Delta table.
4. It writes a matching entry into `commits/`.

Auto Loader also keeps the metadata of every discovered file in a RocksDB key-value store inside the checkpoint. That store is how it knows a file was already seen without re-reading your Bronze table.

If the cluster dies between steps 2 and 4, the restart finds a batch in `offsets/` with no matching commit and replays that same batch. The Delta sink is idempotent per stream and batch ID, so a replayed batch that already landed isn't written twice. Exactly-once comes from three pieces working together: a replayable source, the checkpoint, and an idempotent sink.

**Trigger modes**
- **Default (no trigger set):** runs micro-batches back to back for as long as the stream stays up.
- **`processingTime="1 minute"`:** an always-on stream that starts a batch on a fixed interval. It's the "continuous" pattern for low latency, but the compute bills the whole time.
- **`availableNow=True`:** processes everything that arrived since the last run, split into batches that respect `cloudFiles.maxFilesPerTrigger` and `cloudFiles.maxBytesPerTrigger`, and then stops. This is incremental batch, scheduled from a Lakeflow Job. It's also the pattern you use on serverless compute, which doesn't support time-based triggers.
- **`once=True`:** the older version of availableNow. It's deprecated and ignores rate limits, so it reads everything in one batch.
- **`trigger(continuous=...)`:** Spark's continuous processing mode is a different feature. It doesn't support file sources, so don't confuse it with an always-on Auto Loader stream.

**Example**
```python
(spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", "/Volumes/dev/bronze/_chk/orders/schema")
  .load("/Volumes/dev/landing/orders/")
  .writeStream
  .option("checkpointLocation", "/Volumes/dev/bronze/_chk/orders")
  .trigger(availableNow=True)
  .toTable("dev.bronze.orders"))
```
The first run loads all three files in the folder, then stops. You drop in two more files and run it again, and only those two are loaded. If you kill the cluster in the middle of the second run, the restart replays the batch it didn't finish, and `DESCRIBE HISTORY dev.bronze.orders` shows no duplicate append.

**Tradeoffs and gotchas**
- **The checkpoint is the stream's identity.** If you delete it, the next start treats every file as new (`cloudFiles.includeExistingFiles` defaults to true), and you get duplicates in Bronze.
- **Use one checkpoint per stream.** Never share one between two writers, and keep it on durable, governed storage such as a UC volume or external location.
- **The checkpoint doesn't follow big changes.** Changing the source path or making major query changes can make the old checkpoint invalid.
- **`foreachBatch` gives up the automatic guarantee.** Your own writes there need to be idempotent, for example a `MERGE` or the Delta `txnAppId`/`txnVersion` options.
- **Overwritten files aren't reloaded.** By default, a file that's overwritten under the same name isn't processed again (`cloudFiles.allowOverwrites` is false).
- **Declarative pipelines manage this for you.** In Lakeflow Spark Declarative Pipelines, the platform handles checkpoints and schema locations.

**For the exam:** Know that the checkpoint is what makes Auto Loader incremental and exactly-once. Know that `availableNow` means "process the backlog and stop, run from a Job," while an always-on trigger means low latency at continuous compute cost.
