# Monday review — Week 1: Foundations

**Mon Nov 23, 2026 · ~15–20 min · Week 2 of 8**

Week 1 covered Delta Lake and the lakehouse, compute and storage/compute separation, and Unity Catalog managed vs external tables. Today: answer 10 fresh questions, do one carried-over question, then skim the cheat sheet. Answers are at the bottom, so try them all first.

Reply with your answers like `1B 2C ... 11A` and I'll track anything you miss.

---

## Part 1 — 10 questions

**Q1.** `§1` A team keeps Parquet files in cloud storage and wants commits that are all-or-nothing, plus a way to see what changed in each write. What turns that folder into a Delta table?

- A) A `_delta_log` folder holding ordered JSON commit files
- B) A Hive metastore entry that registers the folder as a table
- C) A `_SUCCESS` marker file written by Spark after each job
- D) Partition folders named by date so writers stay separate

**Q2.** `§1` A cluster that ran your nightly SQL loads is terminated and deleted. What happens to the Delta tables it wrote?

- A) They are removed along with the cluster's local disks
- B) They stay in storage and any other compute can read them
- C) They stay, but only a cluster with the same name can read them
- D) They become read-only until the original cluster is recreated

**Q3.** `§1` A Python notebook runs every night at 2 AM with nobody watching. Today it runs on an all-purpose cluster that stays up all day. What is the better choice?

- A) A larger all-purpose cluster so the run ends faster
- B) A serverless SQL warehouse attached to the notebook
- C) Serverless jobs or job compute made for each run
- D) The same cluster with auto termination turned off

**Q4.** `§1` Forty analysts run ad hoc SQL on curated Gold tables all day, and demand spikes every morning. Which compute fits best?

- A) One shared all-purpose cluster sized for the peak
- B) Classic job compute triggered by each analyst query
- C) A single-node cluster in dedicated access mode
- D) A serverless SQL warehouse that adds clusters

**Q5.** `§1` On a serverless notebook, a data engineer needs the Python library `great_expectations` for a quick check. What works?

- A) Running `%pip install great_expectations` in a cell
- B) Adding a cluster init script that installs the package
- C) Setting `spark.jars.packages` in the Spark config panel
- D) Restarting on a GPU runtime that bundles the library

**Q6.** `§6` `DESCRIBE HISTORY sales.orders` still lists version 12 from 10 days ago. The table is a UC managed table with predictive optimization and default retention. `SELECT * FROM sales.orders VERSION AS OF 12` fails. Why?

- A) Time travel only works with `TIMESTAMP AS OF`, not versions
- B) The data files for that version were vacuumed after 7 days
- C) The history entry expired because logs keep only 7 days
- D) Version numbers reset each time `OPTIMIZE` runs on the table

**Q7.** `§7` Yesterday a job ran a bad `UPDATE` that set every `status` in `main.silver.orders` to `NULL`. The table still exists. What's the fastest fix that keeps the table name and grants?

- A) `UNDROP TABLE main.silver.orders`, then rerun the job
- B) `RESTORE TABLE main.silver.orders TO VERSION AS OF <v>`
- C) `CREATE TABLE main.silver.orders_fix DEEP CLONE` the table
- D) `DROP` the table, then reload it from the Bronze layer

**Q8.** `§7` You run `DROP TABLE` on an external table whose files sit in an external location. What happens to the files?

- A) They are deleted right away from cloud storage
- B) They are deleted after the 7-day recovery window
- C) They stay in place; only the UC metadata is removed
- D) They move into the schema's managed storage location

**Q9.** `§2` `§7` A partner drops CSV and PDF files that you want to keep governed in Unity Catalog before parsing them. Where should they live, and what path do you read?

- A) A managed table, read with `main.raw.partner_files`
- B) DBFS root, read with `dbfs:/FileStore/partner/`
- C) A view, read with `SELECT * FROM main.raw.partner`
- D) A volume, read with `/Volumes/main/raw/partner/`

**Q10.** `§3` A Gold aggregate joins three large tables and takes four minutes to compute. A dashboard hits it hundreds of times a day, and data up to an hour old is fine. Which Gold object fits?

- A) A plain view over the three source tables
- B) A materialized view refreshed on a schedule
- C) A temporary view created in each dashboard session
- D) A common table expression inside each dashboard query

---

## Part 2 — Carried over from your misses

**Q11.** `§1` *(spaced repetition: from W1 P2, reframed around init scripts)* A Python job task runs on serverless. A new requirement says every node must install an OS package and a company CA certificate when the compute starts. What should you do?

- A) Keep serverless and add the init script to the job's environment
- B) Move the task to classic job compute with the init script attached
- C) Run the task on a serverless SQL warehouse that allows init scripts
- D) Install the OS package with `%pip install` at the top of the notebook

---

## Part 3 — One-page cheat sheet

### Must-know points

**Delta and the lakehouse (§1, §6)**
- Delta = Parquet data files + `_delta_log/` (ordered JSON commits, Parquet checkpoints). The log is what gives ACID.
- A write is visible only when its commit lands. Readers always see a consistent snapshot.
- Time travel: `VERSION AS OF` / `TIMESTAMP AS OF`; `DESCRIBE HISTORY` lists versions.
- Two clocks: data files kept **7 days** after removal (`delta.deletedFileRetentionDuration`); log history kept **30 days** (`delta.logRetentionDuration`). History can list a version you can no longer read.
- Predictive optimization runs `OPTIMIZE` and `VACUUM` for you on UC managed tables.

**Compute (§1)**
- Storage and compute are separate: deleting compute never deletes tables.
- Scheduled, unattended work → serverless jobs or job compute (exists only for the run, cheaper DBUs).
- Interactive work → serverless notebook or all-purpose compute (set auto termination).
- Many concurrent SQL/BI users → serverless SQL warehouse (starts in seconds, scales out clusters).
- Serverless always runs the latest runtime; `%pip install` works in notebooks.
- Needs custom init scripts, most Spark configs, GPUs, or R → classic compute.

**Unity Catalog (§7)**
- Hierarchy: metastore → catalog → schema → table / view / volume / function. Names are `catalog.schema.object`.
- Managed table: UC owns the files. `DROP` deletes data after a 7-day window; `UNDROP` brings it back inside that window.
- External table: you own the files at an external location. `DROP` removes only metadata.
- `ALTER TABLE ... SET MANAGED` converts external → managed in place (keeps name, grants, history).
- Volumes hold non-tabular files under `/Volumes/<catalog>/<schema>/<volume>/`.
- Grants on a catalog or schema inherit to current and future children; you also need `USE CATALOG` and `USE SCHEMA`.

**Gold objects (§3)**
- Plain view: no copy, always fresh, cost paid at every query.
- Materialized view: precomputed results, refreshed on a schedule or trigger, fast reads, can lag.

### Exam traps

1. **RESTORE vs UNDROP:** `RESTORE` rolls back a table that still exists; `UNDROP` brings back a dropped one.
2. **History ≠ readable:** `DESCRIBE HISTORY` can show versions whose files were already vacuumed.
3. **"Serverless can do everything":** no custom init scripts, limited Spark configs.
4. **All-purpose for schedules:** almost always the wrong (expensive) answer.
5. **"Dropping an external table deletes data":** it doesn't; only metadata goes.
6. **MV vs view:** light query + must be fresh → view; heavy query + read often + small lag OK → MV.
7. **Missing `USE SCHEMA`:** `SELECT` alone isn't enough to query a table.
8. **DBFS root for governed files:** use a UC volume instead.

---

## Answers explained

**Q1 → A.** The transaction log in `_delta_log/` is the table: ordered commits make writes atomic and record each change. B) A metastore entry only names the folder; it adds no ACID. C) `_SUCCESS` just marks that a Spark job finished; readers can still see partial writes. D) Partitioning organizes files but doesn't coordinate writers.

**Q2 → B.** Storage and compute are separate, so the data lives in cloud storage, governed by UC, and any compute with access can read it. A) Tables never live on cluster disks. C) Access depends on UC permissions, not cluster names. D) There's no link between a table and the cluster that wrote it.

**Q3 → C.** Unattended scheduled work belongs on serverless jobs or job compute, which exist only for the run and cost less. A) Bigger all-purpose compute still bills while idle and at a higher rate. B) SQL warehouses run SQL, not a Python notebook. D) Turning off auto termination makes the idle cost worse.

**Q4 → D.** A serverless SQL warehouse starts in seconds and adds clusters as concurrency grows, then scales back. A) One fixed cluster is either too small at peak or wasted the rest of the day. B) Job compute is for scheduled runs and takes minutes to start. C) A single-node dedicated cluster serves one user.

**Q5 → A.** Notebook-scoped `%pip install` is supported on serverless (you can also use the Environment panel). B) Serverless doesn't run custom init scripts. C) `spark.jars.packages` is for JVM jars, and most Spark configs can't be set on serverless anyway. D) A GPU runtime isn't how you add a Python library, and it doesn't bundle this one.

**Q6 → B.** Predictive optimization vacuums files older than the 7-day default, so the version's data is gone even though the log (30 days) still lists it. A) `VERSION AS OF` is fully supported. C) Log history is kept 30 days by default, not 7. D) `OPTIMIZE` adds a new version; it never resets numbering.

**Q7 → B.** `RESTORE` rolls an existing table back to a good version in place, keeping its name and grants. Use `DESCRIBE HISTORY` to find `<v>`. A) `UNDROP` is only for a dropped table, and this one still exists. C) A deep clone of the current version copies the bad data, and it gives you a new name. D) Dropping and reloading is slow and loses the history and grants.

**Q8 → C.** For an external table, UC drops only the metadata; your files stay at the external location. A) and B) describe a managed table (B is the managed 7-day window). D) Nothing is moved into managed storage.

**Q9 → D.** Volumes govern non-tabular files in UC and are read through `/Volumes/<catalog>/<schema>/<volume>/`. A) Tables hold rows, not raw PDFs. B) DBFS root isn't governed by UC and is discouraged. C) A view is a saved query, not a place to store files.

**Q10 → B.** The query is heavy and read often, and an hour of lag is fine, so precompute it with a materialized view on a schedule. A) A plain view would rerun the four-minute join on every dashboard hit. C) A temporary view is still just a saved query, and it disappears with the session. D) A CTE is part of each query and recomputes every time.
*(This is the flip side of your diagnostic miss, where the query was light and had to be fresh, so a plain view won.)*

**Q11 → B.** Custom init scripts need classic compute, so move this task to classic job compute and attach the script there. A) Serverless environments install Python packages, not OS-level startup scripts. C) SQL warehouses don't run init scripts and don't run Python tasks. D) `%pip` installs Python packages only; it can't install OS packages or certificates.

---

## Want more? (optional, never homework)

- Your Friday deep dive: [Anatomy of a governed table (HTML)](https://raw.githack.com/mariusss21/databricks-study/main/study-plan-v2/W01-foundations/deepdive-anatomy-of-a-governed-table.html)
- Older notes: [Delta time travel and retention](../../explanations/delta-time-travel-and-retention.md) · [Platform wrap: Delta, UC and compute](../../explanations/platform-delta-unity-catalog-compute.md)
- Optional further reading: [community guide, compute and clusters](https://github.com/kengio/databricks-certification-study-guide/blob/main/certifications/data-engineer-associate/01-databricks-intelligence-platform/03-compute-clusters.md)

Official docs: [Delta history and time travel](https://docs.databricks.com/aws/en/delta/history) · [Serverless compute limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations) · [Managed vs external tables](https://docs.databricks.com/aws/en/tables/) · [Volumes](https://docs.databricks.com/aws/en/volumes/) · [Materialized views](https://docs.databricks.com/aws/en/views/materialized)
