# W1 · P2 — Compute choices and separating storage from compute

**Wed Nov 18, 2026 · ~15–20 min · Exam Section 1 (Databricks Intelligence Platform), with a nod to Section 6 (cost and optimization)**
⚙ Exam topic: serverless vs classic all-purpose vs jobs compute vs SQL warehouses; limits and cost models
🧱 Fundamental: why storage and compute are split, elasticity, and what actually drives cost

---

## 0) Warm-up (answer before reading; answers in §5)

**W1.** Forty analysts run ad hoc SQL against curated Gold tables. Demand spikes at 9 AM and is near zero at night. What's the best fit?
- A) One large all-purpose cluster that stays on all day
- B) A serverless SQL warehouse with autoscaling (max clusters raised above 1)
- C) A jobs compute cluster scheduled every 5 minutes
- D) A single-node cluster per analyst

**W2.** A nightly ETL notebook runs at 2 AM on a shared all-purpose cluster. The bill is high. What's the simplest fix?
- A) Turn on Photon
- B) Run it as a Lakeflow Job on serverless or on jobs compute
- C) Add more workers so it finishes faster
- D) Switch the table to an external table

**W3.** Which statement about separating storage from compute is **true**?
- A) When a cluster terminates, the Delta tables it wrote are deleted
- B) Two different compute resources can read the same Delta table at the same time without copying it
- C) Each SQL warehouse keeps its own private copy of the data
- D) Storage must be resized whenever you add workers

**W4.** Your code needs an R library and a custom init script. Which compute should you pick?
- A) Serverless notebook compute
- B) Serverless SQL warehouse
- C) Classic compute (all-purpose for dev, jobs compute for production) with dedicated access mode
- D) Any compute; they all support init scripts

---

## 1) The idea in one breath

Your data sits once in cheap object storage (S3/ADLS/GCS) as Delta tables; compute is rented engines that come and go and read it. So the real question is just *which engine, sized how, running when*. Pick the engine that fits the workload (interactive, scheduled, or SQL/BI), and only pay while it's working.

## 2) The fundamental (tool-agnostic)

**Coupled vs decoupled architecture**

| | Coupled (classic on-prem warehouse, old Hadoop) | Decoupled (cloud lakehouse, Snowflake, BigQuery…) |
|---|---|---|
| Scaling | Need more CPU? Buy more disks too (and the other way round) | Scale compute and storage on their own |
| Idle cost | Machines run 24/7 because they hold the data | Turn compute off; data stays safe in storage |
| Concurrency | Every workload fights over one cluster | Separate engines for BI, ETL, ML, all reading the same copy |
| Cost | Mostly fixed | **Storage** (cheap, per GB-month) + **compute** (expensive, per second of use) |
| Catch | Data locality is fast | Network reads from object storage → caching, file layout and pruning matter (W4) |

**Elasticity** = changing compute size to match load: *scale up* (bigger nodes), *scale out* (more nodes or more clusters), and *scale to zero* (turn off when idle). Two different problems:
- **Big queries / big jobs** → more workers or bigger nodes (throughput).
- **Many concurrent users** → more *clusters* behind one endpoint (concurrency).

**What really drives cost:** compute time × compute size. Idle compute is the classic waste: a cluster left running overnight, or an interactive cluster used for scheduled jobs.

## 3) How Databricks does it

| Compute | Who manages it | Startup | Best for | You pay for |
|---|---|---|---|---|
| **Serverless** (notebooks, jobs, Lakeflow pipelines) | Databricks; runs outside your cloud account | Seconds | Most new notebooks, jobs, pipelines | DBUs for the work, with idle time kept minimal |
| **All-purpose** (classic) | You: node types, workers, runtime, policies | Minutes | Interactive dev, shared exploration | DBUs + cloud VMs while it's up, **idle time included** until auto termination |
| **Jobs compute** (classic) | You define it in the job; created per run, terminated after | Minutes (faster from a pool) | Scheduled production jobs needing classic controls | Only the run, at the cheaper jobs SKU |
| **SQL warehouse** (serverless / pro / classic) | Serverless: Databricks. Pro/classic: your account | Serverless ~seconds; pro/classic minutes | BI, dashboards, ad hoc SQL | DBUs per warehouse size while running; auto stop ends billing |

Small details worth knowing:
- A **DBU** is a normalized unit of processing power, billed per second. The rate depends on compute type and size. It is *not* "one hour on one node".
- **Serverless** always runs the latest runtime (versionless). You manage libraries with *environments*, not cluster libraries, and most Spark configs can't be set. It needs Unity Catalog.
- **Classic knobs:** autoscaling (min/max workers), auto termination (all-purpose), **spot** instances for workers (the driver stays on-demand), **instance pools** for faster starts (idle pool VMs still cost cloud money), **compute policies** so admins can cap sizes, **single node** for small data or pandas-style work.
- **Access modes (classic):** *Standard* (shared; many users with isolation) vs *Dedicated* (one user or group; needed for things like R, ML runtime, GPUs).
- **SQL warehouse scaling:** a warehouse is a set of clusters. Its *size* (2X-Small … 4X-Large) handles heavier queries. *Max clusters* handles more concurrent users. Default min = max = 1, so raise max for concurrency.

Find who's spending what (system tables are the exam-friendly answer):

```sql
-- DBUs per product and SKU over the last 7 days
SELECT
  billing_origin_product,          -- e.g. JOBS, SQL, INTERACTIVE, DLT
  sku_name,
  SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 7 DAYS
GROUP BY ALL
ORDER BY dbus DESC;
```

Storage/compute separation in action: the *same* table can be read by a SQL warehouse for a dashboard and by a serverless job for ETL at the same time. No copy, and Delta's snapshot isolation (P1) keeps both consistent.

Your older notes go deeper: [`explanations/platform-delta-unity-catalog-compute.md`](../../../explanations/platform-delta-unity-catalog-compute.md) and [`explanations/spark-parallelism-single-node-and-serverless.md`](../../../explanations/spark-parallelism-single-node-and-serverless.md).

## 4) Tradeoffs and exam traps

- **Trap: scheduled job on all-purpose compute.** It works, but it costs more (higher SKU, plus idle time). Production schedules → **serverless jobs** or **jobs compute**.
- **Trap: "add workers" for concurrency.** Many users means more **clusters** on the SQL warehouse (max clusters). A bigger *size* helps heavy queries, not queue length.
- **Trap: serverless can do everything.** No custom init scripts, no R, no GPUs, very few Spark configs. Those need **classic** compute.
- **Trap: spot driver.** Only workers use spot. Losing the driver kills the job.
- **Trap: pools are free.** Idle pool instances still bill the cloud provider (not DBUs).
- **Trap: terminating compute deletes data.** No. Tables live in storage; only cluster state and cache go away.
- **Tradeoff:** serverless = fast start, no tuning, less control. Classic = full control, slower start, and you manage idle cost.
- **Tradeoff:** separation makes reads go over the network, so file sizes, clustering and caching matter (W4 deep dive).

## 5) Warm-up answers

**W1 → B (serverless SQL warehouse, autoscaling).** Many users on spiky SQL is the warehouse's job: it adds clusters at 9 AM, stops when idle, and starts in seconds. A) One always-on cluster pays for the night and still queues users at peak. C) Jobs compute is for scheduled runs, not interactive queries. D) Many idle single-node clusters cost a lot and are built for small single-machine work, not shared BI.

**W2 → B (Lakeflow Job on serverless or jobs compute).** Scheduled work should run on compute that exists only for the run, at the jobs SKU. A) Photon may speed it up, but you still pay all-purpose rates and idle time. C) More workers might finish sooner but doesn't fix the wrong compute type. D) Managed vs external is about where data lives and who owns its lifecycle, not compute cost.

**W3 → B (two engines, one copy).** That's the point of decoupling: any authorised compute reads the same Delta files. A) Terminating compute never deletes table data. C) Warehouses cache, but they don't own private copies. D) Object storage grows on its own, separate from compute.

**W4 → C (classic, dedicated access).** Init scripts and R need classic compute, and R needs dedicated access mode. A/B) Serverless hides these controls. D) Serverless doesn't support custom init scripts.

## 6) Interview angle

*"How would you cut a Databricks bill that doubled last quarter?"*
Start with data, not guesses: query `system.billing.usage` by product, SKU, job and warehouse to find the top spenders. Then fix in order: (1) move scheduled work off all-purpose compute onto serverless or jobs compute; (2) set auto termination and warehouse auto stop; (3) right-size (SQL warehouse size vs max clusters, autoscaling ranges, single node for small data); (4) spot workers for fault-tolerant jobs; (5) compute policies so it doesn't creep back. End with the fundamental: storage is cheap and compute is the bill, so pay for compute only while it's doing useful work.

## 7) Optional hands-on (10 min)

1. Open **Compute** in your workspace and look at the tabs (all-purpose, job compute, SQL warehouses, pools, policies). Spot which ones you can create.
2. In the SQL editor on any warehouse, run the `system.billing.usage` query above (you need SELECT on `system.billing`; skip it if you don't have access).
3. Open a SQL warehouse's settings and find **size**, **auto stop**, and **scaling (min/max clusters)**. Don't change shared settings.

## Official docs

- [Compute overview](https://docs.databricks.com/aws/en/compute/)
- [Serverless compute](https://docs.databricks.com/aws/en/compute/serverless/)
- [Compute configuration reference](https://docs.databricks.com/aws/en/compute/configure)
- [SQL warehouse types](https://docs.databricks.com/aws/en/compute/sql-warehouse/warehouse-types)
- [Billable usage system table](https://docs.databricks.com/aws/en/admin/system-tables/billing)
- [Exam guide](https://www.databricks.com/learn/certification/data-engineer-associate)

*Optional further reading (not homework):* [kengio guide: Compute and Clusters](https://github.com/kengio/databricks-certification-study-guide/blob/main/certifications/data-engineer-associate/01-databricks-intelligence-platform/03-compute-clusters.md)

---
*Reply with your warm-up answers (e.g. `W1 B, W2 C ...`). Any misses go into Monday's review.*
