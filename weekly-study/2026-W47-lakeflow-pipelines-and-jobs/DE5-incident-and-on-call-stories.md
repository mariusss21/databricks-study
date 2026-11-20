# DE5: Incident and on-call stories (detect, mitigate, communicate, learn)

- **Date:** Fri 2026-11-20
- **Week:** 2026-W47
- **Theme:** Lakeflow pipelines and Jobs in depth
- **Paired Databricks pill (Gerald):** DB5: Monitoring and troubleshooting Jobs: run history vs baseline, repair runs, notifications, system tables for jobs

---

Good morning, Mario. Gerald covers the **tools** today: run history, repair runs, notifications, and system tables for Jobs. This pill is the **interview story** behind them. Almost every US senior or tech-lead loop asks some version of "Tell me about a production incident you owned." They aren't testing whether you've had incidents. Everyone has. They want to see how you **detect, mitigate, communicate, and learn**.

## The incident lifecycle, in five verbs

1. **Detect.** Did an alert find it, or a stakeholder? "The CFO noticed" is a monitoring gap, and that gap belongs in your story.
2. **Mitigate.** Stop the bleeding first, then look for the root cause. Pause the downstream job, roll back the table, or serve yesterday's data with a banner.
3. **Communicate.** Say who's affected, what they should do, and when the next update comes, even when the update is "still investigating."
4. **Fix.** Fix it for real: the code, the data backfill, and the safe rerun.
5. **Learn.** Write a **blameless postmortem**: a timeline, contributing factors (usually more than one), and action items with owners and dates. Blameless means you ask "why did the system let this happen?", not "who did it?"

A **runbook** is the cheat sheet for the person on call: the symptom, how to check it, the safe actions, and when to escalate. Seniors write them after incident #1, so incident #2 takes 10 minutes.

## The Databricks link (this week's Lakeflow theme)

| Phase | What you'd use on Databricks |
|---|---|
| Detect | Job **notifications** on failure, **duration thresholds** (run-time health rules), pipeline **expectations** and the event log (DB2), and SQL alerts on `system.lakeflow.*` tables |
| Mitigate | Pause the schedule, then `RESTORE TABLE ... TO VERSION AS OF` (Delta time travel) on a batch-written table. On pipeline-managed streaming tables, do a full refresh instead |
| Fix | **Repair run**, which reruns only the failed tasks and the ones after them. It's safe only if your writes are **idempotent** (MERGE on keys, sequence guards from DE3) |
| Learn | `system.lakeflow.job_run_timeline` gives you the facts for the postmortem: when it started failing, how often, and the termination codes |

Senior line: *"Repair runs and time travel make recovery cheap, but only because we designed the writes to be idempotent. That's the real incident-readiness work."*

## Hands-on: failed or slow runs vs each job's baseline

```sql
-- Last 7 days: runs that failed or took over 2x their 30-day median
WITH runs AS (
  SELECT job_id, run_id, period_start_time, result_state,
         termination_code, run_duration_seconds
  FROM system.lakeflow.job_run_timeline
  WHERE period_start_time >= current_date() - INTERVAL 30 DAYS
    AND result_state IS NOT NULL   -- runs over 1h span rows; the final row has the result
),
baseline AS (
  SELECT job_id, percentile_approx(run_duration_seconds, 0.5) AS p50_s
  FROM runs WHERE result_state = 'SUCCEEDED'
  GROUP BY job_id
)
SELECT r.job_id, r.run_id, r.period_start_time, r.result_state,
       r.termination_code, r.run_duration_seconds, b.p50_s
FROM runs r JOIN baseline b USING (job_id)
WHERE r.period_start_time >= current_date() - INTERVAL 7 DAYS
  AND (r.result_state <> 'SUCCEEDED' OR r.run_duration_seconds > 2 * b.p50_s)
ORDER BY r.period_start_time DESC;
```

To recover a bad batch write:

```sql
DESCRIBE HISTORY silver.orders;                    -- find the last good version
RESTORE TABLE silver.orders TO VERSION AS OF 412;  -- then repair-run the job
```

(A restore shows up downstream as a change, so warn anyone reading the table's change data feed.)

## Interview question

**"Tell me about a production data incident you owned."**

**Strong answer shape (STAR + L).** Fill it with a real story of your own. The code factory scrapers or a Stack pipeline both work. Keep the numbers real.

- **Situation (15 s):** "At Stack, our [daily X pipeline] feeds [who]. One morning [symptom], and [impact: N dashboards / a client report / a delay of N hours]."
- **Task (10 s):** "I was the tech lead, so I owned the incident: getting it stable and keeping stakeholders informed."
- **Action (60 s, the bulk, and say *I*):** "First I **mitigated**: I [paused downstream / rolled back the table]. I posted a status update with the next update time. Then I found [the contributing factors, e.g. an upstream schema change plus a missing contract check]. I fixed it with [a backfill or rerun], which was safe because [the writes are idempotent]."
- **Result (15 s):** "We were back in [N hours]. Afterwards I ran a blameless postmortem, and we added [an alert, a contract test, a runbook]. [The same class of issue hasn't come back / detection went from hours to minutes]."
- **Learning (10 s):** "What I changed in how I lead: [e.g. every new source now ships with a freshness alert and a runbook entry]."

Senior signals: you mitigated before you looked for the root cause, you communicated without being asked, you talk about **contributing factors** rather than "the root cause," and the action items changed the **system**, not just the code.

Follow-up they'll ask: **"How would you set up on-call for a small data team?"** Give a short answer: severity levels tied to business impact, alerts only on things a human must act on (freshness, failures on critical-path jobs, cost spikes), a weekly rotation with a handoff note, runbooks linked from each alert, and a monthly review of noisy alerts.

## English phrasing tips

- **"Mitigate" vs. "fix":** "I mitigated in 20 minutes; the full fix shipped the next day." This shows you know the difference.
- Say **"contributing factors,"** not "the guilty one" or "whose fault."
- Use **"I"** for your actions and **"we"** for the team's results. Interviewers in the US are listening for *your* part.
- *Blameless* is said BLAME-less, and *postmortem* is said post-MOR-tem.

## Go deeper

- [Troubleshoot and repair job failures (Databricks docs)](https://docs.databricks.com/aws/en/jobs/repair-job-failures)
- [Jobs system table reference (Databricks docs)](https://docs.databricks.com/aws/en/admin/system-tables/jobs)
- [Postmortem culture: learning from failure (Google SRE book)](https://sre.google/sre-book/postmortem-culture/)
