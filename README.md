# Certification Study Log, AI-assisted

Mario Carvalho, a data engineer, studies for certifications with an AI study coach (Grok Bot). The coach writes original daily pills, retrieval-practice recall checks, and weekly reviews, and tracks mistakes by exam section. All questions are original, never exam dumps.

## How it works

- Light load, designed around focus: one short piece on weekdays.
- Spaced review on Mondays: a quiz plus a one-page cheat sheet.
- Evening recall checks, to pull the day's ideas back from memory.
- A misses log, grouped by exam section and brought back in later reviews.
- Everything is committed here, week by week.

## Certifications

| Certification | Folder | Status |
|---|---|---|
| Databricks Certified Data Engineer Associate | [certifications/databricks-data-engineer-associate/](certifications/databricks-data-engineer-associate/) | In progress (exam Jan 2027) |

## Repo structure

Each certification is one folder:

```
certifications/<cert-slug>/
  README.md       plan, weekly rhythm, and an index of every piece
  misses.md       questions missed, grouped by exam section
  W01-<theme>/    that week's pills, Friday deep dive, recall/, and assets/
  W02-<theme>/
  ...
```

A future certification is a new sibling folder under `certifications/`, with the same layout.

## Other material

- `explanations/`: short explainers on Delta and ACID, Unity Catalog, compute, the medallion architecture, and related topics.
- `courses/`: Databricks Academy notes (Introduction to Apache Spark, Developing Applications with Apache Spark, Structured Streaming, Monitoring and Optimization).
