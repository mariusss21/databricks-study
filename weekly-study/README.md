# Weekly study

One folder per week holds all of that week's material: Gerald's daily Databricks pills, NJ in Dollar's daily Data Engineering pills, and Zé Carlos's Databricks and Data Engineering deep dives. Gerald picks each week's theme and the others follow it.

## Weeks

Newest first.

| Week | Theme | Dates |
|---|---|---|
| [2026-W44](2026-W44-bronze-to-silver-transforms/README.md) | Bronze to Silver transforms | Mon 2026-10-26 to Sun 2026-11-01 |
| [2026-W43](2026-W43-ingestion-into-bronze/README.md) | Ingestion into Bronze | Mon 2026-10-19 to Sun 2026-10-25 |

## Naming convention

```
weekly-study/<YYYY>-W<ww>-<theme-slug>/
  README.md                  theme, dates, day-by-day table, deep dives, connection
  DB<n>-<topic-slug>.md      Databricks pill (Gerald), n = 1 (Mon) .. 7 (Sun)
  DE<n>-<topic-slug>.md      Data Engineering pill (NJ in Dollar), n = 1 (Mon) .. 7 (Sun)
  deepdive-db-<slug>.html    Monday Databricks deep dive (Zé Carlos)
  deepdive-de-<slug>.html    Friday Data Engineering deep dive (Zé Carlos)
```

- `<YYYY>-W<ww>`: ISO year and ISO week number (weeks start on Monday), e.g. `2026-W43`.
- `<theme-slug>`, `<topic-slug>`, `<slug>`: kebab-case, e.g. `ingestion-into-bronze`.
- An optional Markdown brief behind a deep dive sits next to it as `deepdive-<db|de>-<slug>-brief.md`.
