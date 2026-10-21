# Data Engineering pills

Daily Data Engineering interview pills, written by **NJ in Dollar**. Each week follows the same theme as Gerald's Databricks pills (see [`../databricks-pills/`](../databricks-pills/README.md)): Gerald covers the Databricks side, and these pills cover the general Data Engineering concepts behind it.

## Weeks (newest first)

| Week | Dates | Theme |
|------|-------|-------|
| [2026-W43](2026-W43-ingestion-into-bronze/README.md) | 2026-10-19 to 2026-10-25 | Ingestion into Bronze |

## Naming convention

```
data-engineering/<YYYY>-W<ww>-<theme-slug>/NN-<ddd>-<YYYY-MM-DD>-<topic-slug>.md
```

- `<YYYY>-W<ww>`: ISO year and ISO week number (weeks start on Monday), e.g. `2026-W43`.
- `<theme-slug>`: the week's shared theme in kebab-case, e.g. `ingestion-into-bronze`.
- `NN`: day of the week, `01` (Monday) to `07` (Sunday).
- `<ddd>`: three-letter lowercase day name: `mon`, `tue`, `wed`, `thu`, `fri`, `sat`, `sun`.
- `<YYYY-MM-DD>`: the pill's date.
- `<topic-slug>`: the topic in kebab-case.

Each week folder has a `README.md` with the theme, the date range, and a table listing each day's Data Engineering topic (linked) and the paired Databricks pill from Gerald.
