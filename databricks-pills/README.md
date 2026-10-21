# Databricks pills

Daily Databricks pills, maintained by **Gerald**, Mario's Databricks tutor bot. Gerald picks each week's theme; NJ in Dollar's Data Engineering pills ([`../data-engineering/`](../data-engineering/README.md)) and Zé Carlos's deep dives ([`../deep-dives/`](../deep-dives/README.md)) follow it.

## Naming convention

Same layout and naming as the Data Engineering pills:

```
databricks-pills/<YYYY>-W<ww>-<theme-slug>/NN-<ddd>-<YYYY-MM-DD>-<topic-slug>.md
```

- `<YYYY>-W<ww>`: ISO year and ISO week number (weeks start on Monday), e.g. `2026-W43`.
- `<theme-slug>`: the week's shared theme in kebab-case, e.g. `ingestion-into-bronze`.
- `NN`: day of the week, `01` (Monday) to `07` (Sunday).
- `<ddd>`: three-letter lowercase day name: `mon`, `tue`, `wed`, `thu`, `fri`, `sat`, `sun`.
- `<YYYY-MM-DD>`: the pill's date.
- `<topic-slug>`: the topic in kebab-case.

Each week folder gets a `README.md` with the theme, the date range, and the list of that week's pills.
