# Deep dives

Weekly interactive deep dives by **Zé Carlos**. Each one is a single self-contained HTML file and follows the week's shared theme (set by Gerald's Databricks pills).

## Layout

```
deep-dives/<YYYY>-W<ww>-<theme-slug>/<YYYY-MM-DD>-<track>-<slug>.html
deep-dives/<YYYY>-W<ww>-<theme-slug>/briefs/   (optional: the Markdown briefs behind each deep dive)
```

- `<YYYY>-W<ww>`: ISO year and ISO week number (weeks start on Monday), e.g. `2026-W43`.
- `<theme-slug>`: the week's shared theme in kebab-case, e.g. `ingestion-into-bronze`.
- `<YYYY-MM-DD>`: the deep dive's date.
- `<track>`: one of `databricks`, `ai-engineering`, `data-engineering`.
- `<slug>`: the topic in kebab-case.

## Opening a deep dive

The HTML files open directly in a browser; no server or build step is needed. Either download the file and double-click it, or open it through a raw-HTML preview service. GitHub's normal file view shows the source, not the rendered page.
