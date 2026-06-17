---
name: analyze-sync-logs
description: Generate a human-readable report from Airbyte sync log(s), with errors categorized against documented Faros failure patterns and paired with remediation steps. Use when analyzing connector sync failures, comparing sync runs, triaging errors across many connectors, or debugging connector issues.
allowed-tools: Bash(python3:*)
---

Analyze the provided Airbyte sync log file(s) and present a concise diagnostic report with categorized errors and remediation guidance.

Arguments: $ARGUMENTS (one or more log file paths, or a quoted status string from the Airbyte Sources page)

Steps:

1. Determine the absolute path of the directory containing this SKILL.md file (call it SKILL_DIR).
2. Run the categorizer on all inputs at once:
   - Log files: `python3 $SKILL_DIR/scripts/categorize.py <file> [<file> ...]`
   - A status string instead of (or in addition to) log files: `python3 $SKILL_DIR/scripts/categorize.py --status-text "<status string>"`
   - The output embeds the full parser output for each log under `logs[].parsed`, so a separate parser run is not needed. (For raw parse output only, `scripts/analyze-sync-logs.py <file>` still works unchanged.)
3. Parse the JSON output. All output is already secret-redacted; never quote raw log lines that the categorizer has not redacted.
4. Present a report for each log covering:
   - Sync status, duration, start/end times
   - Source and destination versions and images
   - Key source config values that are present (e.g. url, cutoff_days, start_date, bucket_id/bucket_total, page_size)
   - Key destination config values that are present (e.g. origin, edition, graph, graphql_api)
   - Catalog: number of streams, list stream names with sync modes
   - Records per stream (source reads) with total
   - Destination stats: total read/processed/written/skipped/errored
   - Top destination models by write count
   - **Categorized errors and warnings** (`logs[].findings`), grouped by severity (critical → high → medium → low). For each finding show: category, summary, the matched message, remediation steps, and the source doc link when present. Findings with category `uncategorized` must still be shown verbatim — never drop them.
   - **Record-count sanity** (`logs[].record_count_sanity.flags`): surface any flags (skipped/errored records, read-vs-written deltas, zero-write anomalies) with their severity.
   - **Rate-limit attribution** (`logs[].rate_limit`): if `events > 0`, report the number of backoff events, total wait time, and what fraction of the sync duration was spent waiting — this explains slow syncs.
   - **Suppressed noise** (`logs[].suppressed`): if `count > 0`, note in one line how many non-actionable platform/orchestrator log lines were filtered out (e.g. resource-cleanup, runtime warnings), so the omission is transparent. Do not itemize them.
5. If multiple logs are provided, lead with the triage summary (`triage`):
   - Connectors ordered worst-first (`triage.prioritized`), with status, worst severity, and finding counts
   - Finding counts by severity and by category
   - Then per-log details as above, worst connector first
6. If multiple logs are for the same connection, add a comparison section:
   - Flag any differences in versions, config, or catalog
   - Show record count changes per stream (delta and direction)
   - Show model write count changes (top movers)
   - Highlight anything that could explain failures or anomalies (e.g. large record count swings, new errors, status changes)
7. End with a brief "Key observations" section calling out anything notable
8. If the user wants a shareable artifact, rerun with `--report <path>.md` to emit a standalone Markdown triage report.

Notes:

- The pattern knowledge base lives in `scripts/error_patterns.json`. Site-specific patterns can be added in `scripts/error_patterns.local.json` (same structure); overlay entries take priority over the bundled KB.
- `match_confidence` is `high` for patterns sourced from Faros documentation and `medium` for generic industry patterns; mention low-confidence matches as "likely" rather than definitive.
