# Analyze Airbyte Sync Logs

For the automated deployment handoff, start with the **[handoff documentation](handoff/README.md)**.
It includes the solution guide, deployment steps, JSON reference, and knowledge-base
maintenance instructions in a self-contained release snapshot. Run its deployment
commands from `handoff/`.

An AI coding assistant skill to generate human-readable diagnostic reports from Airbyte connector sync logs.

Compatible with [OpenAI Codex](https://openai.com/index/codex/) and [Claude Code](https://docs.anthropic.com/en/docs/claude-code).

## Overview

Parses Airbyte sync log files and produces structured reports with:
- Sync status, duration, and timing
- Source and destination versions
- Configuration summaries
- Per-stream record counts
- Destination write statistics by model
- State tracking (initial and final)
- Errors and warnings, **categorized against documented failure patterns with remediation steps**
- Record-count sanity checks (skipped/errored records, read-vs-written deltas, silent write failures)
- Rate-limit attribution (how much sync time was spent waiting on API throttling)
- Cross-connector triage: analyze many logs at once and get a severity-ordered, worst-first summary

All output is secret-redacted: tokens, API keys, passwords, and common credential formats are masked before anything is emitted.

## Installation

Clone the skill into your project:

```bash
git clone https://github.com/faros-ai/analyze-airbyte-sync-logs-skill .agents/skills/analyze-sync-logs
```

Or add as a submodule:

```bash
git submodule add https://github.com/faros-ai/analyze-airbyte-sync-logs-skill .agents/skills/analyze-sync-logs
```

### Claude Code

Claude Code looks for skills in `.claude/skills/`. Create a symlink:

```bash
mkdir -p .claude/skills
ln -s ../../.agents/skills/analyze-sync-logs .claude/skills/analyze-sync-logs
```

## Usage

Ask your AI assistant to analyze a sync log:

- "Analyze the sync log at `/path/to/sync_log.txt`"
- "What happened in this sync?"
- "Compare these two sync logs"
- "Were there any errors in this sync?"
- "Triage all the logs in `/path/to/logs/` and tell me what to fix first"
- "What does this mean: 'Failure — sonarqube-feed exit code was 1'?"

### Standalone (no AI assistant)

The categorizer also runs directly:

```bash
# Single log: enriched JSON with categories, severities, remediation
python3 scripts/categorize.py sync_log.txt

# Many logs: adds a cross-connector triage summary, worst first
python3 scripts/categorize.py logs/*.log

# Standalone Markdown report (shareable artifact)
python3 scripts/categorize.py logs/*.log --report triage.md

# Categorize a status string from the Airbyte Sources page
python3 scripts/categorize.py --status-text "Failure — sonarqube-feed exit code was 1"
```

The raw parser remains available and unchanged: `python3 scripts/analyze-sync-logs.py sync_log.txt`.

### Extending the knowledge base

Error patterns live in `scripts/error_patterns.json`. Each entry has an `id`, `category`, `severity`, `provenance` (`faros-doc` or `generic`), regex `match.any` list, `summary`, `remediation` steps, and an optional `source_doc` link. Patterns are evaluated in order; the first match wins.

To add site-specific patterns without touching the bundled KB, create `scripts/error_patterns.local.json` with the same structure — overlay entries take priority. Unmatched errors are always surfaced verbatim as `uncategorized`, never dropped.

### Automated triage

For continuous triage without anyone in the loop, run the analyzer on a schedule on infrastructure that can reach the Airbyte API. Nothing stays resident between runs and no AI assistant is involved.

```
schedule → fetch-airbyte-logs.py → categorize.py --report → emit-sync-events.py → JSON lines
                                           └─ full reports on disk; optional email
```

In both cases:

- `fetch-airbyte-logs.py` polls the Airbyte API for sync jobs that finished since the last poll (tracked in a state file), and by default fetches logs for **failed and incomplete** jobs only.
- Set `TRIAGE_FETCH_ALL=1` to also fetch succeeded jobs, so record-count sanity checks catch silent anomalies in "successful" syncs.
- Each analyzed sync emits one compact, redacted `airbyte.sync.triage` JSON event to stdout, even if it has no findings. No newly analyzed syncs means no stdout; operational diagnostics still use stderr.
- Full Markdown stays on disk by default. Set `TRIAGE_STDOUT_REPORT=1` to also print reports with findings between `TRIAGE-REPORT-BEGIN` / `TRIAGE-REPORT-END` markers.
- A notification is sent only when there are findings, via SMTP (`SMTP_*`, container-friendly) or `mailx` (`TRIAGE_EMAIL_TO`, on VM hosts). The recipient can be a distribution list or a Teams channel email address.
- Reports and enriched JSON accumulate in the work directory (`TRIAGE_WORK_DIR`).
- Reports and JSON events pass through the secret-redaction layer before they are written or sent.

See the [event schema and Datadog notes](deploy/k8s/README.md#json-event-fields-and-datadog) for field meanings and collection prerequisites.

#### On a VM / docker-compose host (cron)

1. Clone this repo onto the host, e.g. `/opt/sync-analyzer`.
2. Provide the API endpoint and (if required) credentials via environment variables — never hardcode them:
   - `AIRBYTE_API_URL` (default `http://localhost:8000`)
   - `AIRBYTE_API_USER` / `AIRBYTE_API_PASSWORD` (optional basic auth)
3. Add a crontab entry:

```cron
*/30 * * * * AIRBYTE_API_URL=http://localhost:8000 TRIAGE_EMAIL_TO=alerts@example.com /opt/sync-analyzer/scripts/triage-cron.sh >> $HOME/.sync-triage/cron.log 2>&1
```

#### On Kubernetes (CronJob)

If Airbyte is deployed in a Kubernetes cluster, deploy the analyzer as a `CronJob` in the same cluster. The scripts are delivered as a ConfigMap (no image build, no registry, no in-cluster git pull) and state persists on a PVC. See **[`deploy/k8s/`](deploy/k8s/README.md)** for the manifests and a step-by-step guide, including commands to discover the in-cluster Airbyte API URL.

```bash
cd deploy/k8s && kubectl apply -k .
```

### Tests

```bash
python3 tests/run_tests.py
```

Synthetic logs in `tests/logs/` cover each pattern class, redaction, triage, rate-limit aggregation, and malformed input. No customer data.

## Output

The skill extracts structured data from the log file, which the AI assistant then uses to generate a human-readable report tailored to your question. Reports typically include:

- Sync status summary (success/failure, duration)
- Record counts per stream
- Destination write statistics
- Errors and warnings with context
- State changes between syncs
- Comparisons when analyzing multiple logs

## Common Queries

| Question | What to Ask |
|----------|-------------|
| Did the sync succeed? | "What's the sync status?" |
| How many records synced? | "Show records per stream" |
| Any errors? | "Were there any errors?" |
| Compare two syncs | "Compare these logs and highlight differences" |
| Triage many connectors | "Categorize the errors in these logs and tell me what to fix first" |
| Why was a sync slow? | "How much of this sync was spent rate-limited?" |
| Decode a status string | "What does 'exit code was 1' mean for this source?" |
| Check state preservation | "What's the initial and final state for X stream?" |
| Connector versions | "What source and destination versions were used?" |

## License

Apache 2.0 - see [LICENSE](LICENSE)
