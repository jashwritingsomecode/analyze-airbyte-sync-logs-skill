#!/usr/bin/env bash
# Cron entry point: fetch new Airbyte sync logs, categorize them, and
# (optionally) email the triage report when there are findings.
#
# Environment:
#   AIRBYTE_API_URL / AIRBYTE_API_USER / AIRBYTE_API_PASSWORD  (see fetch-airbyte-logs.py)
#   TRIAGE_WORK_DIR    working directory (default: ~/.sync-triage)
#   TRIAGE_EMAIL_TO    if set, email the report here via mailx (a Teams
#                      channel email address works too)
#   TRIAGE_FETCH_ALL   if set to 1, fetch succeeded jobs as well so
#                      record-count sanity checks run on healthy-looking syncs
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="${TRIAGE_WORK_DIR:-$HOME/.sync-triage}"
LOG_DIR="$WORK_DIR/logs"
mkdir -p "$LOG_DIR"

FETCH_ARGS=(--output-dir "$LOG_DIR" --state "$WORK_DIR/state.json")
if [ "${TRIAGE_FETCH_ALL:-0}" = "1" ]; then
  FETCH_ARGS+=(--all)
fi

NEW_LOGS="$(python3 "$SKILL_DIR/scripts/fetch-airbyte-logs.py" "${FETCH_ARGS[@]}")"
if [ -z "$NEW_LOGS" ]; then
  exit 0
fi

STAMP="$(date +%Y%m%d-%H%M%S)"
REPORT="$WORK_DIR/triage-$STAMP.md"
JSON_OUT="$WORK_DIR/triage-$STAMP.json"

# Connection names are sanitized to [A-Za-z0-9._-], so xargs is safe here.
echo "$NEW_LOGS" | xargs python3 "$SKILL_DIR/scripts/categorize.py" \
  --report "$REPORT" --compact > "$JSON_OUT"

HAS_FINDINGS="$(python3 - "$JSON_OUT" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
t = d["triage"]
flags = any(l["record_count_sanity"]["flags"] for l in d["logs"])
print(1 if (t["findings_by_severity"] or flags) else 0)
PY
)"

if [ "$HAS_FINDINGS" = "1" ] && [ -n "${TRIAGE_EMAIL_TO:-}" ]; then
  mailx -s "Airbyte sync triage: findings in $(echo "$NEW_LOGS" | wc -l | tr -d ' ') sync(s)" \
    "$TRIAGE_EMAIL_TO" < "$REPORT"
fi
