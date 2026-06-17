#!/usr/bin/env bash
# Entry point for both the VM cron job and the Kubernetes CronJob: fetch new
# Airbyte sync logs, categorize them, and (optionally) email the triage report
# when there are findings.
#
# Environment:
#   AIRBYTE_API_URL / AIRBYTE_API_USER / AIRBYTE_API_PASSWORD  (see fetch-airbyte-logs.py)
#   TRIAGE_WORK_DIR    working directory (default: ~/.sync-triage; use a mounted
#                      volume path in Kubernetes so state survives between runs)
#   TRIAGE_FETCH_ALL   if set to 1, fetch succeeded jobs as well so
#                      record-count sanity checks run on healthy-looking syncs
#
# Notification (only sent when there are findings), in order of preference:
#   SMTP_HOST (+ SMTP_FROM/SMTP_TO/...)  container-friendly; see notify.py
#   TRIAGE_EMAIL_TO                      uses mailx if available (VM hosts)
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

if [ "$HAS_FINDINGS" = "1" ]; then
  COUNT="$(echo "$NEW_LOGS" | wc -l | tr -d ' ')"
  SUBJECT="Airbyte sync triage: findings in $COUNT sync(s)"
  if [ -n "${SMTP_HOST:-}" ]; then
    python3 "$SKILL_DIR/scripts/notify.py" --subject "$SUBJECT" "$REPORT"
  elif [ -n "${TRIAGE_EMAIL_TO:-}" ] && command -v mailx >/dev/null 2>&1; then
    mailx -s "$SUBJECT" "$TRIAGE_EMAIL_TO" < "$REPORT"
  else
    echo "Findings present; no notifier configured. Report at $REPORT" >&2
  fi
fi
