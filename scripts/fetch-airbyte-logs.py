#!/usr/bin/env python3
"""Fetch new terminal Airbyte sync job logs via the Airbyte API.

Polls the API for sync jobs that finished since the last run (tracked in a
state file), writes each job's log text to a file, and prints the file paths
to stdout — ready to pipe into categorize.py. Designed to run from cron;
between runs nothing is resident.

Connection/job discovery prefers the Config API (POST /api/v1/...) and falls
back automatically to the Public API (GET /api/public/v1/...) on a 404, so the
tool works against both legacy and Public-API-only (Airbyte 1.x) instances.
Log *content* is retrieved from the Config API attempt/get_for_job endpoint
(with a legacy jobs/get fallback); the Public API exposes no structured
per-attempt log endpoint, so if the Config API is fully blocked, log retrieval
will fail with a clear per-job warning rather than write empty files.

Endpoint and credentials come from environment variables (never hardcoded):
  AIRBYTE_API_URL       base URL, e.g. http://localhost:8000 (default)
  AIRBYTE_API_USER      optional basic-auth user
  AIRBYTE_API_PASSWORD  optional basic-auth password

Zero dependencies beyond the Python standard library.
"""

import argparse
import base64
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

DEFAULT_API_URL = "http://localhost:8000"
TERMINAL_STATUSES = {"succeeded", "failed", "cancelled", "incomplete"}
DEFAULT_FETCH_STATUSES = {"failed", "incomplete"}
STATE_ID_CAP = 5000


def _auth_header():
    user = os.environ.get("AIRBYTE_API_USER")
    password = os.environ.get("AIRBYTE_API_PASSWORD")
    if user and password:
        creds = base64.b64encode(f"{user}:{password}".encode()).decode()
        return f"Basic {creds}"
    return None


def api_request(base_url, path_or_url, method="GET", payload=None, timeout=30):
    """Single HTTP entry point for both API families. Accepts an absolute URL
    (used by Public API pagination, whose `next` links may be absolute) or a
    path joined onto base_url. Sends a JSON body only when payload is given."""
    headers = {"Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    auth = _auth_header()
    if auth:
        headers["Authorization"] = auth
    url = path_or_url if re.match(r"^https?://", path_or_url) else base_url.rstrip("/") + path_or_url
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def api_post(base_url, path, payload, timeout=30):
    return api_request(base_url, path, method="POST", payload=payload, timeout=timeout)


def api_get(base_url, path_or_url, timeout=30):
    return api_request(base_url, path_or_url, method="GET", timeout=timeout)


# Which API family answers discovery calls on this instance: "config"
# (POST /api/v1/...) or "public" (GET /api/public/v1/...). Learned from the
# first 404 and reused, so we don't re-probe the dead family on every call.
_API_MODE = {"discovery": None}


def list_connections(base_url):
    """List sync connections, preferring the Config API and falling back to the
    Public API (v1.8+) if the Config endpoint is absent (404)."""
    if _API_MODE["discovery"] == "public":
        return _list_connections_public(base_url)
    try:
        conns = _list_connections_config(base_url)
        _API_MODE["discovery"] = "config"
        return conns
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise  # auth/transient — surface it, don't mask as "no Config API"
    _API_MODE["discovery"] = "public"
    return _list_connections_public(base_url)


def _list_connections_config(base_url):
    connections = []
    workspaces = api_post(base_url, "/api/v1/workspaces/list", {}).get("workspaces", [])
    for ws in workspaces:
        ws_id = ws.get("workspaceId")
        if not ws_id:
            continue
        resp = api_post(base_url, "/api/v1/connections/list", {"workspaceId": ws_id})
        connections.extend(resp.get("connections", []))
    return connections


def _list_connections_public(base_url):
    """Airbyte Public API: GET /api/public/v1/connections, cursor-paginated via
    a `next` link. Connection objects already expose connectionId/name, matching
    what the Config API returns, so no per-field remap is needed here."""
    connections = []
    next_url = "/api/public/v1/connections?limit=100&offset=0"
    while next_url:
        resp = api_get(base_url, next_url)
        connections.extend(resp.get("data", []))
        next_url = resp.get("next") or None
    return connections


def list_jobs(base_url, connection_id, page_size):
    """List recent sync jobs for a connection in whichever API family is live."""
    if _API_MODE["discovery"] == "public":
        return _list_jobs_public(base_url, connection_id, page_size)
    try:
        return _list_jobs_config(base_url, connection_id, page_size)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
        _API_MODE["discovery"] = "public"
        return _list_jobs_public(base_url, connection_id, page_size)


def _list_jobs_config(base_url, connection_id, page_size):
    resp = api_post(base_url, "/api/v1/jobs/list", {
        "configTypes": ["sync"],
        "configId": connection_id,
        "pagination": {"pageSize": page_size, "rowOffset": 0},
    })
    return resp.get("jobs", [])


def _list_jobs_public(base_url, connection_id, page_size):
    """Airbyte Public API: GET /api/public/v1/jobs. Returns flat job objects
    (jobId/status, no nested attempts), so normalize each into the Config-API
    {"job": {...}, "attempts": [...]} shape the rest of the script expects."""
    resp = api_get(
        base_url,
        f"/api/public/v1/jobs?connectionId={connection_id}&jobType=sync&limit={page_size}",
    )
    return [_normalize_public_job(j) for j in resp.get("data", [])]


def _normalize_public_job(j):
    """Adapt a Public API job to the Config API entry shape. Public jobs carry
    no attempt list; synthesize one from attemptCount (when present) so
    _latest_attempt_number resolves the right attemptNumber for log retrieval."""
    n = j.get("attemptCount")
    attempts = [{"attempt": {"id": i}} for i in range(n)] if isinstance(n, int) and n > 0 else []
    return {
        "job": {"id": j.get("jobId"), "status": j.get("status")},
        "attempts": attempts,
    }


_LEVEL_MAP = {"WARNING": "WARN", "ERR": "ERROR", "FATAL": "ERROR", "SEVERE": "ERROR"}
_KNOWN_LEVELS = {"INFO", "WARN", "ERROR", "DEBUG", "TRACE"}


def _normalize_level(level):
    up = str(level or "INFO").upper()
    up = _LEVEL_MAP.get(up, up)
    return up if up in _KNOWN_LEVELS else "INFO"


def _format_event_ts(ts):
    """Render a structured-log timestamp as 'YYYY-MM-DD HH:MM:SS' so the
    downstream parser (which keys on that prefix) can read it."""
    try:
        if isinstance(ts, str) and ts.isdigit():
            ts = int(ts)
        if isinstance(ts, (int, float)):
            secs = ts / 1000.0 if ts > 1e11 else float(ts)  # epoch ms vs s
            return datetime.fromtimestamp(secs, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(ts, str):
            m = re.match(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})", ts)
            if m:
                return f"{m.group(1)} {m.group(2)}"
    except (ValueError, OverflowError, OSError):
        pass
    return "1970-01-01 00:00:00"


def _events_to_lines(events):
    """Reconstruct '[ts] LEVEL message' lines from structured log events."""
    lines = []
    for ev in events:
        ts = _format_event_ts(ev.get("timestamp"))
        lvl = _normalize_level(ev.get("level"))
        lines.append(f"[{ts}] {lvl} {ev.get('message', '')}")
    return lines


def _attempt_log_lines(payload):
    """Extract log lines from an attempt/get_for_job (or legacy jobs/get
    attempt) payload, preferring structured events over formatted logLines."""
    logs = payload.get("logs") or {}
    if logs.get("events"):
        return _events_to_lines(logs["events"])
    if logs.get("logLines"):
        return list(logs["logLines"])
    return []


def _latest_attempt_number(job_entry):
    attempts = job_entry.get("attempts") or []
    nums = []
    for a in attempts:
        ar = a.get("attempt", a) if isinstance(a, dict) else {}
        if isinstance(ar.get("id"), int):
            nums.append(ar["id"])
    if nums:
        return max(nums)
    return max(len(attempts) - 1, 0)


def get_job_log_text(base_url, job_entry):
    """Fetch the latest attempt's logs for a job. Uses the structured-logging
    endpoint (attempt/get_for_job) and falls back to legacy inline jobs/get
    logs only if that endpoint is absent (older Airbyte)."""
    job = job_entry.get("job", job_entry)
    job_id = job.get("id")
    attempt_number = _latest_attempt_number(job_entry)
    try:
        resp = api_post(base_url, "/api/v1/attempt/get_for_job",
                        {"jobId": job_id, "attemptNumber": attempt_number})
        return "\n".join(_attempt_log_lines(resp))
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise  # transient/auth error — let the caller retry next poll
    # Legacy fallback: older Airbyte inlines logs in jobs/get attempts.
    detail = api_post(base_url, "/api/v1/jobs/get", {"id": job_id})
    lines = []
    for att in detail.get("attempts", []):
        logs = att.get("logs") or att.get("attempt", {}).get("logs") or {}
        lines.extend(_attempt_log_lines({"logs": logs}))
    return "\n".join(lines)


def load_state(path):
    try:
        with open(path, encoding="utf-8") as f:
            state = json.load(f)
            return set(state.get("processed_job_ids", []))
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        return set()


def save_state(path, processed_ids):
    ids = sorted(processed_ids)[-STATE_ID_CAP:]
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"processed_job_ids": ids}, f)


def safe_name(name):
    return re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-") or "connection"


_API_ERRORS = (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError)


def detect_log_shape(base_url, job_entry):
    """Probe one job's latest attempt to report WHERE log content lives,
    without storing it. Returns (info, top_level_keys); info reports logType
    and the event/logLine counts so an unfamiliar version can be diagnosed."""
    job = job_entry.get("job", job_entry)
    job_id = job.get("id")
    attempt_number = _latest_attempt_number(job_entry)
    resp = api_post(base_url, "/api/v1/attempt/get_for_job",
                    {"jobId": job_id, "attemptNumber": attempt_number})
    logs = resp.get("logs") or {}
    info = {
        "logType": resp.get("logType"),
        "events": len(logs.get("events") or []),
        "logLines": len(logs.get("logLines") or []),
        "other_log_keys": sorted(k for k in logs if k not in ("events", "logLines", "version")),
    }
    return info, list(resp.keys())


def dry_run_report(base_url, connections, processed, wanted, max_jobs, sample):
    """Hit the API read-only and report what WOULD be fetched plus the detected
    log shape. Writes no files and does not update state."""
    status_tally = {}
    would_fetch = []
    for conn in connections:
        conn_id = conn.get("connectionId")
        conn_name = safe_name(conn.get("name", conn_id or "connection"))
        if not conn_id:
            continue
        try:
            jobs = list_jobs(base_url, conn_id, max_jobs)
        except _API_ERRORS as e:
            print(f"warning: failed to list jobs for {conn_name}: {e}", file=sys.stderr)
            continue
        for entry in jobs:
            job = entry.get("job", entry)
            job_id = job.get("id")
            status = (job.get("status") or "").lower()
            if job_id is None:
                continue
            status_tally[status] = status_tally.get(status, 0) + 1
            if job_id in processed or status not in TERMINAL_STATUSES:
                continue
            if status in wanted:
                would_fetch.append((conn_name, entry))

    print("=== DRY RUN (no files written, state untouched) ===")
    print(f"API URL                : {base_url}")
    print(f"Discovery API          : {_API_MODE['discovery'] or 'config'}")
    print(f"Connections discovered : {len(connections)}")
    print(f"Recent jobs by status  : {status_tally}")
    print(f"New jobs to be fetched  : {len(would_fetch)}")

    print(f"\nProbing log shape on up to {sample} job(s):")
    probed = ok = errored = 0
    for conn_name, entry in would_fetch[:sample]:
        job_id = entry.get("job", entry).get("id")
        try:
            info, top_keys = detect_log_shape(base_url, entry)
        except _API_ERRORS as e:
            print(f"  job {job_id} ({conn_name}): ERROR {e}")
            errored += 1
            continue
        probed += 1
        if info["events"] > 0 or info["logLines"] > 0:
            ok += 1
        print(f"  job {job_id} ({conn_name}): logType={info['logType']} "
              f"events={info['events']} logLines={info['logLines']} "
              f"top_keys={top_keys}")
        if info["other_log_keys"]:
            print(f"    unrecognized log keys: {info['other_log_keys']}")

    print()
    if probed == 0 and errored and _API_MODE["discovery"] == "public":
        print("VERDICT: discovery works via the Public API, but the Config API "
              "log endpoint (attempt/get_for_job) is unreachable — log content "
              "cannot be retrieved. The Public API exposes no per-attempt log "
              "endpoint; make the Config API reachable for log fetching.")
    elif probed == 0 and errored:
        print("VERDICT: connectivity OK, but log retrieval failed on every probed "
              "job — see the ERROR lines above.")
    elif probed == 0:
        print("VERDICT: connectivity OK, but no fetchable failed jobs to probe — "
              "log shape unconfirmed.")
    elif ok == probed:
        print("VERDICT: log retrieval looks COMPATIBLE (logLines/events present).")
    else:
        print("VERDICT: log retrieval may be INCOMPATIBLE with this Airbyte version. "
              "Share the bracketed shape/keys above so the fetcher can be adapted.")


def main():
    ap = argparse.ArgumentParser(
        description="Fetch new Airbyte sync job logs for triage"
    )
    ap.add_argument("--api-url", default=os.environ.get("AIRBYTE_API_URL", DEFAULT_API_URL),
                    help="Airbyte API base URL (or set AIRBYTE_API_URL)")
    ap.add_argument("--output-dir", default="./airbyte-logs",
                    help="Directory to write fetched log files")
    ap.add_argument("--state", default=None,
                    help="State file tracking processed job ids "
                         "(default: <output-dir>/.fetch-state.json)")
    ap.add_argument("--all", action="store_true",
                    help="Fetch all terminal jobs, including succeeded/cancelled "
                         "(default: failed and incomplete only)")
    ap.add_argument("--max-jobs", type=int, default=20,
                    help="Max recent jobs to inspect per connection")
    ap.add_argument("--dry-run", action="store_true",
                    help="Probe the API read-only: report what would be fetched "
                         "and the detected log shape, without writing files or "
                         "updating state")
    ap.add_argument("--sample", type=int, default=5,
                    help="In --dry-run, how many jobs to probe for log shape")
    args = ap.parse_args()

    state_path = args.state or os.path.join(args.output_dir, ".fetch-state.json")
    processed = load_state(state_path)
    wanted = TERMINAL_STATUSES if args.all else DEFAULT_FETCH_STATUSES

    try:
        connections = list_connections(args.api_url)
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError) as e:
        print(f"Error reaching Airbyte API at {args.api_url}: {e}", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        dry_run_report(args.api_url, connections, processed, wanted,
                       args.max_jobs, args.sample)
        return

    os.makedirs(args.output_dir, exist_ok=True)
    fetched = 0
    for conn in connections:
        conn_id = conn.get("connectionId")
        conn_name = safe_name(conn.get("name", conn_id or "connection"))
        if not conn_id:
            continue
        try:
            jobs = list_jobs(args.api_url, conn_id, args.max_jobs)
        except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError) as e:
            print(f"warning: failed to list jobs for {conn_name}: {e}", file=sys.stderr)
            continue

        for entry in jobs:
            job = entry.get("job", entry)
            job_id = job.get("id")
            status = (job.get("status") or "").lower()
            if job_id is None or job_id in processed:
                continue
            if status not in TERMINAL_STATUSES:
                continue  # still running; leave for a later poll
            processed.add(job_id)
            if status not in wanted:
                continue
            try:
                text = get_job_log_text(args.api_url, entry)
            except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError) as e:
                print(f"warning: failed to fetch log for job {job_id} "
                      f"({conn_name}): {e}", file=sys.stderr)
                processed.discard(job_id)  # retry next poll
                continue
            if not text.strip():
                continue
            out_path = os.path.join(args.output_dir, f"{conn_name}__job{job_id}.log")
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(text + "\n")
            print(os.path.abspath(out_path))
            fetched += 1

    save_state(state_path, processed)
    print(f"Fetched {fetched} new log(s) across {len(connections)} connection(s)",
          file=sys.stderr)


if __name__ == "__main__":
    main()
