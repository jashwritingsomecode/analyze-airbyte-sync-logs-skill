#!/usr/bin/env python3
"""Fetch new terminal Airbyte sync job logs via the Airbyte Config API.

Polls the API for sync jobs that finished since the last run (tracked in a
state file), writes each job's log text to a file, and prints the file paths
to stdout — ready to pipe into categorize.py. Designed to run from cron;
between runs nothing is resident.

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


def api_post(base_url, path, payload, timeout=30):
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    auth = _auth_header()
    if auth:
        headers["Authorization"] = auth
    req = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def list_connections(base_url):
    connections = []
    workspaces = api_post(base_url, "/api/v1/workspaces/list", {}).get("workspaces", [])
    for ws in workspaces:
        ws_id = ws.get("workspaceId")
        if not ws_id:
            continue
        resp = api_post(base_url, "/api/v1/connections/list", {"workspaceId": ws_id})
        connections.extend(resp.get("connections", []))
    return connections


def list_jobs(base_url, connection_id, page_size):
    resp = api_post(base_url, "/api/v1/jobs/list", {
        "configTypes": ["sync"],
        "configId": connection_id,
        "pagination": {"pageSize": page_size, "rowOffset": 0},
    })
    return resp.get("jobs", [])


def get_job_log_text(base_url, job_id):
    """Fetch a job's log lines, tolerating both Airbyte log payload shapes."""
    detail = api_post(base_url, "/api/v1/jobs/get", {"id": job_id})
    lines = []
    for entry in detail.get("attempts", []):
        logs = entry.get("logs") or entry.get("attempt", {}).get("logs") or {}
        if logs.get("logLines"):
            lines.extend(logs["logLines"])
        elif logs.get("events"):
            for ev in logs["events"]:
                lines.append(str(ev.get("message", "")))
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


def detect_log_shape(base_url, job_id):
    """Probe one job's logs to report WHERE log content lives, without storing
    it. Returns (per_attempt_shapes, top_level_jobs_get_keys). Each shape is
    ('logLines'|'events', count) when recognized, else a keys hint so an
    unfamiliar Airbyte version can be diagnosed."""
    detail = api_post(base_url, "/api/v1/jobs/get", {"id": job_id})
    shapes = []
    for entry in detail.get("attempts", []):
        logs = entry.get("logs") or entry.get("attempt", {}).get("logs") or {}
        if logs.get("logLines"):
            shapes.append(("logLines", len(logs["logLines"])))
        elif logs.get("events"):
            shapes.append(("events", len(logs["events"])))
        elif logs:
            shapes.append(("logs-keys:" + "|".join(sorted(map(str, logs.keys()))), 0))
        else:
            shapes.append(("attempt-keys:" + "|".join(sorted(map(str, entry.keys()))), 0))
    return shapes, list(detail.keys())


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
                would_fetch.append((conn_name, job_id))

    print("=== DRY RUN (no files written, state untouched) ===")
    print(f"API URL                : {base_url}")
    print(f"Connections discovered : {len(connections)}")
    print(f"Recent jobs by status  : {status_tally}")
    print(f"New jobs to be fetched  : {len(would_fetch)}")

    print(f"\nProbing log shape on up to {sample} job(s):")
    probed = ok = 0
    for conn_name, job_id in would_fetch[:sample]:
        try:
            shapes, top_keys = detect_log_shape(base_url, job_id)
        except _API_ERRORS as e:
            print(f"  job {job_id} ({conn_name}): ERROR {e}")
            continue
        probed += 1
        if any(s[0] in ("logLines", "events") and s[1] > 0 for s in shapes):
            ok += 1
        print(f"  job {job_id} ({conn_name}): attempts={shapes or 'none'}")
        if not shapes:
            print(f"    jobs/get top-level keys: {top_keys}")

    print()
    if probed == 0:
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
                text = get_job_log_text(args.api_url, job_id)
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
