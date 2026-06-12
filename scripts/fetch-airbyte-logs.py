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
    args = ap.parse_args()

    state_path = args.state or os.path.join(args.output_dir, ".fetch-state.json")
    os.makedirs(args.output_dir, exist_ok=True)
    processed = load_state(state_path)
    wanted = TERMINAL_STATUSES if args.all else DEFAULT_FETCH_STATUSES

    try:
        connections = list_connections(args.api_url)
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError) as e:
        print(f"Error reaching Airbyte API at {args.api_url}: {e}", file=sys.stderr)
        sys.exit(1)

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
