# Kubernetes deployment — automated sync-log triage

Runs the analyzer as a `CronJob` inside the cluster where Airbyte is deployed.
No custom image, no registry, and no outbound calls except to the in-cluster
Airbyte API and (optionally) your SMTP relay. The scripts are stdlib-only and
are delivered as a ConfigMap generated from this repo.

```
CronJob (every 30 min)
  └─ pod: python:3.12-slim
       ├─ scripts mounted read-only from the analyzer-scripts ConfigMap
       ├─ state + reports on a PersistentVolumeClaim (survives between runs)
       └─ triage-cron.sh:
            fetch-airbyte-logs.py  → Airbyte API (in-cluster service)
            categorize.py --report → triage.md + JSON
            emit-sync-events.py    → one compact JSON line per analyzed sync
            notify.py              → email/Teams, only when findings exist
```

## Prerequisites

- `kubectl` with access to the cluster, and `kustomize` (or `kubectl apply -k`, which has it built in).
- The namespace Airbyte runs in, and the in-cluster URL of the Airbyte server API.

## Step 1 — Discover the environment-specific values

These manifests ship with sensible defaults that you must confirm against the
actual cluster.

```bash
# Which namespace is Airbyte in?
kubectl get pods -A | grep -i airbyte

# The server API service + port (the default assumes airbyte-airbyte-server-svc:8001)
kubectl -n hnc-airbyte-fds get svc | grep -i server

# Confirm the API answers (and whether it needs auth) from inside the cluster:
kubectl -n hnc-airbyte-fds run triage-probe --rm -it --restart=Never \
  --image=curlimages/curl -- \
  sh -c "curl -s -X POST \
    http://airbyte-airbyte-server-svc:8001/api/v1/workspaces/list \
    -H 'Content-Type: application/json' -d '{}'"
```

A JSON response listing workspaces means the URL is right and no auth is
needed. A 401/403 means basic auth is required — capture those credentials for
Step 3. Update `AIRBYTE_API_URL` in `cronjob.yaml` and `namespace` in
`kustomization.yaml` to match what you find.

### Validate end-to-end without writing anything (`--dry-run`)

Before scheduling, confirm the fetcher can reach the API *and* read job logs on
this Airbyte version. From a machine that can reach the API (e.g. via
`kubectl port-forward svc/airbyte-airbyte-server-svc 8001:8001`, then point at
`http://localhost:8001`):

```bash
AIRBYTE_API_URL=http://localhost:8001 \
  python3 scripts/fetch-airbyte-logs.py --dry-run
```

It reports the connections found, recent jobs by status, how many would be
fetched, and — critically — the detected **log shape** per sampled job. A
`VERDICT: ... COMPATIBLE` line means log retrieval works on this version. An
`INCOMPATIBLE` verdict prints the actual response keys so the log-reading code
can be adapted. It writes no files and does not touch the state file.

## Step 2 — Configure output and optional notification

Every analyzed sync produces one compact JSON object on stdout with
`event: airbyte.sync.triage`, status, and brief finding descriptions. A failed
sync with no findings still emits an event. The default fetch selection stays
**failed/incomplete**; set `TRIAGE_FETCH_ALL=1` to include all terminal jobs,
including successes and cancellations. With no new logs to analyze, stdout is
empty. Operational diagnostics use stderr, which Kubernetes also collects.

Full Markdown and enriched JSON reports are retained on the PVC as
`triage-<timestamp>.md` and `.json`; retrieve them with `kubectl cp` from a pod
mounting the volume. Full Markdown is **off on stdout by default** to limit
log-ingestion volume. Set `TRIAGE_STDOUT_REPORT=1` to restore marked reports
when findings or sanity flags exist, in addition to the JSON events:
`=== TRIAGE-REPORT-BEGIN ... ===` / `=== TRIAGE-REPORT-END ... ===`.

Email is optional on top: edit the `SMTP_*` env values in `cronjob.yaml`.
Leave `SMTP_HOST` empty to skip email entirely. `SMTP_TO` can be a
distribution list or a Teams channel email address.

### JSON event fields and Datadog

Example event (one physical line):

```json
{"event":"airbyte.sync.triage","connector":"example-source","job_id":42,"status":"failed","worst_severity":"high","finding_count":1,"top_finding":{"category":"permission","pattern_id":"auth-failure","summary":"Authentication failed."},"findings":[{"level":"error","category":"permission","severity":"high","pattern_id":"auth-failure","summary":"Authentication failed.","summary_truncated":false}],"sanity_flags":0,"sanity_details":[],"suppressed_noise_count":0,"rate_limit_wait_s":0}
```

| Field | Meaning |
| --- | --- |
| `event` | Always `airbyte.sync.triage`; use this to select analyzer events. |
| `connector`, `job_id` | Existing analyzer connector label; job ID extracted from the fetcher's `__job<ID>.log` filename. Other filenames yield `null`. |
| `status` | Status parsed from the log, including `completed` or `failed`; `unknown` when absent. No success is inferred from zero findings. |
| `worst_severity`, `top_finding` | Most severe error/warning finding, or `null` when none. Existing analyzer semantics exclude sanity flags here. |
| `finding_count`, `findings` | All actionable error/warning findings, each with level, category, severity, pattern ID, and brief summary. Repeated findings are retained. |
| `sanity_flags`, `sanity_details` | Count and existing check/severity/detail objects for record-count anomalies. Check these separately from `worst_severity`. |
| `suppressed_noise_count` | Non-actionable noise counted separately from findings. |
| `rate_limit_wait_s` | Detected rate-limit wait in seconds; zero when none was detected. |

Each finding summary is redacted before whitespace normalization and shortening
to 240 characters; `summary_truncated` indicates shortening. Unknown findings
use a message excerpt. Full redacted messages remain in the enriched JSON on
the PVC; Markdown includes remediation and sampled message excerpts. No findings
are removed to meet a per-event size cap; event size
therefore grows with finding count. The formatter adds no analysis or API calls.

Before relying on Datadog, verify collection for the namespace with
`kube_namespace:hnc-airbyte-fds`, then verify the analyzer pod's own events and
JSON field parsing. Existing Airbyte logs alone do not establish that a new
pod is included by all collection rules. The analyzer sends no requests to
Datadog; an existing cluster agent must collect its logs.

Once collection and field parsing are confirmed, a starting query is
`@event:airbyte.sync.triage`. Build status/severity counts, connector rankings,
and rate-limit wait summaries from those events. Category/pattern breakdowns
need validation of how the chosen Datadog pipeline handles the `findings`
array; one event can contain several findings. Alerting on sanity anomalies
must use `sanity_flags`/`sanity_details`, not only status or worst severity.
Unknown status needs investigation and is not a healthy signal.

Recovery, success-rate, or consecutive-failure monitors need successful syncs
(`TRIAGE_FETCH_ALL=1`), a stable connection identity, and verified event ordering
and completeness. The current parser can fall back to a filename for connector
identity; these advanced monitors are examples to configure and validate, not
built-in guarantees. Fetch state is recorded before analysis/output, so an
interrupted run may leave an event missing. Empty sync logs are not analyzed.

## Step 3 — Create the credentials Secret (only if needed)

Never commit credentials. Create the Secret imperatively, including only the
keys you actually use:

```bash
kubectl -n hnc-airbyte-fds create secret generic sync-log-triage-secrets \
  --from-literal=AIRBYTE_API_USER='<user>' \
  --from-literal=AIRBYTE_API_PASSWORD='<password>' \
  --from-literal=SMTP_USERNAME='<smtp-user>' \
  --from-literal=SMTP_PASSWORD='<smtp-password>'
```

See `secret.example.yaml` for the expected keys. If Airbyte's API needs no auth
and you use no SMTP auth, you can skip this step — all secret-sourced env vars
are marked `optional`.

## Step 4 — Deploy

```bash
# From this directory (deploy/k8s):
kubectl apply -k .
```

This creates the ServiceAccount, PVC, the `analyzer-scripts` ConfigMap (built
from ../../scripts), and the CronJob.

## Step 5 — Verify

```bash
# Trigger a one-off run without waiting for the schedule:
kubectl -n hnc-airbyte-fds create job --from=cronjob/sync-log-triage triage-manual-1

# Watch it and read logs:
kubectl -n hnc-airbyte-fds get jobs -w
kubectl -n hnc-airbyte-fds logs job/triage-manual-1

# Inspect the generated report on the PVC via a throwaway pod, or:
kubectl -n hnc-airbyte-fds logs job/triage-manual-1   # wrapper prints where the report landed
```

The first run fetches recent failed/incomplete jobs and records their IDs in
the state file; subsequent runs only process newly finished jobs.

## Updating the scripts or knowledge base

Edit any script (or add `../../scripts/error_patterns.local.json` and reference
it in `kustomization.yaml`), then re-run `kubectl apply -k .`. The ConfigMap is
regenerated and the next scheduled run picks it up.

## Security notes (SOC 2)

- **No hardcoded secrets** — credentials come from a Secret via env vars; the
  manifests contain only placeholders.
- **Least privilege** — dedicated ServiceAccount with no RBAC and token
  automount disabled; the pod runs non-root, read-only root filesystem, all
  Linux capabilities dropped, `RuntimeDefault` seccomp.
- **Bounded audit trail** — `successfulJobsHistoryLimit`/`failedJobsHistoryLimit`
  retain recent run history for inspection without unbounded growth.
- **Output redaction** — every report passes through the analyzer's secret
  redaction before being written or emailed.
- **Egress** — an optional `networkpolicy.yaml` restricts the pod's egress to
  DNS, the Airbyte API, and SMTP; tune the selectors before enabling it.
