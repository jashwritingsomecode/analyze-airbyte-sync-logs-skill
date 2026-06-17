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
kubectl -n airbyte get svc | grep -i server

# Confirm the API answers (and whether it needs auth) from inside the cluster:
kubectl -n airbyte run triage-probe --rm -it --restart=Never \
  --image=curlimages/curl -- \
  sh -c "curl -s -X POST \
    http://airbyte-airbyte-server-svc:8001/api/v1/workspaces/list \
    -H 'Content-Type: application/json' -d '{}'"
```

A JSON response listing workspaces means the URL is right and no auth is
needed. A 401/403 means basic auth is required — capture those credentials for
Step 3. Update `AIRBYTE_API_URL` in `cronjob.yaml` and `namespace` in
`kustomization.yaml` to match what you find.

## Step 2 — (Optional) configure notification

Edit the `SMTP_*` env values in `cronjob.yaml`. Leave `SMTP_HOST` empty to skip
alerting entirely — reports still accumulate on the PVC and can be read with
`kubectl cp`. `SMTP_TO` can be a distribution list or a Teams channel email
address.

## Step 3 — Create the credentials Secret (only if needed)

Never commit credentials. Create the Secret imperatively, including only the
keys you actually use:

```bash
kubectl -n airbyte create secret generic sync-log-triage-secrets \
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
kubectl -n airbyte create job --from=cronjob/sync-log-triage triage-manual-1

# Watch it and read logs:
kubectl -n airbyte get jobs -w
kubectl -n airbyte logs job/triage-manual-1

# Inspect the generated report on the PVC via a throwaway pod, or:
kubectl -n airbyte logs job/triage-manual-1   # wrapper prints where the report landed
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
