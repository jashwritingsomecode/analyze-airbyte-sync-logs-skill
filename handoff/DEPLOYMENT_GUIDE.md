# Airbyte Sync Log Analyzer deployment guide

Run the commands below from the extracted package root, where `kustomization.yaml`
is located. Use the package contents as the starting point for your deployment
repository. The application runs in a stock `python:3.12-slim` container; its
scripts and knowledge base are mounted through a ConfigMap.

## 1 Confirm the environment and connectivity

The platform team needs `kubectl` access to the Airbyte namespace and permission
to create the workload, ConfigMap, PVC, service account, and any required Secret.
The default PVC requests 1 GiB using the default StorageClass; adjust it as needed.
Python 3 is needed on the workstation for the read-only probe and local tests.

The supplied manifests use illustrative values. Replace them and the commands
below with the namespace and service name used by your Airbyte installation:

- Namespace: `airbyte`
- Airbyte API: `http://airbyte-airbyte-server-svc:8001`

In one terminal, keep this port-forward running:

```bash
kubectl -n airbyte port-forward svc/airbyte-airbyte-server-svc 8001:8001
```

In a second terminal, from the package root:

```bash
AIRBYTE_API_URL=http://localhost:8001 python3 scripts/fetch-airbyte-logs.py --dry-run
```

The probe makes read-only calls and writes no logs or polling state. Check its
discovery results and final `VERDICT`. A compatible result requires successful
log retrieval from sampled jobs, not just a working connection-list endpoint.
If no suitable jobs are available, repeat when a completed job is available.

The fetcher supports optional HTTP Basic authentication through
`AIRBYTE_API_USER` and `AIRBYTE_API_PASSWORD`. If authentication is required,
supply the approved credentials through those environment variables. A 401/403
alone does not establish which authentication method the service expects.

Discovery tries the Config API and falls back to Public API discovery on 404.
Reading log content still requires the Config API log endpoint or its legacy
fallback. The probe checks that distinction. Stop the port-forward when finished.

## 2 Configure this package

| File or setting | Action |
| --- | --- |
| Root `kustomization.yaml` | Confirm the namespace. Keep the script files beneath this root. |
| `deploy/k8s/cronjob.yaml` | Confirm `AIRBYTE_API_URL`; leave `suspend: true` during manual verification. |
| `deploy/k8s/pvc.yaml` | Confirm StorageClass, capacity, and suitability for the cluster. |
| `schedule` | Default is every 30 minutes once unsuspended. |
| `TRIAGE_FETCH_ALL` | Default `0` selects failed/incomplete jobs. `1` includes all terminal jobs, including successes and cancellations. |
| `TRIAGE_STDOUT_REPORT` | Default `0` prints compact JSON events. `1` also prints Markdown reports when findings or sanity flags exist. |
| `SMTP_HOST` | Empty by default; optional email is disabled. Configure the `SMTP_*` fields only if email is wanted. |

The optional NetworkPolicy is not enabled. If used, tune its selectors and
destinations to the environment before adding it to the root resources list.

If Airbyte needs Basic auth, create the Secret with the required keys using your
normal credentials process. For example:

```bash
kubectl -n airbyte create secret generic sync-log-triage-secrets \
  --from-literal=AIRBYTE_API_USER='<user>' \
  --from-literal=AIRBYTE_API_PASSWORD='<password>'
```

For authenticated optional email, include `SMTP_USERNAME` and `SMTP_PASSWORD` in
the same Secret. `deploy/k8s/secret.example.yaml` documents the supported keys.
If neither service needs credentials, no Secret is required; references are optional.

## 3 Render and deploy with the schedule paused

```bash
kubectl kustomize .
kubectl apply -k .
kubectl -n airbyte get cronjob sync-log-triage
kubectl -n airbyte get pvc sync-log-triage-state
```

Confirm `SUSPEND` is true. The manifest creates a service account, PVC,
`analyzer-scripts` ConfigMap, and the paused CronJob. Some StorageClasses bind
the PVC only when the first pod starts.

## 4 Verify a manual run

```bash
kubectl -n airbyte create job --from=cronjob/sync-log-triage triage-manual-1
kubectl -n airbyte logs -f job/triage-manual-1
kubectl -n airbyte wait --for=condition=complete --timeout=660s job/triage-manual-1
```

Wait for the pod to start before reading logs. If the Job fails or times out,
inspect its pod logs and `kubectl describe job`; resolve the cause before proceeding.
Inspect diagnostics even if the Job completes: a completed container alone does
not prove that every selected sync log was retrieved and analyzed.

For each nonempty log actually analyzed, look for one JSON object with
`"event":"airbyte.sync.triage"`. Kubernetes also collects operational stderr
messages. With no eligible new logs, no JSON events is expected; use a known
completed failed/incomplete job to establish the full path, or enable all terminal
jobs to include a suitable successful job. The first poll can include recent history
(up to 20 jobs per connection); it is not an unlimited backfill.

Reports are written to `/data/triage-<timestamp>.md` and `.json`. Downloaded raw
logs are under `/data/logs`, and polling state is `/data/state.json`.
To inspect files after the analyzer exits, use your platform's volume access process
with a temporary pod mounting `sync-log-triage-state`. ReadWriteOnce storage may
require coordinating mounts. `kubectl cp` requires a running pod; it cannot copy
from the completed analyzer container. Confirm that both report formats exist
and correspond to the analyzed jobs.

After the first Job completes, run a second manual Job with a different name:

```bash
kubectl -n airbyte create job --from=cronjob/sync-log-triage triage-manual-2
kubectl -n airbyte logs -f job/triage-manual-2
kubectl -n airbyte wait --for=condition=complete --timeout=660s job/triage-manual-2
```

Previously processed jobs should not emit again during normal polling. If new
eligible Airbyte jobs finished between polls, events for those jobs are expected.
Run manual jobs sequentially: CronJob concurrency controls do not prevent two
independently created manual jobs from overlapping on the shared state volume.

## 5 Verify Datadog collection and configure monitors

Search for `kube_namespace:airbyte`, then identify the analyzer pod's own
events and verify JSON field parsing. Existing Airbyte logs alone do not confirm
collection of this new workload. The analyzer makes no direct Datadog calls;
the existing cluster collector must forward the pod logs.

Use `@event:airbyte.sync.triage` as the starting event query once the field is parsed.
Configure monitors for the chosen failed statuses, finding severities, and sanity
flags. Read **docs/EVENT_REFERENCE.md** for field meanings and monitoring limits.
Verify a known event reaches the intended query and, where configured, alert route.

## 6 Enable the schedule and hand over operation

Change `suspend: true` to `suspend: false` in `deploy/k8s/cronjob.yaml`, then:

```bash
kubectl apply -k .
kubectl -n airbyte get cronjob sync-log-triage
```

Confirm `SUSPEND` is false and inspect a subsequent scheduled Job. Persist this
setting in the deployment repository so later applies keep the intended schedule.
To pause future runs, set it back to true and apply; existing Jobs continue.

The platform team owns the schedule, workload, access, and storage. Reports and
raw logs accumulate; define retention and capacity management for the volume.
Job-history limits do not delete files on the PVC. Report/event masking recognizes
known secret formats; raw downloaded logs retain their original contents.

Assign a knowledge-base owner to review unknown failures, maintain local patterns,
and verify remediation guidance. Follow **docs/KNOWLEDGE_BASE.md**. For script or
rule updates, apply from the package root when no analyzer Job is running; the
next Job mounts the refreshed ConfigMap. No custom image build is required.

## Deployment completion checks

- The read-only probe retrieves logs from this Airbyte environment.
- A manual run emits the expected event and writes both report formats.
- A second sequential poll avoids duplicate processing of the same jobs.
- The analyzer's own event reaches Datadog and its fields are queryable.
- The intended monitor and alert route have been exercised where configured.
- The schedule is enabled and a scheduled run has been observed.
- Workload, monitoring, and local knowledge-base owners are assigned.

This confirms deployment of post-sync log analysis. Individual-record validation,
automatic repair, and blocking downstream consumption remain outside its scope.
