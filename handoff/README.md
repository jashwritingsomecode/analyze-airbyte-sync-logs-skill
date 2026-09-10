# Airbyte Sync Log Analyzer customer handoff

This package contains the implemented analyzer and the material needed to deploy
and operate it. It reads Airbyte sync logs and produces categorized findings,
remediation guidance, compact JSON events, and detailed reports. Automated
analysis uses deterministic Python rules and makes read-only Airbyte API calls.

Prepared September 10, 2026. Analyzer source revision: `a2c7111`.

This is a self-contained release snapshot for browsing and deployment. From
a checkout of this branch, run `cd handoff` before following its commands.
Namespace and service names are generic examples; configure your environment
before deployment. The original assistant-skill files remain at the repository root.

An [editable Word copy of the solution guide](docs/Airbyte_Sync_Log_Analyzer_Explained.docx)
is included alongside the Markdown version.

## Start here

1. Read **[the solution guide](docs/SOLUTION_GUIDE.md)** for the purpose, value,
   end-to-end workflow, examples, limitations, and customer ownership.
2. Have the platform team follow **[DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)** from this package root.
   The schedule is initially paused so the team can verify a manual run first.
3. Have the observability team verify collection of the analyzer's JSON events
   and configure searches and monitors using **[the JSON event reference](docs/EVENT_REFERENCE.md)**.
4. Assign an owner for local failure patterns and remediation guidance;
   **[the knowledge-base guide](docs/KNOWLEDGE_BASE.md)** includes a worked customization example.

Use the source included here for this handoff. No AI assistant installation,
custom image build, or Python package installation is needed.

## Package contents

| Path | Purpose |
| --- | --- |
| `scripts/` | Analyzer, fetcher, event formatter, optional mailer, entrypoint, and initial knowledge base. |
| `kustomization.yaml` | Deployment entry point at the package root. |
| `deploy/k8s/` | CronJob, storage, service account, and optional configuration templates. |
| `DEPLOYMENT_GUIDE.md` | Connectivity, installation, manual verification, scheduling, and operating steps. |
| `docs/` | Solution guide, JSON reference, and knowledge-base maintenance guide. |
| `examples/` | Synthetic local-rule example and matching log. |
| `tests/` | The supplied test suite and synthetic fixtures, including intentionally fake credentials for masking tests. |
| `RELEASE_NOTES.md` | Source version and handoff packaging changes. |
| `VALIDATION.md` | Checks performed on this package and environment checks for deployment. |
| `SOURCE_MANIFEST.json`, `SHA256SUMS` | Source provenance and package file checksums. |
| `LICENSE` | Apache 2.0 license. |

## Scope and operation

By default, the analyzer polls every 30 minutes for failed/incomplete jobs.
Successful jobs are optional. Each analyzed sync emits one JSON event, including
syncs with zero findings; a poll with no new logs emits no events. Reports stay
on persistent storage. Email is optional and disabled by default.

The customer deploys and operates the workload, configures Datadog collection
and monitors, and maintains the local pattern knowledge base. Faros supplies
the initial code and guidance. Unknown failures remain visible for investigation.

This is post-sync log analysis. It does not automatically repair failures,
trigger syncs, validate individual source records, or block downstream consumption.
Deployment and live Datadog collection are confirmed in the customer's environment.

## Run the supplied tests

From this directory, with Python 3 and Bash available:

```bash
python3 tests/run_tests.py
```

The tests use synthetic logs and local HTTP stub servers. They require permission
to bind localhost ports and do not contact a production Airbyte instance.
