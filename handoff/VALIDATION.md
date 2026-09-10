# Package validation

Verified September 10, 2026 against the packaged files.

| Check | Result |
| --- | --- |
| `python3 tests/run_tests.py` | 35 tests passed on macOS with Python 3.9.6, using synthetic fixtures and local HTTP stubs. |
| `bash -n scripts/triage-cron.sh` | Passed. |
| Root `kubectl kustomize .` | Passed with kubectl v1.34.1 / Kustomize v5.7.1 and default loading restrictions. |
| Rendered resources | ServiceAccount, ConfigMap, PVC, and suspended CronJob; namespace and schedule checked. |
| ConfigMap contents | All seven runtime/knowledge-base files present with exact packaged contents. |
| Source provenance | Runtime scripts, bundled knowledge base, tests, fixtures, and license match revision `a2c7111`. Manifest adaptations are described in RELEASE_NOTES.md. |
| Local overlay example | Produced the documented pattern finding; example report generated successfully. |
| Documentation | Local Markdown links checked; all eight pages of the Word guide rendered and visually inspected. |

The test suite covers categorization, redaction, count checks, rate-limit totals,
Config and Public API discovery shapes, cron output, JSON events, optional Markdown,
and normal duplicate suppression. No production Airbyte endpoint was contacted.

Kustomize rendering verifies manifest construction; it does not validate admission
policies, storage provisioning, image availability, or execution in the target cluster.
The stock Python 3.12 container was not executed during these local package checks.
Follow DEPLOYMENT_GUIDE.md to verify connectivity, a manual run, stored reports,
Datadog collection, and a scheduled run in the customer's environment.

SHA256SUMS records every other delivered file. To check after extraction, run
`shasum -a 256 -c SHA256SUMS` on macOS or `sha256sum -c SHA256SUMS` on Linux.

## GitHub snapshot verification

The generic snapshot retains the tested runtime and fixtures byte for byte.
Its Kustomize build, embedded scripts, relative documentation links, and file
checksums are rechecked for publication. No cluster deployment is performed.
