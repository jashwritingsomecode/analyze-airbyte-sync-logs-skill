# Handoff release notes

Package prepared September 10, 2026, from source revision
`a2c711127dd40a6546c1726f5ea103183b505122`.

The seven runtime and knowledge-base files in `scripts/`, the supplied test suite,
synthetic fixtures, and license are unchanged from that revision.

The release emits one compact redacted JSON event per analyzed sync, with Markdown
and enriched JSON reports retained on storage. Full Markdown stdout is optional
and disabled by default. The failed/incomplete fetch default is unchanged.

## Packaging changes

- Deployment uses a root `kustomization.yaml` with all scripts beneath its root,
  so standard Kustomize file-loading restrictions are respected. Run deployment
  commands from this root, not from `deploy/k8s`.
- The packaged CronJob adds `suspend: true` so manual verification precedes
  scheduling. Enable the schedule using the deployment guide.
- The root README and deployment instructions are curated for this delivery;
  the solution guide includes customer knowledge-base ownership. Event and local
  customization references and a synthetic overlay example are included.
- Internal onboarding, ticket discussion, historical plans, assistant-skill setup,
  real logs, generated customer reports, and authoring/QA files are excluded.

## Current operating limits

- The parser can miss fatal messages labeled INFO. Status and connector labels
  may be unknown or use filename fallbacks when metadata is missing.
- Fetch state is recorded before analysis/output completes. An interruption can
  leave an event missing; empty logs are skipped. Polling is bounded recent-job
  discovery, not a complete historical event stream.
- Recovery, success-rate, and consecutive-failure monitors need additional
  validation of coverage, stable identity, and ordering. Successful jobs require
  `TRIAGE_FETCH_ALL=1`.
- Report/event masking covers recognized credential formats; raw downloaded logs
  retain original contents. Files accumulate on the PVC without automatic retention.
- The analyzer does not deploy Datadog dashboards or monitors. It performs no
  automatic repairs, individual-record validation, or downstream blocking.

## GitHub publication

This self-contained `handoff/` snapshot uses generic namespace and service
examples. It adds a Markdown solution guide with diagrams for GitHub reading.
Runtime scripts, the bundled knowledge base, and tests match the source
revision above; the customer-specific distribution remains separate.
