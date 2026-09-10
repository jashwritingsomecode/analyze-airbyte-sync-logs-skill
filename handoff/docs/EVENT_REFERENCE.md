# JSON event reference


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
`kube_namespace:airbyte`, then verify the analyzer pod's own events and
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
