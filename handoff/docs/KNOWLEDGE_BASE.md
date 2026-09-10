# Maintaining the local knowledge base

Faros supplies an initial set of failure patterns and remediation guidance.
The customer owns extending and maintaining local guidance as new failures arise
and operating procedures change. The analyzer does not automatically learn from logs.

## Recommended maintenance workflow

1. Review uncategorized findings and supporting log evidence.
2. Investigate the cause and verify useful remediation steps.
3. Add a narrowly targeted rule with a clear explanation and severity.
4. Check matching and unrelated synthetic or sanitized messages for false positives.
5. Deploy the reviewed rule and inspect subsequent reports for accuracy.

Keep raw customer logs out of shared source and test fixtures. The included test
logs are synthetic, including the fake credentials used to test masking.

## Rule structure and ordering

Local rules belong in `scripts/error_patterns.local.json`, using the same
`{"patterns": [...]}` structure as the bundled `scripts/error_patterns.json`.
Each rule needs a unique `id`, a category and severity from the bundled taxonomy,
`provenance` (`generic` or `faros-doc`), regex strings in `match.any`, a plain-language
`summary`, and a list of `remediation` steps. Use `generic` for local operational
guidance. Add `source_doc` only for a verified relevant URL; otherwise use null.

The overlay is loaded before the bundled rules. The first matching rule wins;
put specific expressions before broad ones. Matching is case-insensitive. The
`connectors` field is descriptive metadata in this implementation, not an enforced
filter; make message expressions specific enough for their intended use.
Invalid regexes are skipped with a warning, so inspect stderr as well as the report.

## Try the included synthetic example

From the package root:

```bash
python3 scripts/categorize.py examples/local-maintenance.log \
  --overlay examples/error_patterns.local.example.json \
  --report /tmp/airbyte-local-example.md
```

The output should include `local-upstream-maintenance-example` with category
`api_error` and severity `medium`. The message is deliberately artificial and
is only a demonstration; replace it with investigated, tested signatures for
your environment before creating the production overlay.

To deploy real local rules, create `scripts/error_patterns.local.json` and
uncomment its line in the **root** `kustomization.yaml`. Then run `kubectl apply -k .`
from the package root when no analyzer Job is running. Future Jobs load the updated
ConfigMap. The existing tests validate the bundled knowledge base; use explicit
overlay examples and checks to validate your local additions.

## Limits of a pattern update

Unknown error messages remain visible as `uncategorized`. Known platform noise is
counted separately. A pattern can classify only messages the parser extracts:
fatal messages labeled INFO can be missed and need a parser change, not just a
new rule. Review guidance as connector versions and environment details change;
a pattern match is an investigation starting point, not proof of root cause.
