#!/usr/bin/env python3
"""Re-emit categorized results as compact, redacted per-sync stdout events."""

import argparse
import json
import os
import re
import sys

from categorize import redact_obj, redact_text


SUMMARY_LIMIT = 240
JOB_ID_RE = re.compile(r"__job([0-9]+)\.log$")


def brief_finding(finding):
    # Unknown findings need actual context; their KB summary is generic.
    text = (finding["message"] if finding["category"] == "uncategorized"
            else finding["summary"])
    # Mask before shortening, so a token cannot be cut into an unrecognized form.
    text = " ".join(redact_text(text).split())
    truncated = len(text) > SUMMARY_LIMIT
    return {
        "level": finding["level"],
        "category": finding["category"],
        "severity": finding["severity"],
        "pattern_id": finding["pattern_id"],
        "summary": text[:SUMMARY_LIMIT - 3] + "..." if truncated else text,
        "summary_truncated": truncated,
    }


def sync_events(output):
    logs = {entry["file"]: entry for entry in output["logs"]}
    for priority in output["triage"]["prioritized"]:
        entry = logs[priority["file"]]
        job = JOB_ID_RE.search(os.path.basename(entry["file"]))
        findings = [brief_finding(f) for f in entry["findings"]]
        top = None
        if priority["top_finding"] is not None:
            top = {key: findings[0][key]
                   for key in ("category", "pattern_id", "summary")}
        yield redact_obj({
            "event": "airbyte.sync.triage",
            "connector": priority["connector"],
            "job_id": int(job.group(1)) if job else None,
            "status": priority["status"] or "unknown",
            # Preserve the analyzer's semantics: sanity flags are separate.
            "worst_severity": priority["worst_severity"],
            "finding_count": priority["finding_count"],
            "top_finding": top,
            "findings": findings,
            "sanity_flags": priority["sanity_flag_count"],
            "sanity_details": entry["record_count_sanity"]["flags"],
            "suppressed_noise_count": entry["suppressed"]["count"],
            "rate_limit_wait_s": (entry.get("rate_limit") or {}).get("total_wait_seconds", 0),
        })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("triage_json", help="JSON artifact produced by categorize.py")
    args = parser.parse_args()
    with open(args.triage_json, encoding="utf-8") as source:
        output = json.load(source)
    for event in sync_events(output):
        sys.stdout.write(json.dumps(event, separators=(",", ":"), allow_nan=False) + "\n")


if __name__ == "__main__":
    main()
