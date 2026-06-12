#!/usr/bin/env python3
"""Categorize Airbyte sync log errors against a documented knowledge base.

Consumes raw Airbyte sync logs (parsed via analyze-sync-logs.py) or
pre-parsed JSON, matches errors/warnings against error_patterns.json,
and emits enriched JSON with categories, severities, and remediation
steps, plus a cross-connector triage summary. Optionally renders a
standalone Markdown report.

Zero dependencies beyond the Python standard library.
"""

import argparse
import importlib.util
import json
import os
import re
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_KB = os.path.join(SCRIPT_DIR, "error_patterns.json")
DEFAULT_OVERLAY = os.path.join(SCRIPT_DIR, "error_patterns.local.json")

SEVERITY_RANK = {"critical": 0, "high": 1, "medium": 2, "low": 3}

# ---------------------------------------------------------------------------
# Secret redaction
# ---------------------------------------------------------------------------

REDACTED = "***REDACTED***"

_SECRET_KEY_NAMES = (
    r"token|secret|api[_-]?key|apikey|password|passwd|pwd|private[_-]?key|"
    r"access[_-]?token|refresh[_-]?token|authorization|client[_-]?secret|credentials?"
)

_SECRET_KV_RE = re.compile(
    r"(?i)([\"']?\b(?:%s)\b[\"']?\s*[:=]\s*)([\"']?)([^\s\"',}{\]]+)(\2)" % _SECRET_KEY_NAMES
)

_BEARER_RE = re.compile(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._+/=-]{8,}")

_TOKEN_FORMAT_RE = re.compile(
    r"\b("
    r"gh[pousr]_[A-Za-z0-9]{20,}"            # GitHub tokens
    r"|github_pat_[A-Za-z0-9_]{20,}"
    r"|glpat-[A-Za-z0-9_-]{15,}"             # GitLab PAT
    r"|xox[baprs]-[A-Za-z0-9-]{10,}"         # Slack
    r"|AKIA[0-9A-Z]{16}"                     # AWS access key id
    r"|sk-[A-Za-z0-9_-]{20,}"                # generic sk- API keys
    r"|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}"  # JWT
    r")\b"
)

_SECRET_KEY_FIELD_RE = re.compile(r"(?i)^(?:%s)$" % _SECRET_KEY_NAMES)


def redact_text(text):
    if not isinstance(text, str):
        return text
    text = _SECRET_KV_RE.sub(lambda m: m.group(1) + m.group(2) + REDACTED + m.group(4), text)
    text = _BEARER_RE.sub(lambda m: m.group(1) + " " + REDACTED, text)
    text = _TOKEN_FORMAT_RE.sub(REDACTED, text)
    return text


def redact_obj(obj):
    """Recursively redact strings; fully mask values of secret-named keys."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if _SECRET_KEY_FIELD_RE.match(str(k)) and isinstance(v, (str, int, float)):
                out[k] = REDACTED
            else:
                out[k] = redact_obj(v)
        return out
    if isinstance(obj, list):
        return [redact_obj(v) for v in obj]
    if isinstance(obj, str):
        return redact_text(obj)
    return obj


# ---------------------------------------------------------------------------
# Knowledge base
# ---------------------------------------------------------------------------

def load_patterns(kb_path, overlay_path=None):
    """Load KB entries, compiling match regexes. Overlay entries take priority."""
    patterns = []
    paths = []
    if overlay_path and os.path.isfile(overlay_path):
        paths.append(overlay_path)
    paths.append(kb_path)

    for path in paths:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for entry in data.get("patterns", []):
            compiled = []
            for rx in entry.get("match", {}).get("any", []):
                try:
                    compiled.append(re.compile(rx, re.IGNORECASE))
                except re.error as e:
                    print(
                        f"warning: skipping invalid regex {rx!r} in pattern "
                        f"{entry.get('id')!r} ({path}): {e}",
                        file=sys.stderr,
                    )
            if compiled:
                patterns.append({"entry": entry, "regexes": compiled})
    return patterns


def match_message(message, patterns):
    for p in patterns:
        for rx in p["regexes"]:
            if rx.search(message):
                return p["entry"], rx.pattern
    return None, None


def build_finding(message, level, timestamp, patterns):
    entry, matched = match_message(message, patterns)
    if entry:
        return {
            "timestamp": timestamp,
            "level": level,
            "message": message,
            "category": entry["category"],
            "severity": entry["severity"],
            "pattern_id": entry["id"],
            "summary": entry["summary"],
            "remediation": entry["remediation"],
            "source_doc": entry.get("source_doc"),
            "match_confidence": "high" if entry.get("provenance") == "faros-doc" else "medium",
            "matched_pattern": matched,
        }
    return {
        "timestamp": timestamp,
        "level": level,
        "message": message,
        "category": "uncategorized",
        "severity": "high" if level == "error" else "low",
        "pattern_id": None,
        "summary": "No documented pattern matched; shown verbatim for manual review.",
        "remediation": [
            "Review the full message and surrounding log lines.",
            "If this is a recurring failure mode, add a pattern to error_patterns.local.json."
        ],
        "source_doc": None,
        "match_confidence": None,
        "matched_pattern": None,
    }


# ---------------------------------------------------------------------------
# Rate-limit wait aggregation
# ---------------------------------------------------------------------------

_RL_CONTEXT_RE = re.compile(
    r"(?i)rate.?limit|throttl|quota|too many requests|\b429\b|abuse detection|secondary limit"
)

_WAIT_RE = re.compile(
    r"(?i)(?:wait(?:ing)?(?:\s+for)?|retry(?:ing)?\s+(?:in|after)|"
    r"sleep(?:ing)?(?:\s+for)?|back(?:ing)?\s?off(?:\s+for)?|retry-after\s*[:=]?)"
    r"\s*:?\s*(\d+(?:\.\d+)?)\s*(ms|milliseconds?|s|secs?|seconds?|m|mins?|minutes?|h|hours?)?\b"
)


def _to_seconds(value, unit):
    v = float(value)
    if unit:
        u = unit.lower()
        if u.startswith("ms") or u.startswith("milli"):
            return v / 1000.0
        if u.startswith("m"):
            return v * 60.0
        if u.startswith("h"):
            return v * 3600.0
        return v
    # No unit: assume seconds for plausible values, otherwise milliseconds.
    return v if v <= 3600 else v / 1000.0


def aggregate_rate_limit_waits(log_path):
    events = 0
    total_wait = 0.0
    samples = []
    with open(log_path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if not _RL_CONTEXT_RE.search(line):
                continue
            m = _WAIT_RE.search(line)
            if not m:
                continue
            events += 1
            total_wait += _to_seconds(m.group(1), m.group(2))
            if len(samples) < 3:
                samples.append(redact_text(line.strip()))
    return {
        "events": events,
        "total_wait_seconds": round(total_wait),
        "samples": samples,
    }


# ---------------------------------------------------------------------------
# Record-count sanity (DEVPROD-315)
# ---------------------------------------------------------------------------

SKIP_PCT_THRESHOLD = 5.0
MISMATCH_TOLERANCE_PCT = 1.0


def record_count_sanity(parsed):
    flags = []
    src_total = sum(parsed.get("records_per_stream", {}).values())
    d = parsed.get("destination", {})
    read = d.get("records_read", 0)
    processed = d.get("records_processed", 0)
    written = d.get("records_written", 0)
    skipped = d.get("records_skipped", 0)
    errored = d.get("records_errored", 0)
    status = (parsed.get("sync", {}).get("status") or "").lower()

    def flag(check, severity, detail):
        flags.append({"check": check, "severity": severity, "detail": detail})

    if errored:
        flag("destination_errored", "high",
             f"{errored} records errored at the destination — check destination logs/mappings.")

    if skipped and processed:
        pct = skipped / processed * 100
        flag("destination_skipped",
             "medium" if pct >= SKIP_PCT_THRESHOLD else "low",
             f"{skipped} of {processed} processed records were skipped "
             f"({pct:.1f}%) — possible mapping/filter anomaly.")

    if src_total and read and read < src_total * (1 - MISMATCH_TOLERANCE_PCT / 100):
        flag("read_vs_source_delta", "medium",
             f"Destination read {read} records but source streams emitted {src_total} "
             f"— records may have been lost in transit.")

    if src_total and processed and written == 0 and "fail" not in status:
        flag("zero_writes", "high",
             f"Source emitted {src_total} records and destination processed {processed}, "
             f"but 0 records were written — silent write failure.")

    if processed and (written + skipped + errored) < processed * (1 - MISMATCH_TOLERANCE_PCT / 100):
        flag("unaccounted_records", "medium",
             f"Destination processed {processed} records but only "
             f"{written + skipped + errored} are accounted for "
             f"(written {written} + skipped {skipped} + errored {errored}).")

    return {
        "totals": {
            "source_records_read": src_total,
            "destination_records_read": read,
            "destination_records_processed": processed,
            "destination_records_written": written,
            "destination_records_skipped": skipped,
            "destination_records_errored": errored,
        },
        "flags": flags,
    }


# ---------------------------------------------------------------------------
# Per-log categorization
# ---------------------------------------------------------------------------

_BUCKET_RE = re.compile(r"__bucket__\d+$")


def connector_name(parsed, filepath):
    dest_cfg = parsed.get("destination_config") or {}
    origin = dest_cfg.get("origin")
    if origin:
        return _BUCKET_RE.sub("", str(origin))
    image = parsed.get("source_image")
    if image:
        return image.rsplit("/", 1)[-1].split(":")[0]
    return os.path.splitext(os.path.basename(filepath))[0]


def categorize_parsed(parsed, patterns, filepath, rate_limit=None):
    findings = []
    for err in parsed.get("errors", []):
        findings.append(build_finding(err["message"], "error", err.get("timestamp"), patterns))
    for warn in parsed.get("warnings", []):
        findings.append(build_finding(warn["message"], "warn", warn.get("timestamp"), patterns))
    findings.sort(key=lambda f: (SEVERITY_RANK.get(f["severity"], 9), f["timestamp"] or ""))

    return {
        "file": filepath,
        "connector": connector_name(parsed, filepath),
        "sync": parsed.get("sync"),
        "findings": findings,
        "rate_limit": rate_limit,
        "record_count_sanity": record_count_sanity(parsed),
        "parsed": parsed,
    }


# ---------------------------------------------------------------------------
# Triage summary
# ---------------------------------------------------------------------------

def triage_summary(results):
    by_severity = {}
    by_category = {}
    prioritized = []
    for r in results:
        worst = None
        for f in r["findings"]:
            by_severity[f["severity"]] = by_severity.get(f["severity"], 0) + 1
            by_category[f["category"]] = by_category.get(f["category"], 0) + 1
            if worst is None or SEVERITY_RANK.get(f["severity"], 9) < SEVERITY_RANK.get(worst["severity"], 9):
                worst = f
        for flag in r["record_count_sanity"]["flags"]:
            by_severity[flag["severity"]] = by_severity.get(flag["severity"], 0) + 1
        status = (r["sync"] or {}).get("status")
        prioritized.append({
            "connector": r["connector"],
            "file": r["file"],
            "status": status,
            "finding_count": len(r["findings"]),
            "sanity_flag_count": len(r["record_count_sanity"]["flags"]),
            "worst_severity": worst["severity"] if worst else None,
            "top_finding": {
                "category": worst["category"],
                "summary": worst["summary"],
                "pattern_id": worst["pattern_id"],
            } if worst else None,
        })
    prioritized.sort(key=lambda p: (
        SEVERITY_RANK.get(p["worst_severity"], 9),
        -(p["finding_count"] + p["sanity_flag_count"]),
        p["connector"],
    ))
    return {
        "connectors_analyzed": len(results),
        "findings_by_severity": by_severity,
        "findings_by_category": by_category,
        "prioritized": prioritized,
    }


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------

def _fmt_duration(seconds):
    if seconds is None:
        return "unknown"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def render_report(output):
    lines = []
    results = output["logs"]
    triage = output["triage"]

    lines.append("# Airbyte Sync Triage Report")
    lines.append("")
    lines.append(f"Analyzed **{triage['connectors_analyzed']}** log(s).")
    lines.append("")

    if triage["findings_by_severity"]:
        lines.append("## Summary")
        lines.append("")
        lines.append("| Severity | Findings |")
        lines.append("|----------|----------|")
        for sev in ("critical", "high", "medium", "low"):
            if sev in triage["findings_by_severity"]:
                lines.append(f"| {sev} | {triage['findings_by_severity'][sev]} |")
        lines.append("")
        lines.append("| Category | Findings |")
        lines.append("|----------|----------|")
        for cat, n in sorted(triage["findings_by_category"].items(), key=lambda kv: -kv[1]):
            lines.append(f"| {cat} | {n} |")
        lines.append("")

    lines.append("## Connectors (worst first)")
    lines.append("")
    lines.append("| Connector | Status | Worst severity | Findings | Sanity flags |")
    lines.append("|-----------|--------|----------------|----------|--------------|")
    for p in triage["prioritized"]:
        lines.append(
            f"| {p['connector']} | {p['status'] or 'unknown'} | "
            f"{p['worst_severity'] or '-'} | {p['finding_count']} | {p['sanity_flag_count']} |"
        )
    lines.append("")

    order = {r["file"]: i for i, r in enumerate(results)}
    ordered = sorted(results, key=lambda r: next(
        (i for i, p in enumerate(triage["prioritized"]) if p["file"] == r["file"]),
        order[r["file"]],
    ))

    for r in ordered:
        sync = r["sync"] or {}
        lines.append(f"## {r['connector']}")
        lines.append("")
        lines.append(f"- **File:** `{r['file']}`")
        lines.append(f"- **Status:** {sync.get('status') or 'unknown'}")
        lines.append(f"- **Duration:** {_fmt_duration(sync.get('duration_seconds'))}")

        rl = r.get("rate_limit")
        if rl and rl["events"]:
            pct = ""
            dur = sync.get("duration_seconds")
            if dur:
                pct = f" ({min(100.0, rl['total_wait_seconds'] / dur * 100):.0f}% of sync time)"
            lines.append(
                f"- **Rate limiting:** {rl['events']} backoff event(s), "
                f"~{_fmt_duration(rl['total_wait_seconds'])} spent waiting{pct}"
            )
        lines.append("")

        if r["findings"]:
            # Group repeated findings (same pattern, or same message when
            # uncategorized) so remediation is shown once per failure mode.
            groups = []
            by_key = {}
            for f in r["findings"]:
                key = f["pattern_id"] or f["message"]
                if key in by_key:
                    by_key[key].append(f)
                else:
                    by_key[key] = [f]
                    groups.append(by_key[key])

            current_sev = None
            for group in groups:
                f = group[0]
                if f["severity"] != current_sev:
                    current_sev = f["severity"]
                    lines.append(f"### {current_sev.capitalize()} findings")
                    lines.append("")
                conf = f" — confidence: {f['match_confidence']}" if f["match_confidence"] else ""
                count = f" — {len(group)} occurrences" if len(group) > 1 else ""
                lines.append(f"**[{f['category']}] {f['summary']}**{conf}{count}")
                lines.append("")
                for occ in group[:3]:
                    if occ["timestamp"]:
                        lines.append(f"- At `{occ['timestamp']}`: `{occ['message'][:300]}`")
                    else:
                        lines.append(f"- `{occ['message'][:300]}`")
                if len(group) > 3:
                    lines.append(f"- ... and {len(group) - 3} more occurrences")
                for step in f["remediation"]:
                    lines.append(f"- Remediation: {step}")
                if f["source_doc"]:
                    lines.append(f"- Reference: {f['source_doc']}")
                lines.append("")
        else:
            lines.append("No errors or warnings found.")
            lines.append("")

        sanity = r["record_count_sanity"]
        if sanity["flags"]:
            lines.append("### Record-count sanity")
            lines.append("")
            for flag in sanity["flags"]:
                lines.append(f"- **{flag['severity']}** ({flag['check']}): {flag['detail']}")
            lines.append("")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Input handling
# ---------------------------------------------------------------------------

def _load_parser_module():
    path = os.path.join(SCRIPT_DIR, "analyze-sync-logs.py")
    spec = importlib.util.spec_from_file_location("analyze_sync_logs", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_input(filepath, parser_module):
    """Return (parsed_json, is_raw_log). Pre-parsed JSON files are detected by content."""
    try:
        with open(filepath, encoding="utf-8") as f:
            head = f.read(1).strip()
            if head == "{":
                f.seek(0)
                data = json.load(f)
                if isinstance(data, dict) and "sync" in data and "errors" in data:
                    return data, False
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        pass
    return parser_module.parse_log(filepath), True


def main():
    ap = argparse.ArgumentParser(
        description="Categorize Airbyte sync log errors with remediation guidance"
    )
    ap.add_argument("logfiles", nargs="*", help="Raw sync log(s) or pre-parsed JSON file(s)")
    ap.add_argument("--status-text", action="append", default=[],
                    help="Categorize a status string (e.g. from the Airbyte Sources page) "
                         "instead of / in addition to log files. Repeatable.")
    ap.add_argument("--report", metavar="PATH",
                    help="Write a standalone Markdown triage report to PATH")
    ap.add_argument("--patterns", default=DEFAULT_KB, help="Path to the pattern KB JSON")
    ap.add_argument("--overlay", default=DEFAULT_OVERLAY,
                    help="Path to a local overlay pattern file (default: "
                         "error_patterns.local.json next to this script, if present)")
    ap.add_argument("--compact", action="store_true", help="Compact JSON output")
    args = ap.parse_args()

    if not args.logfiles and not args.status_text:
        ap.error("provide at least one log file or --status-text")

    try:
        patterns = load_patterns(args.patterns, args.overlay)
    except (OSError, json.JSONDecodeError) as e:
        print(f"Error loading pattern KB: {e}", file=sys.stderr)
        sys.exit(1)

    parser_module = _load_parser_module() if args.logfiles else None

    results = []
    for path in args.logfiles:
        try:
            parsed, is_raw = load_input(path, parser_module)
        except (FileNotFoundError, PermissionError, OSError) as e:
            print(f"Error reading {path}: {e}", file=sys.stderr)
            sys.exit(1)
        rate_limit = aggregate_rate_limit_waits(path) if is_raw else None
        # State payloads are large and irrelevant to categorization
        for key in ("compressed_state", "initial_stream_states", "final_stream_states"):
            parsed.pop(key, None)
        results.append(categorize_parsed(parsed, patterns, path, rate_limit))

    status_findings = [
        build_finding(text, "error", None, patterns) for text in args.status_text
    ]

    output = {"logs": results, "triage": triage_summary(results)}
    if status_findings:
        output["status_text_findings"] = status_findings

    output = redact_obj(output)

    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            f.write(render_report(output))
        print(f"Report written to {args.report}", file=sys.stderr)

    json.dump(output, sys.stdout, indent=None if args.compact else 2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
