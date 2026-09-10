#!/usr/bin/env python3

import argparse
import base64
import binascii
import gzip
import json
import re
import sys
from datetime import datetime, timezone


def _try_parse_json(s):
    try:
        return json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return None


def _decompress_state(data):
    """Base64 decode + gzip decompress, then parse as JSON."""
    try:
        return json.loads(gzip.decompress(base64.b64decode(data)))
    except (binascii.Error, OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None


_TIMESTAMP_PREFIX = re.compile(
    r"^\[?\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]?\s+"
    r"(?:info|warn|error|debug|trace|INFO|WARN|ERROR|DEBUG|TRACE)\s+"
)

_LOG_LINE = re.compile(
    r"^\[?(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]?\s+"
    r"(info|warn|error|debug|trace|INFO|WARN|ERROR|DEBUG|TRACE)\s+"
    r"(.*)$"
)


def _strip_log_prefix(line):
    """Remove the timestamp + level prefix from a log continuation line."""
    return _TIMESTAMP_PREFIX.sub("", line)


def _epoch_to_iso(epoch_ms):
    """Convert epoch milliseconds to ISO 8601 string."""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()


def _timestamp_diff_seconds(start, end):
    """Compute seconds between two YYYY-MM-DD HH:MM:SS or ISO timestamps."""
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S"):
        try:
            s = datetime.strptime(start, fmt)
            e = datetime.strptime(end, fmt)
            return round((e - s).total_seconds())
        except ValueError:
            continue
    return None


def _simplify_catalog(catalog):
    """Strip full JSON schemas, keep only stream name and sync modes."""
    streams = []
    for entry in catalog.get("streams", []):
        stream = entry.get("stream", entry)
        streams.append({
            "name": stream.get("name", "unknown"),
            "sync_mode": entry.get("sync_mode"),
            "destination_sync_mode": entry.get("destination_sync_mode"),
        })
    return {"streams": streams}


def parse_log(filepath):
    result = {
        "attempt": None,
        "sync": {
            "status": None,
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
        },
        "source_version": None,
        "destination_version": None,
        "source_image": None,
        "destination_image": None,
        "source_config": None,
        "destination_config": None,
        "source_catalog": None,
        "destination_catalog": None,
        "compressed_state": None,
        "initial_stream_states": {},
        "final_stream_states": {},
        "records_per_stream": {},
        "destination": {
            "records_read": 0,
            "messages_read": 0,
            "records_processed": 0,
            "records_written": 0,
            "records_skipped": 0,
            "records_errored": 0,
            "processed_per_stream": {},
            "written_per_model": {},
        },
        "sync_summary": None,
        "errors": [],
        "warnings": [],
    }

    # For accumulating multi-line sync summary JSON
    summary_buf = None  # list of strings when accumulating
    summary_depth = 0

    # Track per-slice reads for streams that don't emit a stream-level total
    slice_counts = {}  # stream -> accumulated count

    # Track min/max timestamps for fallback sync timing.
    # Use min/max rather than first/last so reverse-chronological logs still work.
    min_timestamp = None
    max_timestamp = None

    with open(filepath, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")

            # Accumulate multi-line sync summary
            if summary_buf is not None:
                stripped = _strip_log_prefix(line)
                summary_depth += stripped.count("{") - stripped.count("}")
                summary_buf.append(stripped)
                if summary_depth <= 0:
                    text = "\n".join(summary_buf)
                    brace = text.find("{")
                    if brace >= 0:
                        result["sync_summary"] = _try_parse_json(text[brace:])
                    summary_buf = None
                continue

            # Sync summary: { (multi-line start)
            if re.search(r"[Ss]ync summary:\s*\{", line):
                stripped = _strip_log_prefix(line)
                idx = stripped.find("{")
                summary_buf = [stripped[idx:] if idx >= 0 else stripped]
                summary_depth = stripped.count("{") - stripped.count("}")
                if summary_depth <= 0:
                    result["sync_summary"] = _try_parse_json(summary_buf[0])
                    summary_buf = None
                continue

            # >> ATTEMPT N/M
            m = re.match(r">> ATTEMPT (\d+/\d+)", line)
            if m:
                result["attempt"] = m.group(1)
                continue

            # Track timestamps for sync timing fallback
            ts_match = _LOG_LINE.match(line)
            if ts_match:
                ts = ts_match.group(1)
                if min_timestamp is None or ts < min_timestamp:
                    min_timestamp = ts
                if max_timestamp is None or ts > max_timestamp:
                    max_timestamp = ts

            # Source version: 0.21.0
            m = re.search(r"Source version:\s*(.+)", line)
            if m:
                result["source_version"] = m.group(1).strip()
                continue

            # Destination version: 0.21.0
            m = re.search(r"Destination version:\s*(.+)", line)
            if m:
                result["destination_version"] = m.group(1).strip()
                continue

            # Config: {JSON} — detect source vs destination by content.
            # Destination configs have "edition_configs" or "origin" keys.
            m = re.search(r"Config:\s*(\{.+)", line)
            if m:
                parsed = _try_parse_json(m.group(1))
                if parsed is not None:
                    if "edition_configs" in parsed or "origin" in parsed:
                        result["destination_config"] = parsed
                    else:
                        result["source_config"] = parsed
                continue

            # Catalog: {JSON} — detect source vs destination by content.
            # Destination catalog stream names use "__" separators
            # (e.g. "mysource__github__assignees").
            m = re.search(r"Catalog:\s*(\{.+)", line)
            if m:
                parsed = _try_parse_json(m.group(1))
                if parsed is not None:
                    simplified = _simplify_catalog(parsed)
                    if simplified["streams"] and "__" in simplified["streams"][0]["name"]:
                        result["destination_catalog"] = simplified
                    else:
                        result["source_catalog"] = simplified
                continue

            # State: {"data":"H4sI..."}  — base64/gzip compressed
            m = re.search(r'State:\s*(\{.*"data"\s*:\s*"[A-Za-z0-9+/=]+".*\})', line)
            if m:
                parsed = _try_parse_json(m.group(1))
                if parsed and "data" in parsed:
                    result["compressed_state"] = _decompress_state(parsed["data"])
                continue

            # Setting initial state of <stream> stream to {JSON}
            m = re.search(r"Setting initial state of (\S+) stream to (.+)$", line)
            if m:
                parsed = _try_parse_json(m.group(2))
                if parsed is not None:
                    result["initial_stream_states"][m.group(1)] = parsed
                continue

            # Last recorded state of <stream> stream is {JSON}
            m = re.search(r"Last recorded state of (\S+) stream is (.+)$", line)
            if m:
                parsed = _try_parse_json(m.group(2))
                if parsed is not None:
                    result["final_stream_states"][m.group(1)] = parsed
                continue

            # Finished syncing <stream> stream. Read N records
            m = re.search(r"Finished syncing (\S+) stream\. Read (\d+) records", line)
            if m:
                result["records_per_stream"][m.group(1)] = int(m.group(2))
                continue

            # Finished processing <stream> stream slice {JSON}. Read N records
            m = re.search(
                r"Finished processing (\S+) stream slice .+\. Read (\d+) records", line
            )
            if m:
                stream = m.group(1)
                slice_counts[stream] = slice_counts.get(stream, 0) + int(m.group(2))
                continue

            # Processed records by stream: {JSON}
            m = re.search(r"Processed records by stream:\s*(\{.+\})", line)
            if m:
                parsed = _try_parse_json(m.group(1))
                if parsed:
                    result["destination"]["processed_per_stream"] = parsed
                continue

            # Wrote N records
            m = re.search(r"Wrote (\d+) records$", line)
            if m:
                result["destination"]["records_written"] = int(m.group(1))
                continue

            # Wrote records by model: {JSON}
            m = re.search(r"Wrote records by model:\s*(\{.+\})", line)
            if m:
                parsed = _try_parse_json(m.group(1))
                if parsed:
                    result["destination"]["written_per_model"] = parsed
                continue

            # Read N records (destination)
            m = re.search(r"^.*Read (\d+) records$", line)
            if m:
                result["destination"]["records_read"] = int(m.group(1))
                continue

            # Read N messages
            m = re.search(r"Read (\d+) messages$", line)
            if m:
                result["destination"]["messages_read"] = int(m.group(1))
                continue

            # Processed N records (destination total)
            m = re.search(r"^.*Processed (\d+) records$", line)
            if m:
                result["destination"]["records_processed"] = int(m.group(1))
                continue

            # Skipped N records
            m = re.search(r"Skipped (\d+) records$", line)
            if m:
                result["destination"]["records_skipped"] = int(m.group(1))
                continue

            # Errored N records
            m = re.search(r"Errored (\d+) records$", line)
            if m:
                result["destination"]["records_errored"] = int(m.group(1))
                continue

            # [source] image: .../airbyte-jira-source:0.21.0 resources: ...
            m = re.search(r"\[source\]\s+image:\s*(\S+)", line)
            if m:
                result["source_image"] = m.group(1)
                continue

            # [destination] image: .../airbyte-faros-destination:0.21.0 resources: ...
            m = re.search(r"\[destination\]\s+image:\s*(\S+)", line)
            if m:
                result["destination_image"] = m.group(1)
                continue

            # Collect errors and warnings by log level
            m = _LOG_LINE.match(line)
            if m:
                level = m.group(2).lower()
                if level == "error":
                    result["errors"].append({
                        "timestamp": m.group(1),
                        "message": m.group(3),
                    })
                elif level == "warn":
                    result["warnings"].append({
                        "timestamp": m.group(1),
                        "message": m.group(3),
                    })

    # Merge slice counts into records_per_stream for streams without a total
    for stream, count in slice_counts.items():
        if stream not in result["records_per_stream"]:
            result["records_per_stream"][stream] = count

    # Derive sync info from sync_summary, falling back to log timestamps
    ss = result["sync_summary"]
    if ss:
        result["sync"]["status"] = ss.get("status")
        total_stats = ss.get("totalStats", {})
        start = total_stats.get("replicationStartTime")
        end = total_stats.get("replicationEndTime")
        if start:
            result["sync"]["start_time"] = _epoch_to_iso(start)
        if end:
            result["sync"]["end_time"] = _epoch_to_iso(end)
        if start and end:
            result["sync"]["duration_seconds"] = round((end - start) / 1000)

    if not result["sync"]["start_time"] and min_timestamp:
        result["sync"]["start_time"] = min_timestamp
    if not result["sync"]["end_time"] and max_timestamp:
        result["sync"]["end_time"] = max_timestamp
    if result["sync"]["duration_seconds"] is None:
        s = result["sync"]["start_time"]
        e = result["sync"]["end_time"]
        if s and e:
            result["sync"]["duration_seconds"] = _timestamp_diff_seconds(s, e)

    return result


def main():
    parser = argparse.ArgumentParser(description="Analyze Airbyte sync logs")
    parser.add_argument("logfile", help="Path to sync log file")
    parser.add_argument(
        "--compact", action="store_true", help="Compact JSON output (no indentation)"
    )
    parser.add_argument(
        "--state", action="store_true",
        help="Include state fields (compressed_state, initial/final stream states)"
    )
    args = parser.parse_args()

    try:
        result = parse_log(args.logfile)
    except (FileNotFoundError, PermissionError, OSError) as e:
        print(f"Error reading log file: {e}", file=sys.stderr)
        sys.exit(1)

    if not args.state:
        del result["compressed_state"]
        del result["initial_stream_states"]
        del result["final_stream_states"]

    indent = None if args.compact else 2
    json.dump(result, sys.stdout, indent=indent)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
