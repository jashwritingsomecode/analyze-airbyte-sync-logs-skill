#!/usr/bin/env python3
"""Test suite for the sync-log parser + categorization engine.

Run: python3 tests/run_tests.py
"""

import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(TESTS_DIR)
SCRIPTS = os.path.join(ROOT, "scripts")
LOGS = os.path.join(TESTS_DIR, "logs")

PARSER = os.path.join(SCRIPTS, "analyze-sync-logs.py")
CATEGORIZER = os.path.join(SCRIPTS, "categorize.py")
FETCHER = os.path.join(SCRIPTS, "fetch-airbyte-logs.py")


def log(name):
    return os.path.join(LOGS, name)


def run(script, *args):
    return subprocess.run(
        [sys.executable, script, *args], capture_output=True, text=True
    )


def categorize(*args):
    proc = run(CATEGORIZER, *args)
    if proc.returncode != 0:
        raise AssertionError(f"categorize.py failed: {proc.stderr}")
    return json.loads(proc.stdout), proc


def findings_by_id(result, log_index=0):
    return {f["pattern_id"]: f for f in result["logs"][log_index]["findings"]}


class TestParserRegression(unittest.TestCase):
    def test_clean_log_parses(self):
        proc = run(PARSER, log("success_clean.log"))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        parsed = json.loads(proc.stdout)
        self.assertEqual(parsed["sync"]["status"], "completed")
        self.assertEqual(parsed["sync"]["duration_seconds"], 3540)
        self.assertEqual(parsed["source_version"], "0.21.0")
        self.assertEqual(parsed["records_per_stream"]["pull_requests"], 1200)
        self.assertEqual(parsed["records_per_stream"]["commits"], 1200)
        self.assertEqual(parsed["destination"]["records_written"], 2400)
        self.assertEqual(parsed["errors"], [])
        self.assertEqual(parsed["warnings"], [])


class TestCategorization(unittest.TestCase):
    def test_clean_log_no_findings(self):
        result, _ = categorize(log("success_clean.log"))
        entry = result["logs"][0]
        self.assertEqual(entry["findings"], [])
        self.assertEqual(entry["record_count_sanity"]["flags"], [])
        self.assertEqual(entry["connector"], "code-acme-github")

    def test_auth_failure(self):
        result, _ = categorize(log("auth_failure.log"))
        by_id = findings_by_id(result)
        self.assertIn("auth-failure", by_id)
        self.assertEqual(by_id["auth-failure"]["category"], "permission")
        self.assertEqual(by_id["auth-failure"]["severity"], "high")
        self.assertEqual(by_id["auth-failure"]["match_confidence"], "medium")
        self.assertIn("source-exit-nonzero", by_id)
        self.assertTrue(by_id["auth-failure"]["remediation"])

    def test_oom_failure(self):
        result, _ = categorize(log("oom_failure.log"))
        by_id = findings_by_id(result)
        self.assertIn("out-of-memory", by_id)
        self.assertEqual(by_id["out-of-memory"]["category"], "resource_oom")
        self.assertEqual(by_id["out-of-memory"]["severity"], "critical")
        self.assertEqual(by_id["out-of-memory"]["match_confidence"], "high")
        self.assertIn("source_doc", by_id["out-of-memory"])

    def test_tls_failure(self):
        result, _ = categorize(log("tls_failure.log"))
        by_id = findings_by_id(result)
        self.assertIn("self-signed-certificate", by_id)
        self.assertEqual(by_id["self-signed-certificate"]["category"], "network_tls")

    def test_rate_limit_findings(self):
        result, _ = categorize(log("rate_limited.log"))
        cats = [f["category"] for f in result["logs"][0]["findings"]]
        self.assertEqual(cats.count("rate_limit"), 3)

    def test_uncategorized_fallback(self):
        result, _ = categorize(log("unknown_error.log"))
        f = result["logs"][0]["findings"][0]
        self.assertEqual(f["category"], "uncategorized")
        self.assertIsNone(f["pattern_id"])
        self.assertIn("Flux capacitor", f["message"])

    def test_status_text_mode(self):
        result, _ = categorize("--status-text",
                               "Failure — sonarqube-feed exit code was 1")
        f = result["status_text_findings"][0]
        self.assertEqual(f["pattern_id"], "source-exit-nonzero")
        self.assertEqual(f["category"], "partial_sync")


class TestRecordCountSanity(unittest.TestCase):
    def test_high_skip_flagged(self):
        result, _ = categorize(log("partial_skip.log"))
        flags = {f["check"]: f for f in result["logs"][0]["record_count_sanity"]["flags"]}
        self.assertIn("destination_skipped", flags)
        self.assertEqual(flags["destination_skipped"]["severity"], "medium")


class TestRateLimitAggregation(unittest.TestCase):
    def test_wait_totals(self):
        result, _ = categorize(log("rate_limited.log"))
        rl = result["logs"][0]["rate_limit"]
        self.assertEqual(rl["events"], 3)
        self.assertEqual(rl["total_wait_seconds"], 480)

    def test_bucket_suffix_stripped(self):
        result, _ = categorize(log("rate_limited.log"))
        self.assertEqual(result["logs"][0]["connector"], "code-acme-gitlab")


class TestTriage(unittest.TestCase):
    def test_multi_connector_prioritization(self):
        result, _ = categorize(
            log("success_clean.log"), log("auth_failure.log"),
            log("oom_failure.log"), log("tls_failure.log"),
        )
        triage = result["triage"]
        self.assertEqual(triage["connectors_analyzed"], 4)
        self.assertEqual(triage["prioritized"][0]["connector"], "code-acme-bitbucket")
        self.assertEqual(triage["prioritized"][0]["worst_severity"], "critical")
        self.assertEqual(triage["prioritized"][-1]["connector"], "code-acme-github")
        self.assertGreaterEqual(triage["findings_by_severity"]["high"], 3)


class TestRedaction(unittest.TestCase):
    def test_secrets_masked_in_json(self):
        result, proc = categorize(log("secrets.log"))
        out = proc.stdout
        self.assertNotIn("ghp_FAKEabcdefghijklmnopqrstuvwxyz1234", out)
        self.assertNotIn("hunter2secret", out)
        self.assertNotIn("fake_key_abc123def456", out)
        self.assertIn("***REDACTED***", out)
        src_cfg = result["logs"][0]["parsed"]["source_config"]
        self.assertEqual(src_cfg["api_key"], "***REDACTED***")
        self.assertEqual(src_cfg["token"], "***REDACTED***")

    def test_secrets_masked_in_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = os.path.join(tmp, "report.md")
            categorize(log("secrets.log"), "--report", report)
            with open(report, encoding="utf-8") as f:
                content = f.read()
            self.assertNotIn("ghp_FAKEabcdefghijklmnopqrstuvwxyz1234", content)
            self.assertNotIn("hunter2secret", content)


class TestMarkdownReport(unittest.TestCase):
    def test_report_renders(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = os.path.join(tmp, "report.md")
            categorize(
                log("oom_failure.log"), log("rate_limited.log"),
                log("success_clean.log"), "--report", report,
            )
            with open(report, encoding="utf-8") as f:
                content = f.read()
            self.assertIn("# Airbyte Sync Triage Report", content)
            self.assertIn("## Connectors (worst first)", content)
            self.assertIn("Remediation:", content)
            self.assertIn("Rate limiting:", content)
            self.assertIn("backoff event(s)", content)


class TestRobustness(unittest.TestCase):
    def test_malformed_log_no_crash(self):
        result, proc = categorize(log("malformed.log"))
        self.assertNotIn("Traceback", proc.stderr)
        self.assertEqual(result["logs"][0]["findings"], [])

    def test_preparsed_json_input(self):
        proc = run(PARSER, log("auth_failure.log"))
        with tempfile.TemporaryDirectory() as tmp:
            json_path = os.path.join(tmp, "parsed.json")
            with open(json_path, "w", encoding="utf-8") as f:
                f.write(proc.stdout)
            result, _ = categorize(json_path)
        by_id = findings_by_id(result)
        self.assertIn("auth-failure", by_id)
        self.assertIsNone(result["logs"][0]["rate_limit"])

    def test_kb_is_valid(self):
        with open(os.path.join(SCRIPTS, "error_patterns.json"), encoding="utf-8") as f:
            kb = json.load(f)
        ids = [p["id"] for p in kb["patterns"]]
        self.assertEqual(len(ids), len(set(ids)), "duplicate pattern ids")
        for p in kb["patterns"]:
            self.assertIn(p["category"], kb["taxonomy"], p["id"])
            self.assertIn(p["severity"], kb["severities"], p["id"])
            self.assertTrue(p["match"]["any"], p["id"])
            self.assertTrue(p["remediation"], p["id"])
            self.assertIn(p["provenance"], ("faros-doc", "generic"), p["id"])


class _StubAirbyteHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        if self.path == "/api/v1/workspaces/list":
            resp = {"workspaces": [{"workspaceId": "ws-1"}]}
        elif self.path == "/api/v1/connections/list":
            resp = {"connections": [
                {"connectionId": "conn-1", "name": "code acme github"},
            ]}
        elif self.path == "/api/v1/jobs/list":
            resp = {"jobs": [
                {"job": {"id": 42, "status": "failed"}},
                {"job": {"id": 41, "status": "succeeded"}},
                {"job": {"id": 40, "status": "running"}},
            ]}
        elif self.path == "/api/v1/jobs/get":
            line = ("[2026-06-10 08:00:05] ERROR HTTP 401 Unauthorized: Bad credentials"
                    if body.get("id") == 42 else
                    "[2026-06-10 08:00:05] INFO Finished syncing commits stream. Read 10 records")
            resp = {
                "job": {"id": body.get("id")},
                "attempts": [{"logs": {"logLines": [line]}}],
            }
        else:
            self.send_response(404)
            self.end_headers()
            return
        data = json.dumps(resp).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


class TestFetcher(unittest.TestCase):
    def setUp(self):
        self.server = HTTPServer(("127.0.0.1", 0), _StubAirbyteHandler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.api_url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def fetch(self, outdir, *extra):
        env = dict(os.environ, AIRBYTE_API_URL=self.api_url)
        proc = subprocess.run(
            [sys.executable, FETCHER, "--output-dir", outdir, *extra],
            capture_output=True, text=True, env=env,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return [p for p in proc.stdout.splitlines() if p.strip()]

    def test_fetches_failed_jobs_and_dedupes(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = self.fetch(tmp)
            self.assertEqual(len(paths), 1)
            self.assertIn("code-acme-github__job42.log", paths[0])
            with open(paths[0], encoding="utf-8") as f:
                self.assertIn("401 Unauthorized", f.read())
            # Second poll: job already processed, nothing new
            self.assertEqual(self.fetch(tmp), [])

    def test_all_flag_includes_succeeded(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = self.fetch(tmp, "--all")
            names = [os.path.basename(p) for p in paths]
            self.assertIn("code-acme-github__job42.log", names)
            self.assertIn("code-acme-github__job41.log", names)
            # Running job 40 must not be fetched or marked processed
            self.assertEqual(len(names), 2)

    def test_fetched_log_flows_through_categorizer(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = self.fetch(tmp)
            result, _ = categorize(paths[0])
            by_id = findings_by_id(result)
            self.assertIn("auth-failure", by_id)


if __name__ == "__main__":
    unittest.main(verbosity=2)
