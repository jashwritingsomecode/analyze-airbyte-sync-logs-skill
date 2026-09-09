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
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(TESTS_DIR)
SCRIPTS = os.path.join(ROOT, "scripts")
LOGS = os.path.join(TESTS_DIR, "logs")

PARSER = os.path.join(SCRIPTS, "analyze-sync-logs.py")
CATEGORIZER = os.path.join(SCRIPTS, "categorize.py")
FETCHER = os.path.join(SCRIPTS, "fetch-airbyte-logs.py")
NOTIFIER = os.path.join(SCRIPTS, "notify.py")
CRON = os.path.join(SCRIPTS, "triage-cron.sh")
EMITTER = os.path.join(SCRIPTS, "emit-sync-events.py")


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

    def test_noise_suppressed_and_retry_categorized(self):
        log_text = "\n".join([
            "[2026-06-10 08:00:00] ERROR Error closing resource io.airbyte.container."
            "orchestrator.worker.io.LocalContainerAirbyteSource@1a2b3c; recording",
            "[2026-06-10 08:00:01] ERROR (Use `node --trace-warnings ...` to show "
            "where the warning was created)",
            "[2026-06-10 08:00:02] ERROR runJobs failed; recording failure but "
            "continuing to finish.",
            "[2026-06-10 08:00:03] ERROR Failing job: 16032, reason: Job failed after "
            "too many retries for connection abc-123",
        ])
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "noisy.log")
            with open(path, "w", encoding="utf-8") as f:
                f.write(log_text + "\n")
            result, _ = categorize(path)
        entry = result["logs"][0]
        # The 3 noise lines are routed out of findings...
        self.assertEqual(entry["suppressed"]["count"], 3)
        # ...leaving only the real terminal-failure finding.
        self.assertEqual(len(entry["findings"]), 1)
        self.assertEqual(entry["findings"][0]["pattern_id"], "job-failed-retries")
        self.assertEqual(entry["findings"][0]["category"], "partial_sync")
        self.assertEqual(result["triage"]["suppressed_noise_total"], 3)
        self.assertNotIn("noise", result["triage"]["findings_by_category"])

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

    def test_status_text_source_check_failed(self):
        # The generic check-phase failure string Airbyte shows when a sync
        # never starts and produces no detailed log.
        result, _ = categorize(
            "--status-text",
            "Failure in source: Checking source connection failed - please "
            "review this connection's configuration to prevent future syncs "
            "from failing",
        )
        f = result["status_text_findings"][0]
        self.assertEqual(f["pattern_id"], "source-check-failed")
        self.assertEqual(f["category"], "config_setup")
        self.assertEqual(f["severity"], "high")


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
                {"job": {"id": 42, "status": "failed"}, "attempts": [{"id": 0}, {"id": 1}]},
                {"job": {"id": 41, "status": "succeeded"}, "attempts": [{"id": 0}]},
                {"job": {"id": 40, "status": "running"}, "attempts": [{"id": 0}]},
            ]}
        elif self.path == "/api/v1/attempt/get_for_job":
            # Structured-logging shape: logs.events[] with separate fields.
            msg = ("HTTP 401 Unauthorized: Bad credentials"
                   if body.get("jobId") == 42 else
                   "Finished syncing commits stream. Read 10 records")
            level = "ERROR" if body.get("jobId") == 42 else "INFO"
            resp = {
                "attempt": {"id": body.get("attemptNumber")},
                "logType": "structured",
                "logs": {
                    "version": "1",
                    "logLines": [],
                    "events": [{
                        "timestamp": 1781920805000,
                        "message": msg,
                        "level": level,
                        "logSource": "source",
                        "caller": None,
                    }],
                },
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

    def test_dry_run_reports_shape_without_side_effects(self):
        env = dict(os.environ, AIRBYTE_API_URL=self.api_url)
        with tempfile.TemporaryDirectory() as tmp:
            proc = subprocess.run(
                [sys.executable, FETCHER, "--output-dir", tmp, "--dry-run"],
                capture_output=True, text=True, env=env,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            out = proc.stdout
            self.assertIn("DRY RUN", out)
            self.assertIn("structured", out)        # detected logType
            self.assertIn("COMPATIBLE", out)        # verdict
            # No files or state written in dry-run mode.
            self.assertFalse(os.path.exists(os.path.join(tmp, ".fetch-state.json")))
            self.assertEqual(os.listdir(tmp), [])

    def test_fetched_log_flows_through_categorizer(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = self.fetch(tmp)
            result, _ = categorize(paths[0])
            by_id = findings_by_id(result)
            self.assertIn("auth-failure", by_id)


class _StubPublicApiHandler(BaseHTTPRequestHandler):
    """Mimics a Public-API-only Airbyte (1.x): the legacy Config list endpoints
    404, discovery is served at /api/public/v1/..., and log content still comes
    from the Config attempt/get_for_job endpoint."""

    def _send(self, obj, code=200):
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/api/public/v1/connections"):
            self._send({"data": [
                {"connectionId": "conn-1", "name": "code acme github"},
            ], "next": None})
        elif self.path.startswith("/api/public/v1/jobs"):
            self._send({"data": [
                {"jobId": 42, "status": "failed", "attemptCount": 2},
                {"jobId": 41, "status": "succeeded", "attemptCount": 1},
                {"jobId": 40, "status": "running", "attemptCount": 1},
            ]})
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        if self.path == "/api/v1/attempt/get_for_job":
            msg = ("HTTP 401 Unauthorized: Bad credentials"
                   if body.get("jobId") == 42 else
                   "Finished syncing commits stream. Read 10 records")
            level = "ERROR" if body.get("jobId") == 42 else "INFO"
            self._send({
                "attempt": {"id": body.get("attemptNumber")},
                "logType": "structured",
                "logs": {"version": "1", "logLines": [], "events": [{
                    "timestamp": 1781920805000, "message": msg, "level": level,
                    "logSource": "source", "caller": None,
                }]},
            })
        else:
            # All legacy Config list endpoints are gone on this instance.
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):
        pass


class TestFetcherPublicApi(unittest.TestCase):
    def setUp(self):
        self.server = HTTPServer(("127.0.0.1", 0), _StubPublicApiHandler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.api_url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def _run(self, outdir, *extra):
        env = dict(os.environ, AIRBYTE_API_URL=self.api_url)
        proc = subprocess.run(
            [sys.executable, FETCHER, "--output-dir", outdir, *extra],
            capture_output=True, text=True, env=env,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return proc

    def test_falls_back_to_public_api_for_discovery(self):
        # Config list endpoints 404 -> discovery must use the Public API, and
        # the failed job's log (via Config attempt/get_for_job) still lands.
        with tempfile.TemporaryDirectory() as tmp:
            paths = [p for p in self._run(tmp).stdout.splitlines() if p.strip()]
            self.assertEqual(len(paths), 1)
            self.assertIn("code-acme-github__job42.log", paths[0])
            with open(paths[0], encoding="utf-8") as f:
                self.assertIn("401 Unauthorized", f.read())

    def test_dry_run_reports_public_discovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = self._run(tmp, "--dry-run").stdout
            self.assertIn("Discovery API          : public", out)
            self.assertIn("Connections discovered : 1", out)
            self.assertIn("COMPATIBLE", out)


class _CronLogMixin:
    """Serve synthetic raw logs through either discovery API's real fetch path."""

    def do_POST(self):
        if self.path != "/api/v1/attempt/get_for_job":
            return super().do_POST()
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        data = json.dumps({
            "logType": "formatted",
            "logs": {"logLines": self.server.logs[body["jobId"]].splitlines()},
        }).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class _CronConfigHandler(_CronLogMixin, _StubAirbyteHandler):
    pass


class _CronPublicHandler(_CronLogMixin, _StubPublicApiHandler):
    pass


class TestCronEvents(unittest.TestCase):
    handlers = (_CronConfigHandler, _CronPublicHandler)

    @contextmanager
    def cron_environment(self, handler, failed_text=None, **settings):
        with open(log("success_clean.log"), encoding="utf-8") as source:
            clean = source.read()
        with open(log("auth_failure.log"), encoding="utf-8") as source:
            failed = source.read()
        server = HTTPServer(("127.0.0.1", 0), handler)
        server.logs = {42: failed if failed_text is None else failed_text, 41: clean}
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                env = dict(os.environ)
                for key in list(env):
                    if key.startswith(("AIRBYTE_", "SMTP_", "TRIAGE_")):
                        env.pop(key)
                env.update(AIRBYTE_API_URL=f"http://127.0.0.1:{server.server_address[1]}",
                           TRIAGE_WORK_DIR=tmp, **settings)
                yield tmp, env
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def poll(self, env):
        proc = subprocess.run(["bash", CRON], env=env, capture_output=True,
                              text=True, timeout=30)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return proc.stdout

    def test_default_json_and_duplicate_poll(self):
        for handler in self.handlers:
            with self.subTest(api=handler.__name__), self.cron_environment(handler) as (tmp, env):
                lines = self.poll(env).splitlines()
                self.assertEqual(len(lines), 1)
                event = json.loads(lines[0])
                self.assertEqual(event["event"], "airbyte.sync.triage")
                self.assertEqual(event["job_id"], 42)
                self.assertEqual(event["status"], "failed")
                self.assertEqual(event["worst_severity"], "high")
                self.assertEqual(event["finding_count"], len(event["findings"]))
                self.assertTrue(any(f["pattern_id"] == "auth-failure" for f in event["findings"]))
                self.assertEqual(set(event), {
                    "event", "connector", "job_id", "status", "worst_severity",
                    "finding_count", "top_finding", "findings", "sanity_flags",
                    "sanity_details", "suppressed_noise_count", "rate_limit_wait_s",
                })
                self.assertTrue(any(p.endswith(".md") for p in os.listdir(tmp)))
                self.assertTrue(any(p.startswith("triage-") and p.endswith(".json")
                                    for p in os.listdir(tmp)))
                self.assertEqual(self.poll(env), "")

    def test_all_includes_success_and_no_finding_failure(self):
        failed = '[2026-06-10 08:00:00] INFO Sync summary: {"status":"failed"}\n'
        for handler in self.handlers:
            with self.subTest(api=handler.__name__), self.cron_environment(
                    handler, failed, TRIAGE_FETCH_ALL="1") as (_, env):
                events = {e["job_id"]: e for e in map(json.loads, self.poll(env).splitlines())}
                self.assertEqual(set(events), {41, 42})
                self.assertEqual(events[41]["status"], "completed")
                self.assertEqual(events[42]["status"], "failed")
                for event in events.values():
                    self.assertEqual(event["finding_count"], 0)
                    self.assertEqual(event["findings"], [])
                    self.assertIsNone(event["top_finding"])
                    self.assertIsNone(event["worst_severity"])
                self.assertEqual(self.poll(env), "")

    def test_unknown_status_noise_and_warning_redaction(self):
        secret = "cron-private-secret"
        tail = "full-message-tail"
        message = 'Odd failure "quoted" password=' + secret + " " + "x" * 260 + tail
        raw = (f"[2026-06-10 08:00:00] ERROR {message}\n"
               "[2026-06-10 08:00:01] WARN Unrecognized warning token=" + secret + "\n"
               "[2026-06-10 08:00:02] ERROR runJobs failed; recording failure but continuing to finish.\n")
        for handler in self.handlers:
            with self.subTest(api=handler.__name__), self.cron_environment(handler, raw) as (tmp, env):
                out = self.poll(env)
                self.assertNotIn(secret, out)
                self.assertNotIn(tail, out)
                event = json.loads(out)
                self.assertEqual(event["status"], "unknown")
                self.assertEqual(event["suppressed_noise_count"], 1)
                self.assertEqual(event["finding_count"], 2)
                self.assertEqual({f["level"] for f in event["findings"]}, {"error", "warn"})
                self.assertTrue(event["findings"][0]["summary_truncated"])
                self.assertLessEqual(len(event["findings"][0]["summary"]), 240)
                self.assertIn("***REDACTED***", out)
                # Raw downloads are outside report redaction; inspect report artifacts only.
                for name in os.listdir(tmp):
                    if name.startswith("triage-"):
                        with open(os.path.join(tmp, name), encoding="utf-8") as source:
                            artifact = source.read()
                        self.assertNotIn(secret, artifact)
                        if name.endswith(".json"):
                            # Markdown already samples/truncates messages; JSON is lossless.
                            self.assertIn(tail, artifact)

    def test_sanity_only_and_rate_limit_fields(self):
        raw = ('[2026-06-10 08:00:00] INFO Sync summary: {"status":"completed"}\n'
               '[2026-06-10 08:00:01] INFO Finished syncing commits stream. Read 10 records\n'
               '[2026-06-10 08:00:02] INFO Processed 10 records\n'
               '[2026-06-10 08:00:03] INFO Wrote 0 records\n'
               '[2026-06-10 08:00:04] INFO rate limit: waiting for 30 seconds\n')
        for handler in self.handlers:
            with self.subTest(api=handler.__name__), self.cron_environment(handler, raw) as (_, env):
                event = json.loads(self.poll(env))
                self.assertEqual(event["finding_count"], 0)
                self.assertIsNone(event["worst_severity"])
                self.assertEqual(event["sanity_flags"], len(event["sanity_details"]))
                self.assertTrue(any(f["check"] == "zero_writes" and f["severity"] == "high"
                                    for f in event["sanity_details"]))
                self.assertEqual(event["rate_limit_wait_s"], 30)

    def test_markdown_opt_in_keeps_json_and_redaction(self):
        raw = '[2026-06-10 08:00:00] ERROR Odd failure password=cron-private-secret\n'
        with self.cron_environment(_CronConfigHandler, raw, TRIAGE_STDOUT_REPORT="1") as (_, env):
            out = self.poll(env)
            self.assertEqual(json.loads(out.splitlines()[0])["job_id"], 42)
            self.assertIn("=== TRIAGE-REPORT-BEGIN", out)
            self.assertIn("=== TRIAGE-REPORT-END", out)
            self.assertNotIn("cron-private-secret", out)
            self.assertEqual(self.poll(env), "")

    def test_formatter_redacts_before_truncating(self):
        result, _ = categorize(log("unknown_error.log"))
        entry = result["logs"][0]
        # Deliberately bypass input redaction to exercise the emitter boundary.
        token = "ghp_" + "a" * 36
        entry["findings"][0]["message"] = "x" * 220 + " " + token
        result["triage"]["prioritized"][0]["connector"] = "password=private-connector"
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "input.json")
            with open(path, "w", encoding="utf-8") as dest:
                json.dump(result, dest)
            proc = run(EMITTER, path)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        event = json.loads(proc.stdout)
        self.assertIsNone(event["job_id"])
        self.assertNotIn("ghp_", proc.stdout)
        self.assertNotIn("private-connector", proc.stdout)
        self.assertIn("***REDACTED***", proc.stdout)


class TestNotifier(unittest.TestCase):
    def _load_notify_module(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("notify", NOTIFIER)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_build_message(self):
        mod = self._load_notify_module()
        msg = mod.build_message("subj", "the report body",
                                "from@example.com", ["a@example.com", "b@example.com"])
        self.assertEqual(msg["Subject"], "subj")
        self.assertEqual(msg["From"], "from@example.com")
        self.assertEqual(msg["To"], "a@example.com, b@example.com")
        self.assertIn("the report body", msg.get_content())

    def test_noop_without_smtp_host(self):
        # No SMTP_HOST -> exit 0, send nothing, even with a real report path.
        env = {k: v for k, v in os.environ.items() if not k.startswith("SMTP_")}
        with tempfile.TemporaryDirectory() as tmp:
            report = os.path.join(tmp, "r.md")
            with open(report, "w", encoding="utf-8") as f:
                f.write("# report")
            proc = subprocess.run(
                [sys.executable, NOTIFIER, report],
                capture_output=True, text=True, env=env,
            )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), "")

    def test_missing_from_to_errors(self):
        env = dict(os.environ, SMTP_HOST="smtp.invalid")
        env.pop("SMTP_FROM", None)
        env.pop("SMTP_TO", None)
        with tempfile.TemporaryDirectory() as tmp:
            report = os.path.join(tmp, "r.md")
            with open(report, "w", encoding="utf-8") as f:
                f.write("# report")
            proc = subprocess.run(
                [sys.executable, NOTIFIER, report],
                capture_output=True, text=True, env=env,
            )
        self.assertEqual(proc.returncode, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
