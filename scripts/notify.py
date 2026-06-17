#!/usr/bin/env python3
"""Send a triage report by email via SMTP.

Container-friendly alternative to `mailx` for the Kubernetes CronJob path —
uses only the Python standard library. No-ops cleanly (exit 0) when SMTP is
not configured, so the same wrapper works whether or not alerting is set up.

Environment:
  SMTP_HOST       SMTP relay host. If unset, this script does nothing.
  SMTP_PORT       SMTP port (default 587).
  SMTP_FROM       From address (required when SMTP_HOST is set).
  SMTP_TO         Comma-separated recipients — a distribution list or a
                  Teams channel email address (required when SMTP_HOST is set).
  SMTP_USERNAME   Optional SMTP auth username.
  SMTP_PASSWORD   Optional SMTP auth password.
  SMTP_STARTTLS   "1" (default) to issue STARTTLS before auth, "0" to skip.

Usage: notify.py --subject "..." <report-file>
"""

import argparse
import os
import smtplib
import ssl
import sys
from email.message import EmailMessage


def _recipients(raw):
    return [addr.strip() for addr in (raw or "").split(",") if addr.strip()]


def build_message(subject, body, sender, recipients):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)
    return msg


def send(msg, host, port, username, password, use_starttls):
    with smtplib.SMTP(host, port, timeout=30) as server:
        if use_starttls:
            server.starttls(context=ssl.create_default_context())
        if username and password:
            server.login(username, password)
        server.send_message(msg)


def main():
    ap = argparse.ArgumentParser(description="Email a triage report via SMTP")
    ap.add_argument("report", help="Path to the report file to send")
    ap.add_argument("--subject", default="Airbyte sync triage report")
    args = ap.parse_args()

    host = os.environ.get("SMTP_HOST")
    if not host:
        # Alerting not configured — leave the report on disk and exit quietly.
        return

    sender = os.environ.get("SMTP_FROM")
    recipients = _recipients(os.environ.get("SMTP_TO"))
    if not sender or not recipients:
        print("SMTP_HOST is set but SMTP_FROM/SMTP_TO are missing", file=sys.stderr)
        sys.exit(1)

    try:
        with open(args.report, encoding="utf-8") as f:
            body = f.read()
    except OSError as e:
        print(f"Error reading report {args.report}: {e}", file=sys.stderr)
        sys.exit(1)

    msg = build_message(args.subject, body, sender, recipients)
    try:
        send(
            msg,
            host,
            int(os.environ.get("SMTP_PORT", "587")),
            os.environ.get("SMTP_USERNAME"),
            os.environ.get("SMTP_PASSWORD"),
            os.environ.get("SMTP_STARTTLS", "1") != "0",
        )
    except (smtplib.SMTPException, OSError, ssl.SSLError) as e:
        print(f"Failed to send triage email: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"Triage email sent to {', '.join(recipients)}", file=sys.stderr)


if __name__ == "__main__":
    main()
