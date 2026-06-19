"""Structured logging setup.

JSON logs by default for nix1 ingestion; text mode for local dev.

Secret hygiene
--------------

The ATU API is public (no key, no auth), so there is no upstream API secret
to leak. We still keep the same defensive posture as the Phish vault this was
templated from, two layers, defence in depth:

1. The httpx logger is pinned to WARNING. httpx logs every request URL at
   INFO; WARNING drops the line entirely, which keeps query strings out of
   logs regardless of what they ever come to contain.

2. A redaction filter is attached to the root logger that scrubs
   ``apikey=...`` query params and ``Authorization: Bearer ...`` headers
   from any log record's message and args, in case some other library
   (or a future enrichment source) ever logs a URL or header.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from typing import Any

# Patterns matched against the rendered log message. Conservative: only
# touches the value, never the key, so log lines stay searchable.
_APIKEY_QS_RE = re.compile(r"(?i)(apikey=)[^&\s\"'<>]+")
_AUTHZ_HDR_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)[A-Za-z0-9._\-]+")
_REDACTED = "[REDACTED]"


def _redact(text: str) -> str:
    text = _APIKEY_QS_RE.sub(rf"\1{_REDACTED}", text)
    text = _AUTHZ_HDR_RE.sub(rf"\1{_REDACTED}", text)
    return text


class SecretRedactionFilter(logging.Filter):
    """Scrub API keys and bearer tokens from log records before they format.

    Modifies ``record.msg`` (and a few common ``extra`` keys) in place so
    every downstream handler sees the redacted form, regardless of the
    formatter in use.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        # Materialise the message NOW (with %-args applied) so we can scrub
        # the final string. If we only scrubbed ``record.msg`` we'd miss
        # secrets passed through ``record.args`` — e.g. ``log.info("url=%s",
        # url)`` where ``url`` contains a secret query param.
        if isinstance(record.msg, str):
            try:
                rendered = record.getMessage()
            except (TypeError, ValueError):
                rendered = record.msg
            record.msg = _redact(rendered)
            record.args = ()
        # Common extras callers pass through.
        for key in ("url", "path", "query", "headers"):
            v = record.__dict__.get(key)
            if isinstance(v, str):
                record.__dict__[key] = _redact(v)
        return True


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter — one line per record, ISO timestamps."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
            "lvl": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        # Pull through `extra` keys callers passed.
        for k, v in record.__dict__.items():
            if k in {
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "asctime",
                "message",
                "taskName",
            }:
                continue
            try:
                json.dumps(v)
            except (TypeError, ValueError):
                v = repr(v)
            payload[k] = v
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO", fmt: str = "json") -> None:
    """Configure the root logger once. Idempotent.

    Always installs the secret-redaction filter and silences httpx INFO
    logging, even on repeat calls — those are correctness, not setup.
    """
    root = logging.getLogger()

    # Always silence httpx's per-request INFO log. httpx logs the full URL
    # at INFO; WARNING drops the line entirely.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # Always ensure the redaction filter is attached. Cheap to re-check.
    if not any(isinstance(f, SecretRedactionFilter) for f in root.filters):
        root.addFilter(SecretRedactionFilter())

    if getattr(root, "_umphreys_vault_configured", False):
        root.setLevel(level.upper())
        return

    handler = logging.StreamHandler(sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    # Belt and braces: filter on the handler too so records from
    # third-party loggers that bypass the root filter still get scrubbed.
    handler.addFilter(SecretRedactionFilter())
    root.addHandler(handler)
    root.setLevel(level.upper())
    root._umphreys_vault_configured = True  # type: ignore[attr-defined]
