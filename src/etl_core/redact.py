"""Secret redaction for logs, errors and request summaries.

A :class:`Redactor` is built once per run from the resolved secret values.
Every NodeError message, request summary and diagnostic sample body passes
through it before being persisted or shown, so secret material never leaves
the worker even when requests fail.
"""
from __future__ import annotations

import base64
import re
import urllib.parse
from collections.abc import Iterable
from typing import Any

MASK = "***"

# Query parameter names that are masked in URLs even when their value is not
# a known secret (defense in depth for keys the run does not know about).
SENSITIVE_PARAM_RE = re.compile(
    r"(key|token|secret|password|passwd|pwd|auth|signature|credential|session)",
    re.IGNORECASE,
)

# Secrets shorter than this are not substring-replaced (replacing every "a"
# in every message would destroy output); they are still protected because
# they only ever appear in headers/URLs, which are masked by name.
MIN_REDACTABLE_LENGTH = 4


class Redactor:
    def __init__(self, secrets: Iterable[str] = ()):
        variants: set[str] = set()
        for value in secrets:
            if not value or len(value) < MIN_REDACTABLE_LENGTH:
                continue
            variants.add(value)
            quoted = urllib.parse.quote(value, safe="")
            if quoted != value:
                variants.add(quoted)
            # Basic auth encodes "user:password" as base64 in the header.
            encoded = base64.b64encode(value.encode()).decode()
            if len(encoded) >= MIN_REDACTABLE_LENGTH:
                variants.add(encoded)
        # Replace longest first so overlapping secrets redact fully.
        self._values = sorted(variants, key=len, reverse=True)

    def redact(self, text: str) -> str:
        for value in self._values:
            if value in text:
                text = text.replace(value, MASK)
        return text

    def redact_url(self, url: str) -> str:
        try:
            parts = urllib.parse.urlsplit(url)
        except ValueError:
            return self.redact(url)
        netloc = parts.netloc
        if "@" in netloc:  # mask userinfo credentials
            _, _, hostport = netloc.rpartition("@")
            netloc = f"{MASK}@{hostport}"
        if parts.query:
            pairs = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
            masked = [
                (name, MASK if SENSITIVE_PARAM_RE.search(name) else value)
                for name, value in pairs
            ]
            query = urllib.parse.urlencode(masked, safe="*")
        else:
            query = parts.query
        rebuilt = urllib.parse.urlunsplit(
            (parts.scheme, netloc, parts.path, query, parts.fragment)
        )
        return self.redact(rebuilt)

    def request_summary(self, method: str, url: str) -> str:
        return f"{method.upper()} {self.redact_url(url)}"

    def redact_obj(self, obj: Any) -> Any:
        if isinstance(obj, str):
            return self.redact(obj)
        if isinstance(obj, dict):
            return {key: self.redact_obj(value) for key, value in obj.items()}
        if isinstance(obj, list):
            return [self.redact_obj(item) for item in obj]
        return obj
