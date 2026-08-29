"""Core types — severity levels and message dataclass."""
from dataclasses import dataclass
from enum import StrEnum


class AlertSeverity(StrEnum):
    """Alert severity levels.

    WARN:     Unexpected but non-breaking (e.g., unknown request param).
    ERROR:    Something failed that shouldn't have (e.g., import pipeline error).
    CRITICAL: Infrastructure-level problem (e.g., DB pool exhausted).
    """

    WARN = "warn"
    ERROR = "error"
    CRITICAL = "critical"


# Severities that trigger immediate individual email on first occurrence
IMMEDIATE_SEVERITIES = frozenset({AlertSeverity.ERROR, AlertSeverity.CRITICAL})

# Severities batched into periodic digests
DIGEST_SEVERITIES = frozenset({AlertSeverity.WARN})


@dataclass(frozen=True)
class AlertMessage:
    """Structured alert notification ready for transport delivery."""

    subject: str
    html_body: str
    text_body: str
    severity: str
    category: str
    alert_count: int = 1
    # Deployment environment the alert came from ("staging", "production", ...).
    # None when the host app did not configure one -- subjects and bodies then
    # render exactly as they did before environment labelling existed.
    environment: str | None = None
