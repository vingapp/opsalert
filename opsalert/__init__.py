"""opsalert — standalone operational alerting.

Fire-and-forget alerts with dashboard queries and pluggable delivery.

Usage::

    import opsalert

    # Configure once at startup
    opsalert.configure(session_factory=my_session_factory)

    # Fire alerts from anywhere
    opsalert.warn("sendgrid_delivery", message="SendGrid 429", source="email")
    opsalert.error("sendgrid_delivery", message="SendGrid 500", source="email")
    opsalert.critical("startup_failure", message="DB pool exhausted")
"""
from opsalert._config import configure, get_config, reset_config
from opsalert._dispatch import critical, error, warn
from opsalert.cleanup import cleanup_alerts
from opsalert.delivery import deliver_alerts
from opsalert.model import Alert, OpsAlertBase
from opsalert.query import (
    delete_by_category,
    delete_by_id,
    query_aggregates,
    query_by_trace_id,
    query_categories,
    query_messages,
    query_next_fix,
    query_occurrences,
)
from opsalert.store import fire_alert
from opsalert.transport import CallableTransport, LogTransport, Transport, WebhookTransport
from opsalert.types import DIGEST_SEVERITIES, IMMEDIATE_SEVERITIES, AlertMessage, AlertSeverity


def ensure_tables(engine) -> None:
    """Create opsalert tables if they don't exist.

    Call once at application startup with a sync engine.
    Uses checkfirst=True (default) — safe to call repeatedly.
    """
    OpsAlertBase.metadata.create_all(engine)


__all__ = [
    # Configuration
    "configure",
    "get_config",
    "reset_config",
    "ensure_tables",
    # Fire API
    "warn",
    "error",
    "critical",
    # Direct store access
    "fire_alert",
    # Query API
    "query_categories",
    "query_messages",
    "query_occurrences",
    "query_by_trace_id",
    "query_aggregates",
    "query_next_fix",
    # Delete API
    "delete_by_category",
    "delete_by_id",
    # Sweeper entry points
    "deliver_alerts",
    "cleanup_alerts",
    # Transport
    "Transport",
    "CallableTransport",
    "LogTransport",
    "WebhookTransport",
    # Model (for Alembic integration)
    "Alert",
    "OpsAlertBase",
    # Types
    "AlertSeverity",
    "AlertMessage",
    "IMMEDIATE_SEVERITIES",
    "DIGEST_SEVERITIES",
]
