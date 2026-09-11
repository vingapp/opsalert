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

    # Structured emission: the message is a template, params are its values,
    # and the template is the alert condition's identity.
    opsalert.error(
        "request_anomaly",
        message="{method} {route} exceeded its budget",
        params={"method": "PUT", "route": "/api/view/shares/{stub}/"},
    )
"""
from opsalert._config import configure, get_config, reset_config
from opsalert._dispatch import critical, error, warn
from opsalert.cleanup import cleanup_alerts
from opsalert.delivery import deliver_alerts
from opsalert.ingest import flush
from opsalert.lifecycle import (
    apply_lifecycle_rules,
    effective_disposition,
    set_disposition,
    set_status,
    sync_condition_stats,
)
from opsalert.model import Alert, AlertCondition, OpsAlertBase
from opsalert.query import (
    delete_batch,
    delete_by_category,
    delete_by_id,
    query_aggregates,
    query_attention,
    query_by_trace_id,
    query_categories,
    query_conditions,
    query_messages,
    query_next_fix,
    query_occurrences,
)
from opsalert.signature import condition_signature, normalize_message
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
    "flush",
    # Direct store access
    "fire_alert",
    # Query API
    "query_categories",
    "query_messages",
    "query_occurrences",
    "query_by_trace_id",
    "query_aggregates",
    "query_next_fix",
    "query_conditions",
    "query_attention",
    # Delete API
    "delete_batch",
    "delete_by_category",
    "delete_by_id",
    # Sweeper entry points
    "deliver_alerts",
    "cleanup_alerts",
    "sync_condition_stats",
    "apply_lifecycle_rules",
    # Condition lifecycle
    "set_status",
    "set_disposition",
    "effective_disposition",
    "condition_signature",
    "normalize_message",
    # Transport
    "Transport",
    "CallableTransport",
    "LogTransport",
    "WebhookTransport",
    # Model (for Alembic integration)
    "Alert",
    "AlertCondition",
    "OpsAlertBase",
    # Types
    "AlertSeverity",
    "AlertMessage",
    "IMMEDIATE_SEVERITIES",
    "DIGEST_SEVERITIES",
]
