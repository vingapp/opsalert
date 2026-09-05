"""Dispatch — fire-and-forget alert creation via ingest queue.

Enriches the context, builds an Event with the identity header, and enqueues
it on the bounded in-process queue. One daemon thread writes batches to the
DB (see :mod:`opsalert.ingest`).

No event loop interaction. No session factory call. No DB access on the
caller thread. The call shape is unchanged: ``warn/error/critical(category,
message=, source=, context=)``.
"""
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from opsalert._config import get_config
from opsalert._enrichment import enrich_context
from opsalert.signature import condition_signature, normalize_message, render_template
from opsalert.store import stamp_environment

logger = logging.getLogger(__name__)


def _fire_sync(
    severity: str,
    category: str,
    message: str,
    source: str | None,
    context: dict[str, Any] | None,
    params: dict[str, Any] | None = None,
) -> None:
    """Fire an alert from any context (sync or async).

    Builds an Event and enqueues it. Never raises — all failures are logged,
    caller unaffected.

    No-ops when:
    - testing mode is enabled (alerts would leak outside test transaction)
    - configure() hasn't been called (e.g. test suite without startup)
    """
    try:
        cfg = get_config()
    except RuntimeError:
        # Not configured — silently skip rather than disrupting caller
        return
    if cfg.testing:
        return

    context = enrich_context(context)
    stamped = stamp_environment(context)

    # Compute the identity header
    rendered = render_template(message, params)
    template = message if params else normalize_message(message)
    environment = (stamped or {}).get("environment")

    sig_key = condition_signature(category, source, environment, template)

    from opsalert.ingest import Event, enqueue

    event = Event(
        event_id=uuid.uuid4().hex,
        ts=datetime.now(UTC),
        severity=severity,
        category=category,
        message=rendered,
        source=source,
        context=stamped,
        params=params,
        template=template,
        environment=environment,
        signature_key=sig_key,
    )

    enqueue(event)


def warn(
    category: str,
    *,
    message: str,
    source: str | None = None,
    context: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> None:
    """Fire a WARN alert. For unexpected but non-breaking issues."""
    from opsalert.types import AlertSeverity

    _fire_sync(AlertSeverity.WARN, category, message, source, context, params)


def error(
    category: str,
    *,
    message: str,
    source: str | None = None,
    context: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> None:
    """Fire an ERROR alert. For something that failed that shouldn't have."""
    from opsalert.types import AlertSeverity

    _fire_sync(AlertSeverity.ERROR, category, message, source, context, params)


def critical(
    category: str,
    *,
    message: str,
    source: str | None = None,
    context: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> None:
    """Fire a CRITICAL alert. For infrastructure-level problems."""
    from opsalert.types import AlertSeverity

    _fire_sync(AlertSeverity.CRITICAL, category, message, source, context, params)
