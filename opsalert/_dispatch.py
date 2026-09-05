"""Dispatch — fire-and-forget alert creation via ingest queue.

Enriches the context, builds an Event with the identity header, and enqueues
it on the bounded in-process queue. One daemon thread writes batches to the
DB (see :mod:`opsalert.ingest`).

No event loop interaction. No session factory call. No DB access on the
caller thread. The call shape is unchanged: ``warn/error/critical(category,
message=, source=, context=)``.
"""
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from opsalert._config import get_config
from opsalert._enrichment import enrich_context
from opsalert.signature import (
    event_fingerprint_parts,
    event_signature,
    normalize_message,
    render_template,
    validate_kind,
)
from opsalert.store import stamp_environment

logger = logging.getLogger(__name__)

# Track invalid kinds that have been warned about: (emit_site, kind) -> True
_invalid_kind_warned: set[tuple[str, str]] = set()


def _extract_subjects(context: dict[str, Any] | None) -> list[tuple[str, str]]:
    """Extract subject tuples from enriched context.

    Priority: user_id -> ("user", str(id)); else _identifier_hash ->
    ("ident", hash); else _session_id -> ("session", id).
    """
    if not context:
        return []
    user_id = context.get("_user_id")
    if user_id is not None:
        return [("user", str(user_id))]
    ident_hash = context.get("_identifier_hash")
    if ident_hash is not None:
        return [("ident", str(ident_hash))]
    session_id = context.get("_session_id")
    if session_id is not None:
        return [("session", str(session_id))]
    return []


def _fire_sync(
    severity: str,
    category: str,
    message: str,
    source: str | None,
    context: dict[str, Any] | None,
    params: dict[str, Any] | None = None,
    kind: str | None = None,
    exc: BaseException | None = None,
    stacklevel: int = 1,
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

    # --- Kind validation ---
    if kind is not None and not validate_kind(kind):
        if cfg.testing:
            raise ValueError(
                f"Invalid kind {kind!r}: must match ^[a-z0-9_]+(\\.[a-z0-9_]+)+$"
            )
        # Production: replace with legacy fallback, warn once per (site, kind)
        emit_site_key = ""
        try:
            from opsalert._enrichment import compute_emit_site
            emit_site_key = compute_emit_site(stacklevel=stacklevel)
        except Exception:
            pass
        warn_key = (emit_site_key, kind)
        if warn_key not in _invalid_kind_warned:
            _invalid_kind_warned.add(warn_key)
            logger.warning(
                "opsalert: invalid kind %r from %s; using legacy fallback",
                kind,
                emit_site_key,
            )
        kind = None  # Fall through to legacy

    if cfg.testing:
        return

    # Enrichment — pass exc and stacklevel through
    context = enrich_context(context, exc=exc, stacklevel=stacklevel)
    stamped = stamp_environment(context)

    # Compute the identity header
    rendered = render_template(message, params)
    template = message if params else normalize_message(message)
    environment = (stamped or {}).get("environment")

    # Resolve the actual kind — None means legacy fallback
    actual_kind = kind
    if actual_kind is None:
        actual_kind = f"{category}.legacy"

    # Build the exception chain and origin frame from the resolved exception
    from opsalert.signature import build_exception_chain, extract_origin_frame

    resolved_exc = exc
    if resolved_exc is None:
        import sys
        exc_info = sys.exc_info()
        resolved_exc = exc_info[1] if exc_info else None

    exception_chain = build_exception_chain(resolved_exc)
    in_app_prefixes = cfg.in_app_prefixes
    origin_frame = extract_origin_frame(resolved_exc, in_app_prefixes=in_app_prefixes)

    # V2 fingerprint
    is_legacy = kind is None  # original kind was None
    template_for_fp = template if is_legacy else None
    sig_key = event_signature(
        kind=actual_kind,
        environment=environment,
        exception_chain=exception_chain,
        origin_frame=origin_frame,
        template=template_for_fp,
    )
    fp_parts = event_fingerprint_parts(
        kind=actual_kind,
        environment=environment,
        exception_chain=exception_chain,
        origin_frame=origin_frame,
        template=template_for_fp,
    )
    fingerprint_json = json.dumps(fp_parts)

    # Extract v2 fields from enriched context
    ctx = stamped or {}
    emit_site = ctx.get("_emit_site", "")
    exception_class = ctx.get("_exc_type")
    trace_id = ctx.get("_trace_id")
    span_id = ctx.get("_span_id")
    user_id_val = ctx.get("_user_id")
    org_id_val = ctx.get("_org_id")
    release_val = ctx.get("_release") or cfg.release

    # Parse user_id/org_id as int if possible
    user_id_int = None
    if user_id_val is not None:
        try:
            user_id_int = int(user_id_val)
        except (ValueError, TypeError):
            pass
    org_id_int = None
    if org_id_val is not None:
        try:
            org_id_int = int(org_id_val)
        except (ValueError, TypeError):
            pass

    # Subjects
    subjects = _extract_subjects(ctx)

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
        # v2 fields
        kind=actual_kind,
        fingerprint_version=2,
        fingerprint_json=fingerprint_json,
        emit_site=emit_site,
        exception_class=exception_class,
        trace_id=str(trace_id)[:32] if trace_id else None,
        span_id=str(span_id)[:16] if span_id else None,
        user_id=user_id_int,
        org_id=org_id_int,
        release=str(release_val)[:40] if release_val else None,
        subjects=subjects,
    )

    enqueue(event)


def warn(
    category: str,
    *,
    message: str,
    kind: str | None = None,
    exc: BaseException | None = None,
    stacklevel: int = 1,
    source: str | None = None,
    context: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> None:
    """Fire a WARN alert. For unexpected but non-breaking issues."""
    from opsalert.types import AlertSeverity

    _fire_sync(
        AlertSeverity.WARN, category, message, source, context, params,
        kind=kind, exc=exc, stacklevel=stacklevel,
    )


def error(
    category: str,
    *,
    message: str,
    kind: str | None = None,
    exc: BaseException | None = None,
    stacklevel: int = 1,
    source: str | None = None,
    context: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> None:
    """Fire an ERROR alert. For something that failed that shouldn't have."""
    from opsalert.types import AlertSeverity

    _fire_sync(
        AlertSeverity.ERROR, category, message, source, context, params,
        kind=kind, exc=exc, stacklevel=stacklevel,
    )


def critical(
    category: str,
    *,
    message: str,
    kind: str | None = None,
    exc: BaseException | None = None,
    stacklevel: int = 1,
    source: str | None = None,
    context: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> None:
    """Fire a CRITICAL alert. For infrastructure-level problems."""
    from opsalert.types import AlertSeverity

    _fire_sync(
        AlertSeverity.CRITICAL, category, message, source, context, params,
        kind=kind, exc=exc, stacklevel=stacklevel,
    )
