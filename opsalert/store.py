"""Store — create one alert row per occurrence.

Every call creates a new Alert record. No deduplication at the data layer:
occurrences are facts, and grouping happens above them — at query time via
``category``/``message``, and structurally via the occurrence's
:class:`~opsalert.model.AlertCondition`.

Condition resolution is best-effort by construction. It runs in its own
short-lived transaction where one is available, and degrades to a NULL
``condition_id`` on any failure. An occurrence is never lost, and the caller
never sees an exception, because the alert row is the record of a problem that
already happened — losing it to bookkeeping would be the worst possible trade.
"""
import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy import select, text

from opsalert.model import Alert, AlertCondition
from opsalert.signature import condition_signature, normalize_message, render_template

logger = logging.getLogger(__name__)

# Reserved context key carrying the emit-time template on the occurrence row.
# A params emission's identity is the raw template, and the occurrence stores
# only the RENDERED text — so when condition resolution degrades (F1), the
# adoption sweeper would otherwise have to guess the template back out of the
# rendered message, guess differently, and fork a second condition
# (opsalert#2). Rides ``context_json`` the same way ``_trace_id`` does.
TEMPLATE_CONTEXT_KEY = "_message_template"

# ``Alert.context_json`` is MySQL TEXT — 65535 *bytes*, not characters. An
# oversized context used to raise DataError 1406 mid-flush, which loses the
# whole alert: the record explaining what went wrong is dropped precisely when
# the failure was big enough to produce a huge context. Cap it instead, so a fat
# context costs detail and never the alert.
CONTEXT_MAX_BYTES = 60_000  # headroom under TEXT for the truncation markers
# Long values are cut to this before being replaced wholesale, so a truncated
# stack trace still shows where it started.
_VALUE_PREVIEW_BYTES = 2_000
# Fallback for a context that is mostly structure: how many key names to keep,
# and how long each may be. The key list has to fit the column as well.
_KEY_SAMPLE = 200
_KEY_PREVIEW_BYTES = 100


def _encoded_len(payload: str) -> int:
    return len(payload.encode("utf-8"))


def _truncate_str(value: str, limit: int) -> str:
    """Cut ``value`` to ``limit`` bytes without splitting a UTF-8 sequence."""
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value
    return encoded[:limit].decode("utf-8", errors="ignore")


def serialize_context(context: dict[str, Any] | None) -> str | None:
    """JSON-encode an alert context, capped to fit ``Alert.context_json``.

    Under the cap the context round-trips byte for byte. Over it, the biggest
    values are cut down (largest first) until the payload fits, and the result
    carries ``_truncated`` — the keys that lost data — plus ``_original_bytes``
    so a reader can tell how much was dropped. If shrinking values still isn't
    enough (a context that is mostly structure rather than a few long strings),
    fall back to a marker object listing the keys that were present.
    """
    if not context:
        return None

    serialized = json.dumps(context)
    original_bytes = _encoded_len(serialized)
    if original_bytes <= CONTEXT_MAX_BYTES:
        return serialized

    # Size every value once, then cut the oversized ones down in one pass —
    # largest first, stopping as soon as the running total fits. Re-dumping the
    # whole dict per candidate would be quadratic, and a context big enough to
    # land here is exactly the one that can carry thousands of keys.
    sizes = {key: _encoded_len(json.dumps(value, default=str)) for key, value in context.items()}
    capped: dict[str, Any] = dict(context)
    truncated_keys: list[str] = []
    running = original_bytes

    for key in sorted(sizes, key=lambda k: sizes[k], reverse=True):
        if running <= _budget(truncated_keys, original_bytes):
            break
        if sizes[key] <= _VALUE_PREVIEW_BYTES:
            break  # nothing bigger left to reclaim
        value = capped[key]
        if not isinstance(value, str):
            value = json.dumps(value, default=str)
        capped[key] = _truncate_str(value, _VALUE_PREVIEW_BYTES)
        truncated_keys.append(key)
        running -= sizes[key] - _encoded_len(json.dumps(capped[key]))

    if truncated_keys:
        candidate = json.dumps(
            {**capped, "_truncated": truncated_keys, "_original_bytes": original_bytes}
        )
        if _encoded_len(candidate) <= CONTEXT_MAX_BYTES:
            logger.warning(
                "opsalert: context exceeded %d bytes (%d); truncated keys %s",
                CONTEXT_MAX_BYTES,
                original_bytes,
                truncated_keys,
            )
            return candidate

    # Still too big with every long value cut down — the bulk is structure, not
    # a few fat strings. Keep the shape (a bounded sample of keys) and drop the
    # data; the key list itself has to fit the column too.
    keys = sorted(context)
    sample = keys[:_KEY_SAMPLE]
    logger.warning(
        "opsalert: context exceeded %d bytes (%d) and could not be shrunk by "
        "value; storing key sample only (%d keys)",
        CONTEXT_MAX_BYTES,
        original_bytes,
        len(keys),
    )
    return json.dumps(
        {
            "_truncated": [_truncate_str(k, _KEY_PREVIEW_BYTES) for k in sample],
            "_key_count": len(keys),
            "_original_bytes": original_bytes,
            "_dropped": True,
        }
    )


def _budget(truncated_keys: list[str], original_bytes: int) -> int:
    """Byte ceiling for the capped values, leaving room for the markers."""
    marker_bytes = _encoded_len(json.dumps(truncated_keys)) + len(str(original_bytes)) + 64
    return CONTEXT_MAX_BYTES - marker_bytes


def stamp_environment(context: dict[str, Any] | None) -> dict[str, Any] | None:
    """Add the configured deployment environment to an alert context.

    Every stored occurrence has to say which deployment produced it — reading a
    triage row and not knowing whether it came from staging or production makes
    the row nearly useless. Stamped here, on the single store path, so direct
    ``fire_alert`` callers get it as well as ``opsalert.error(...)``.

    A caller-supplied ``environment`` key wins: the caller knows something we
    don't (e.g. relaying an alert on another deployment's behalf). Returns the
    context untouched when no environment is configured, or when opsalert has
    not been configured at all — storing must never depend on config.
    """
    try:
        from opsalert._config import get_config

        environment = get_config().environment
    except (RuntimeError, ImportError):
        # Not configured at all — storing must never depend on config. Any
        # other failure is a real bug and is left to surface.
        return context
    if not environment:
        return context
    if context and "environment" in context:
        return context
    stamped = dict(context) if context else {}
    stamped["environment"] = environment
    return stamped


def _dialect_name(session: "AsyncSession") -> str:
    """Dialect behind a session, or ``""`` when it cannot be determined."""
    try:
        return session.get_bind().dialect.name
    except Exception:
        logger.exception("opsalert: could not determine the session's dialect")
        return ""


async def _upsert_condition(session: "AsyncSession", values: dict[str, Any]) -> int | None:
    """Insert the condition if absent, return its id either way.

    Dialect-dispatched because "insert or tell me the existing id" has no
    portable spelling and the near-misses are all wrong:

    - MySQL ``ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)`` is the one
      construct that makes ``lastrowid`` report the EXISTING row's id on the
      duplicate path.
    - SQLite ``ON CONFLICT DO UPDATE ... RETURNING id`` — never DO NOTHING,
      which returns no row on conflict and leaves ``lastrowid`` pointing at
      whatever was inserted last.

    Anything else falls back to insert-then-select, which is correct if
    slightly chattier.
    """
    dialect = _dialect_name(session)

    if dialect == "mysql":
        result = await session.execute(upsert_statement("mysql", values))
        return result.lastrowid

    if dialect == "sqlite":
        result = await session.execute(upsert_statement("sqlite", values))
        return result.scalar_one()

    from sqlalchemy.exc import IntegrityError

    try:
        result = await session.execute(AlertCondition.__table__.insert().values(**values))
        return result.inserted_primary_key[0]
    except IntegrityError:
        return await session.scalar(
            select(AlertCondition.id).where(
                AlertCondition.signature_key == values["signature_key"]
            )
        )


def upsert_statement(dialect: str, values: dict[str, Any]):
    """Build the dialect's insert-or-return-existing statement.

    Split out from execution so both branches can be verified without the
    database they target: a MySQL-only construct that is never compiled in CI
    is a bug waiting for production.
    """
    if dialect == "mysql":
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        return (
            mysql_insert(AlertCondition)
            .values(**values)
            .on_duplicate_key_update(id=text("LAST_INSERT_ID(id)"))
        )

    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    return (
        sqlite_insert(AlertCondition)
        .values(**values)
        .on_conflict_do_update(
            index_elements=[AlertCondition.signature_key],
            set_={"updated": datetime.now(UTC)},
        )
        .returning(AlertCondition.id)
    )


async def _lookup_or_create(
    session: "AsyncSession", *, signature_key: str, values: dict[str, Any]
) -> int | None:
    """SELECT first, upsert only on a miss.

    The SELECT is not an optimisation, it is the point: the overwhelmingly
    common case is a condition that already exists, and the upsert — which
    takes a row lock even when it writes nothing — must not be on that path.
    """
    existing = await session.scalar(
        select(AlertCondition.id).where(AlertCondition.signature_key == signature_key)
    )
    if existing is not None:
        return existing
    return await _upsert_condition(session, values)


async def resolve_condition_id(
    session: "AsyncSession",
    *,
    category: str,
    source: str | None,
    environment: str | None,
    template: str,
    severity: str,
) -> int | None:
    """Return the condition id for this signature, or ``None`` if anything fails.

    Transaction isolation (P1): resolution runs in a SHORT-LIVED SESSION of its
    own whenever the config provides a session factory, so the row lock the
    upsert takes is held for that statement pair and nothing else — never
    across the caller's work, which may be an arbitrarily long request or task
    transaction. A deadlock there kills the resolution, not the caller.

    When no escape hatch exists (no ``session_factory`` configured, or a SQLite
    engine whose "separate" session is the same single connection and would
    commit the caller's open transaction along with its own), resolution falls
    back to the caller's session wrapped in ``begin_nested()``. Every statement
    is inside the SAVEPOINT, so a failure rolls back to the savepoint and the
    caller's transaction stays usable.

    Both paths degrade the same way: ANY failure returns None and the
    occurrence is stored as an orphan for the sweeper to adopt (F1).
    """
    signature_key = condition_signature(category, source, environment, template)
    now = datetime.now(UTC)
    values = {
        "signature_key": signature_key,
        "category": category,
        "source": source,
        "environment": environment,
        "message_template": template[:500],
        "status": "new",
        "severity": severity,
        "latest_severity": severity,
        "status_changed_at": now,
        # first_seen is stamped at creation, not left NULL until the next stats
        # sweep (opsalert#4). A condition is created BY a fire, so "first seen"
        # is now; a reader meeting the row in the gap before the sweep would
        # otherwise see a condition that has apparently never happened. This is
        # safe against the sweep, which only ever moves first_seen EARLIER
        # (lifecycle._apply_counts) — so a backdated or pre-existing occurrence
        # still corrects it. last_seen is deliberately NOT stamped: the sweep
        # only moves it LATER, so a value from creation would mask the true
        # time of an occurrence that is older than the row.
        "first_seen": now,
        "created": now,
        "updated": now,
    }

    factory = None
    try:
        from opsalert._config import get_config

        factory = get_config().session_factory
    except (RuntimeError, ImportError):
        # Unconfigured: there is no isolated session to escape to, so the
        # SAVEPOINT path below handles it.
        factory = None

    if factory is not None and _dialect_name(session) != "sqlite":
        try:
            async with factory() as isolated:
                condition_id = await _lookup_or_create(
                    isolated, signature_key=signature_key, values=values
                )
                await isolated.commit()
                return condition_id
        except Exception:
            logger.exception(
                "opsalert: isolated condition resolution failed for category=%s", category
            )
            return None

    try:
        async with session.begin_nested():
            return await _lookup_or_create(session, signature_key=signature_key, values=values)
    except Exception:
        logger.exception(
            "opsalert: condition resolution failed for category=%s; storing occurrence "
            "as an orphan for the sweeper to adopt",
            category,
        )
        return None


async def fire_alert(
    session: "AsyncSession",
    *,
    severity: str,
    category: str,
    message: str,
    source: str | None = None,
    context: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    kind: str | None = None,
    exc: BaseException | None = None,
) -> Alert:
    """Create an alert record. Every call creates one row.

    ``params`` makes the emission structured: ``message`` is then a TEMPLATE
    (``"PUT {route} slow"``), the stored occurrence message is the rendered
    text, and the raw template is the condition's identity — exact, rather
    than guessed by the normalizer. Without ``params`` the message is stored
    as given and identity falls back to :func:`normalize_message`.

    ``kind`` and ``exc`` enable v2 identity through the same functions as
    the dispatch path.
    """
    stamped = stamp_environment(context)
    rendered = render_template(message, params)
    template = message if params else normalize_message(message)
    if params:
        stamped = {**(stamped or {}), TEMPLATE_CONTEXT_KEY: template}

    # Every new row is v2.  kind=None triggers the same legacy fallback
    # as _dispatch: kind = f"{category}.legacy" with the template in the
    # fingerprint parts so two messages that v1 kept separate stay separate.
    from opsalert.signature import (
        build_exception_chain,
        event_fingerprint_parts,
        event_signature,
        extract_origin_frame,
    )

    actual_kind = kind if kind is not None else f"{category}.legacy"
    is_legacy = kind is None

    try:
        from opsalert._config import get_config
        cfg = get_config()
        in_app_prefixes = cfg.in_app_prefixes
    except (RuntimeError, ImportError):
        in_app_prefixes = ()

    exception_chain = build_exception_chain(exc)
    origin_frame = extract_origin_frame(exc, in_app_prefixes=in_app_prefixes)

    template_for_fp = template if is_legacy else None
    fp_parts = event_fingerprint_parts(
        kind=actual_kind,
        environment=(stamped or {}).get("environment"),
        exception_chain=exception_chain,
        origin_frame=origin_frame,
        template=template_for_fp,
    )
    sig_key = event_signature(
        kind=actual_kind,
        environment=(stamped or {}).get("environment"),
        exception_chain=exception_chain,
        origin_frame=origin_frame,
        template=template_for_fp,
    )

    condition_id = await _resolve_v2_condition(
        session,
        signature_key=sig_key,
        category=category,
        source=source,
        environment=(stamped or {}).get("environment"),
        kind=actual_kind,
        is_legacy=is_legacy,
        template=template,
        fingerprint_version=2,
        fingerprint_json=json.dumps(fp_parts),
        message_example=(rendered or "")[:500],
        severity=str(severity),
    )

    alert = Alert(
        severity=severity,
        category=category,
        message=rendered,
        source=source,
        context_json=serialize_context(stamped),
        condition_id=condition_id,
        kind=actual_kind,
        fingerprint_version=2,
        fingerprint_json=json.dumps(fp_parts),
    )
    session.add(alert)
    await session.flush()
    return alert


async def _resolve_v2_condition(
    session: "AsyncSession",
    *,
    signature_key: str,
    category: str,
    source: str | None,
    environment: str | None,
    kind: str,
    is_legacy: bool = False,
    template: str = "",
    fingerprint_version: int,
    fingerprint_json: str | None,
    message_example: str,
    severity: str,
) -> int | None:
    """Resolve a v2 condition (with kind)."""
    now = datetime.now(UTC)
    # Explicit kind: message_template = kind (search matches it).
    # Legacy fallback: message_template = normalized message template.
    msg_template = template[:500] if is_legacy else kind[:500]
    values = {
        "signature_key": signature_key,
        "category": category,
        "source": source,
        "environment": environment,
        "message_template": msg_template,
        "status": "new",
        "severity": severity,
        "latest_severity": severity,
        "status_changed_at": now,
        "first_seen": now,
        "created": now,
        "updated": now,
        "kind": kind,
        "fingerprint_version": fingerprint_version,
        "fingerprint_json": fingerprint_json,
        "message_example": message_example,
    }

    factory = None
    try:
        from opsalert._config import get_config
        factory = get_config().session_factory
    except (RuntimeError, ImportError):
        factory = None

    if factory is not None and _dialect_name(session) != "sqlite":
        try:
            async with factory() as isolated:
                condition_id = await _lookup_or_create(
                    isolated, signature_key=signature_key, values=values
                )
                await isolated.commit()
                return condition_id
        except Exception:
            logger.exception(
                "opsalert: isolated v2 condition resolution failed for kind=%s", kind
            )
            return None

    try:
        async with session.begin_nested():
            return await _lookup_or_create(
                session, signature_key=signature_key, values=values
            )
    except Exception:
        logger.exception(
            "opsalert: v2 condition resolution failed for kind=%s", kind
        )
        return None
