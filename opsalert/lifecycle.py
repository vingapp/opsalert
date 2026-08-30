"""Condition lifecycle — statistics, automatic rules, and human transitions.

Three jobs live here:

1. :func:`sync_condition_stats` folds occurrences into their condition's
   counters, so the counters survive the occurrences (which are pruned).
2. :func:`apply_lifecycle_rules` runs the automatic transitions: reopen,
   auto-close of a resolved condition that has gone quiet, auto-stale of a
   condition nobody ever triaged and that stopped happening.
3. :func:`set_status` / :func:`set_disposition` are the human edits, with
   transition validation and audit stamps.

Plain async functions, no scheduler dependency — the host app wires them into
whatever sweeper it runs.
"""
import json
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import case, func, select, update

from opsalert.model import Alert, AlertCondition
from opsalert.signature import condition_signature, normalize_message
from opsalert.store import _lookup_or_create
from opsalert.types import AlertSeverity

logger = logging.getLogger(__name__)

STATUS_NEW = "new"
STATUS_ACKNOWLEDGED = "acknowledged"
STATUS_RESOLVED = "resolved"
STATUS_CLOSED = "closed"
STATUSES = (STATUS_NEW, STATUS_ACKNOWLEDGED, STATUS_RESOLVED, STATUS_CLOSED)

DISPOSITION_IMMEDIATE = "immediate"
DISPOSITION_DIGEST = "digest"
DISPOSITION_COLLECT = "collect"
DISPOSITIONS = (DISPOSITION_IMMEDIATE, DISPOSITION_DIGEST, DISPOSITION_COLLECT)

# Every transition a human may ask for. Anything absent is rejected loudly
# rather than silently applied: "closed → resolved" is almost always someone
# operating on the wrong row, and a lifecycle that accepts nonsense stops
# being evidence of anything.
ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    STATUS_NEW: frozenset({STATUS_ACKNOWLEDGED, STATUS_RESOLVED, STATUS_CLOSED}),
    STATUS_ACKNOWLEDGED: frozenset({STATUS_NEW, STATUS_RESOLVED, STATUS_CLOSED}),
    STATUS_RESOLVED: frozenset({STATUS_NEW, STATUS_ACKNOWLEDGED, STATUS_CLOSED}),
    STATUS_CLOSED: frozenset({STATUS_NEW, STATUS_ACKNOWLEDGED}),
}

# Occurrences are counted only once they are at least this old. Auto-increment
# order is NOT commit order: a row with a lower id can become visible after a
# row with a higher id, and a watermark that raced ahead of it would skip it
# forever — and, worse, would license cleanup to delete it as "counted".
STATS_LAG_SECONDS = 60

# A resolved condition closes after this much silence at minimum, even when it
# used to fire every few seconds. Ten times the median inter-arrival is the
# real rule; the floor stops a chatty condition from closing during a lunch
# break, and covers the degenerate case (fewer than two occurrences → no
# median at all).
AUTO_CLOSE_FLOOR = timedelta(hours=6)
AUTO_CLOSE_MEDIAN_MULTIPLE = 10
# A condition nobody ever triaged, silent this long, is garbage-collected.
# Mis-collection is self-correcting: a recurrence reopens it (P4).
AUTO_STALE_SILENCE = timedelta(days=30)

# How many recent inter-arrival gaps feed the median.
MEDIAN_SAMPLE = 50

_SEVERITY_ORDER = {
    AlertSeverity.WARN.value: 1,
    AlertSeverity.ERROR.value: 2,
    AlertSeverity.CRITICAL.value: 3,
}
_RANK_TO_SEVERITY = {rank: sev for sev, rank in _SEVERITY_ORDER.items()}

_ALERT_SEVERITY_RANK = case(
    (Alert.severity == AlertSeverity.CRITICAL.value, 3),
    (Alert.severity == AlertSeverity.ERROR.value, 2),
    (Alert.severity == AlertSeverity.WARN.value, 1),
    else_=0,
)

# Severities that, absent an explicit owner override, wake someone up.
_IMMEDIATE_BY_DEFAULT = frozenset({AlertSeverity.ERROR.value, AlertSeverity.CRITICAL.value})


def worst_severity(a: str | None, b: str | None) -> str:
    """The more severe of two severity strings (unknown values rank lowest)."""
    return a if _SEVERITY_ORDER.get(a or "", 0) >= _SEVERITY_ORDER.get(b or "", 0) else b


def effective_disposition(severity: str | None, disposition: str | None) -> str:
    """How loudly this condition should be delivered.

    An explicit owner override always wins. Otherwise it derives from the
    condition's worst-seen severity — error/critical wake someone, warn goes
    into the digest — which is exactly the routing occurrences had before
    conditions existed.
    """
    if disposition in DISPOSITIONS:
        return disposition
    return DISPOSITION_IMMEDIATE if severity in _IMMEDIATE_BY_DEFAULT else DISPOSITION_DIGEST


def _naive(value: datetime | None) -> datetime | None:
    """Drop tzinfo for comparison — some drivers hand back naive UTC."""
    if value is None:
        return None
    return value.replace(tzinfo=None)


def reopen_condition(condition: AlertCondition, *, now: datetime | None = None) -> None:
    """Bring a resolved/closed condition back to ``new``.

    Shared by the delivery path (which reopens INLINE, before it decides
    whether to email — see P4) and by the lifecycle sweep's belt-and-braces
    scan. Clearing the acknowledgement stamps matters: the person who
    acknowledged the old episode has not seen this one.
    """
    now = now or datetime.now(UTC)
    condition.status = STATUS_NEW
    condition.reopened_count = (condition.reopened_count or 0) + 1
    condition.status_changed_at = now
    condition.resolved_at = None
    condition.closed_at = None
    condition.acknowledged_at = None
    condition.acknowledged_by = None
    logger.warning(
        "opsalert: condition %s (%s) reopened — it fired again after being closed out",
        condition.id,
        condition.category,
    )


# =============================================================================
# Statistics
# =============================================================================


async def sync_condition_stats(session, *, now: datetime | None = None) -> dict:
    """Fold new occurrences into their conditions' counters.

    Adoption runs first so an orphan created before the last sweep is counted
    in the same pass it is linked. Then the watermark scan folds every
    occurrence above ``stats_synced_through`` that is old enough to be
    trusted (see :data:`STATS_LAG_SECONDS`).

    Returns ``{"adopted": n, "conditions_updated": n, "occurrences_counted": n}``.
    """
    now = now or datetime.now(UTC)
    horizon = now - timedelta(seconds=STATS_LAG_SECONDS)

    adopted = await _adopt_orphans(session, now=now)
    updated, counted = await _fold_new_occurrences(session, horizon=horizon)

    if adopted or updated:
        logger.info(
            "opsalert: condition stats — adopted %d orphan(s), updated %d condition(s), "
            "counted %d occurrence(s)",
            adopted,
            updated,
            counted,
        )
    return {
        "adopted": adopted,
        "conditions_updated": updated,
        "occurrences_counted": counted,
    }


async def _adopt_orphans(session, *, now: datetime, batch_size: int = 500) -> int:
    """Link every occurrence with a NULL ``condition_id`` to a condition.

    The scan is UNBOUNDED by design (P2) — it walks the whole orphan set by
    ascending id, in memory-bounded batches, and is deliberately NOT gated by
    the stats watermark. An orphan is an occurrence that already lost its
    condition once; gating the repair on the same bookkeeping that failed
    would make the loss permanent.
    """
    adopted = 0
    last_id = 0

    while True:
        rows = (
            await session.execute(
                select(
                    Alert.id,
                    Alert.category,
                    Alert.source,
                    Alert.message,
                    Alert.severity,
                    Alert.context_json,
                )
                .where(Alert.condition_id.is_(None), Alert.id > last_id)
                .order_by(Alert.id)
                .limit(batch_size)
            )
        ).all()
        if not rows:
            break

        batch_ids: list[int] = []
        for row in rows:
            last_id = row.id
            try:
                environment = _environment_from_context(row.context_json)
                template = normalize_message(row.message or "")
                signature_key = condition_signature(
                    row.category, row.source, environment, template
                )
                condition_id = await _lookup_or_create(
                    session,
                    signature_key=signature_key,
                    values={
                        "signature_key": signature_key,
                        "category": row.category,
                        "source": row.source,
                        "environment": environment,
                        "message_template": template[:500],
                        "status": STATUS_NEW,
                        "severity": row.severity,
                        "latest_severity": row.severity,
                        "status_changed_at": now,
                        "created": now,
                        "updated": now,
                    },
                )
            except Exception:
                logger.exception("opsalert: could not adopt orphan occurrence %s", row.id)
                continue
            if condition_id is None:
                continue
            await session.execute(
                update(Alert).where(Alert.id == row.id).values(condition_id=condition_id)
            )
            batch_ids.append(row.id)
            adopted += 1

        # An adopted row whose id is at or below its condition's watermark can
        # never be picked up by the watermark scan, so count it here or its
        # occurrence is invisible in the counters forever.
        if batch_ids:
            await _count_below_watermark(session, batch_ids)

    return adopted


def _environment_from_context(context_json: str | None) -> str | None:
    """Read ``environment`` out of a stored context, tolerating anything."""
    if not context_json:
        return None
    try:
        value = json.loads(context_json).get("environment")
    except (ValueError, TypeError, AttributeError):
        # A context we cannot parse is a context with no environment: the
        # occurrence lands in the unlabelled bucket rather than being skipped.
        logger.warning("opsalert: unparseable context on an orphan occurrence")
        return None
    return value if isinstance(value, str) else None


async def _count_below_watermark(session, occurrence_ids: list[int]) -> None:
    """Fold already-watermarked occurrences into their condition's counters."""
    rows = (
        await session.execute(
            select(
                Alert.condition_id.label("cid"),
                func.count(Alert.id).label("n"),
                func.min(Alert.created).label("first_created"),
                func.max(Alert.created).label("last_created"),
                func.max(_ALERT_SEVERITY_RANK).label("severity_rank"),
            )
            .join(AlertCondition, AlertCondition.id == Alert.condition_id)
            .where(
                Alert.id.in_(occurrence_ids),
                Alert.id <= AlertCondition.stats_synced_through,
            )
            .group_by(Alert.condition_id)
        )
    ).all()

    for row in rows:
        condition = await session.get(AlertCondition, row.cid)
        if condition is None:
            continue
        _apply_counts(
            condition,
            count=row.n,
            first_created=row.first_created,
            last_created=row.last_created,
            severity_rank=row.severity_rank,
        )


def _apply_counts(
    condition: AlertCondition,
    *,
    count: int,
    first_created: datetime | None,
    last_created: datetime | None,
    severity_rank: int | None,
) -> None:
    """Merge a batch of counted occurrences into a condition's counters."""
    condition.occurrence_count = (condition.occurrence_count or 0) + count
    if first_created is not None:
        current = _naive(condition.first_seen)
        if current is None or _naive(first_created) < current:
            condition.first_seen = first_created
    if last_created is not None:
        current = _naive(condition.last_seen)
        if current is None or _naive(last_created) > current:
            condition.last_seen = last_created
    scanned = _RANK_TO_SEVERITY.get(severity_rank or 0)
    if scanned:
        condition.severity = worst_severity(condition.severity, scanned)


async def _fold_new_occurrences(session, *, horizon: datetime) -> tuple[int, int]:
    """Watermark scan: count occurrences above each condition's watermark."""
    rows = (
        await session.execute(
            select(
                Alert.condition_id.label("cid"),
                func.count(Alert.id).label("n"),
                func.min(Alert.created).label("first_created"),
                func.max(Alert.created).label("last_created"),
                func.max(Alert.id).label("max_id"),
                func.max(_ALERT_SEVERITY_RANK).label("severity_rank"),
            )
            .join(AlertCondition, AlertCondition.id == Alert.condition_id)
            .where(
                Alert.id > AlertCondition.stats_synced_through,
                Alert.created < horizon,
            )
            .group_by(Alert.condition_id)
        )
    ).all()

    # Per-condition follow-ups (latest severity, median) run only for the
    # conditions that actually gained occurrences since the last sweep — a
    # handful on a healthy deployment, never a scan of the whole table.
    updated = 0
    counted = 0
    for row in rows:
        condition = await session.get(AlertCondition, row.cid)
        if condition is None:
            continue
        _apply_counts(
            condition,
            count=row.n,
            first_created=row.first_created,
            last_created=row.last_created,
            severity_rank=row.severity_rank,
        )
        condition.latest_severity = await _latest_severity(
            session, condition_id=row.cid, horizon=horizon
        )
        condition.median_interval_seconds = await _median_interval(
            session, condition_id=row.cid, horizon=horizon
        )
        # Only rows we actually counted move the watermark, and every one of
        # them is older than the lag horizon — so no in-flight row can be
        # stranded below it.
        condition.stats_synced_through = max(condition.stats_synced_through or 0, row.max_id)
        updated += 1
        counted += row.n

    await session.flush()
    return updated, counted


async def _latest_severity(session, *, condition_id: int, horizon: datetime) -> str | None:
    return await session.scalar(
        select(Alert.severity)
        .where(Alert.condition_id == condition_id, Alert.created < horizon)
        .order_by(Alert.created.desc(), Alert.id.desc())
        .limit(1)
    )


async def _median_interval(session, *, condition_id: int, horizon: datetime) -> int | None:
    """Median seconds between the last ``MEDIAN_SAMPLE`` occurrences, or None.

    Fewer than two occurrences means there is no interval to speak of; the
    caller falls back to the auto-close floor.
    """
    stamps = (
        await session.execute(
            select(Alert.created)
            .where(Alert.condition_id == condition_id, Alert.created < horizon)
            .order_by(Alert.id.desc())
            .limit(MEDIAN_SAMPLE + 1)
        )
    ).scalars().all()
    if len(stamps) < 2:
        return None

    ordered = sorted(_naive(s) for s in stamps)
    gaps = sorted(
        (ordered[i + 1] - ordered[i]).total_seconds() for i in range(len(ordered) - 1)
    )
    middle = len(gaps) // 2
    median = gaps[middle] if len(gaps) % 2 else (gaps[middle - 1] + gaps[middle]) / 2
    return int(median)


# =============================================================================
# Automatic rules
# =============================================================================


async def apply_lifecycle_rules(session, *, now: datetime | None = None) -> dict:
    """Run the automatic transitions. Returns a per-rule count dict."""
    now = now or datetime.now(UTC)
    reopened = await _reopen_recurrences(session, now=now)
    auto_closed = await _auto_close_resolved(session, now=now)
    auto_staled = await _auto_stale_new(session, now=now)
    await session.flush()
    return {
        "reopened": reopened,
        "auto_closed": auto_closed,
        "auto_staled": auto_staled,
    }


async def _reopen_recurrences(session, *, now: datetime) -> int:
    """Belt for the reopen rule.

    Delivery reopens inline (P4) — that is the braces, and the one that
    matters, because a sweep that ran in the wrong order could otherwise
    collect-swallow a recurrence. This catches conditions whose recurrence
    never reached delivery (already-notified occurrences, delivery disabled).
    """
    conditions = (
        (
            await session.execute(
                select(AlertCondition).where(
                    AlertCondition.status.in_([STATUS_RESOLVED, STATUS_CLOSED]),
                    AlertCondition.last_seen.is_not(None),
                    AlertCondition.last_seen
                    > func.coalesce(
                        AlertCondition.status_changed_at,
                        AlertCondition.resolved_at,
                        AlertCondition.closed_at,
                    ),
                )
            )
        )
        .scalars()
        .all()
    )
    for condition in conditions:
        reopen_condition(condition, now=now)
    return len(conditions)


def _auto_close_threshold(condition: AlertCondition) -> timedelta:
    median = condition.median_interval_seconds
    if not median or median <= 0:
        return AUTO_CLOSE_FLOOR
    return max(AUTO_CLOSE_FLOOR, timedelta(seconds=median * AUTO_CLOSE_MEDIAN_MULTIPLE))


async def _auto_close_resolved(session, *, now: datetime) -> int:
    """Close a resolved condition that has stayed quiet long enough.

    Deliberately unlocked. A concurrent occurrence arriving between the read
    and the write can close a condition that just fired again — and that is
    survivable precisely because it is not final: the next delivery sweep sees
    an unnotified occurrence on a closed condition and reopens it (P4/F6).
    Taking row locks across a sweep of every resolved condition would cost far
    more than the mistake does.
    """
    candidates = (
        (
            await session.execute(
                select(AlertCondition).where(AlertCondition.status == STATUS_RESOLVED)
            )
        )
        .scalars()
        .all()
    )
    closed = 0
    reference_now = _naive(now)
    for condition in candidates:
        last = _naive(condition.last_seen) or _naive(condition.resolved_at) or _naive(
            condition.status_changed_at
        )
        if last is None:
            continue
        if reference_now - last < _auto_close_threshold(condition):
            continue
        condition.status = STATUS_CLOSED
        condition.closed_at = now
        condition.status_changed_at = now
        closed += 1
    return closed


async def _auto_stale_new(session, *, now: datetime) -> int:
    """Close an untriaged condition that stopped happening a month ago.

    Unlocked for the same reason as :func:`_auto_close_resolved`: a recurrence
    reopens it, so a mistimed close costs one sweep of silence, not an alert.
    """
    candidates = (
        (
            await session.execute(
                select(AlertCondition).where(AlertCondition.status == STATUS_NEW)
            )
        )
        .scalars()
        .all()
    )
    staled = 0
    reference_now = _naive(now)
    for condition in candidates:
        last = _naive(condition.last_seen) or _naive(condition.created)
        if last is None or reference_now - last < AUTO_STALE_SILENCE:
            continue
        condition.status = STATUS_CLOSED
        condition.closed_at = now
        condition.status_changed_at = now
        condition.notes = _append_note(
            condition.notes, "auto-closed: no occurrence in 30 days"
        )
        staled += 1
    return staled


def _append_note(existing: str | None, line: str) -> str:
    return f"{existing}\n{line}" if existing else line


# =============================================================================
# Human transitions
# =============================================================================


async def _load(session, condition: AlertCondition | int) -> AlertCondition:
    if isinstance(condition, AlertCondition):
        return condition
    loaded = await session.get(AlertCondition, condition)
    if loaded is None:
        raise ValueError(f"No alert condition with id {condition}")
    return loaded


async def set_status(
    session,
    condition: AlertCondition | int,
    status: str,
    *,
    actor: str | None = None,
    issue_url: str | None = None,
    resolved_by: str | None = None,
    notes: str | None = None,
    now: datetime | None = None,
) -> AlertCondition:
    """Move a condition to ``status``, validating the transition and stamping it.

    Raises ``ValueError`` for an unknown or disallowed transition. This is an
    operator action on an admin surface, not the fire path — refusing it out
    loud is right; a silently-ignored acknowledgement would leave someone
    believing an alert was handled.
    """
    now = now or datetime.now(UTC)
    condition = await _load(session, condition)

    if status not in STATUSES:
        raise ValueError(f"Unknown alert condition status {status!r}")
    current = condition.status or STATUS_NEW
    if status != current and status not in ALLOWED_TRANSITIONS.get(current, frozenset()):
        raise ValueError(f"Cannot move an alert condition from {current!r} to {status!r}")

    condition.status = status
    condition.status_changed_at = now

    if status == STATUS_ACKNOWLEDGED:
        condition.acknowledged_at = now
        condition.acknowledged_by = actor
    elif status == STATUS_RESOLVED:
        condition.resolved_at = now
        condition.resolved_by = resolved_by or actor
    elif status == STATUS_CLOSED:
        condition.closed_at = now
    elif status == STATUS_NEW:
        # Back to untriaged: the old acknowledgement no longer describes it.
        condition.acknowledged_at = None
        condition.acknowledged_by = None
        condition.resolved_at = None
        condition.closed_at = None

    if issue_url is not None:
        condition.issue_url = issue_url
    if notes is not None:
        condition.notes = notes

    await session.flush()
    logger.info(
        "opsalert: condition %s (%s) %s → %s by %s",
        condition.id,
        condition.category,
        current,
        status,
        actor or "system",
    )
    return condition


async def set_disposition(
    session,
    condition: AlertCondition | int,
    disposition: str | None,
    *,
    actor: str | None = None,
) -> AlertCondition:
    """Set (or clear, with ``None``) a condition's delivery disposition."""
    condition = await _load(session, condition)
    if disposition is not None and disposition not in DISPOSITIONS:
        raise ValueError(f"Unknown alert condition disposition {disposition!r}")
    condition.disposition = disposition
    await session.flush()
    logger.info(
        "opsalert: condition %s (%s) disposition → %s by %s",
        condition.id,
        condition.category,
        disposition,
        actor or "system",
    )
    return condition
