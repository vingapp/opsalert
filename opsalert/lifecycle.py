"""Condition lifecycle — statistics, automatic rules, and human transitions.

Three jobs live here:

1. :func:`sync_condition_stats` folds occurrences into their condition's
   counters, so the counters survive the occurrences (which are pruned).
2. :func:`apply_lifecycle_rules` runs the automatic transitions: reopen,
   auto-close of a resolved condition that has gone quiet, auto-stale of a
   condition nobody ever triaged and that stopped happening, and escalation
   of an acknowledged condition that got worse (opsalert#7): a new all-time
   severity, a burst far above its ack-time rate, or a lease that expired
   while it kept firing.
3. :func:`set_status` / :func:`set_disposition` are the human edits, with
   transition validation and audit stamps.

Plain async functions, no scheduler dependency — the host app wires them into
whatever sweeper it runs.

opsalert#7 option 3 (tie the acknowledgement to the GitHub issue staying
open) is deliberately NOT implemented: opsalert runs inside the host app and
has no GitHub credentials or network access to check issue state.

The three baseline columns the escalation rule reads (``acknowledged_severity``,
``acknowledged_occurrence_count``, ``acknowledged_until``) are DDL owned by the
host app's alembic migrations, not by this package's ``ensure_tables``
(create_all only, never alters an existing table) — the host MUST apply that
migration before running this version, since ``select(AlertCondition)``
selects every mapped column and a missing column fails the query outright.
"""
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import case, func, select, update

from opsalert.model import Alert, AlertCondition, AlertConditionSubject
from opsalert.signature import condition_signature, normalize_message
from opsalert.store import TEMPLATE_CONTEXT_KEY, _lookup_or_create
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

# opsalert#7 — an acknowledged condition reopens when it gets worse than it
# was at ack time. Burst detection compares the CURRENT rate against a
# baseline rate computed over the condition's whole pre-ack lifetime
# (occurrence_count / age), never against ``median_interval_seconds``: the
# median is the median of the last MEDIAN_SAMPLE gaps, and for a convoy-shaped
# condition (bursts separated by long quiet stretches) that median tracks the
# few seconds BETWEEN convoy members even when the long-run rate is a handful
# per hour — a median-based rule would never trip for exactly the conditions
# this rule exists to catch (prod condition 261).
ACK_BURST_WINDOW = timedelta(minutes=15)
ACK_BURST_MIN = 10
ACK_BURST_MULTIPLE = 5
ACK_BURST_PEAK_MULTIPLE = 1.5
# Floor under the baseline's age denominator: a condition acknowledged
# minutes after it first appeared must not get a wildly inflated baseline
# rate from a near-zero age.
ACK_BASELINE_MIN_AGE = timedelta(hours=1)
# A condition reopens if this many NEW distinct subjects appear since ack.
ACK_SUBJECT_SPREAD = 5

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
    condition.acknowledged_severity = None
    condition.acknowledged_occurrence_count = None
    condition.acknowledged_until = None
    condition.acknowledged_peak_15m = None
    condition.acknowledged_subject_count = None
    # acknowledged_release is KEPT through reopen: it is "release at last ack"
    # and remains a valid baseline for is_regression in the attention feed.
    # Cleared only on new→ack re-stamp, resolve, and close.
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
                context = _context_dict(row.context_json)
                environment = _context_str(context, "environment")
                # A params emission stored its emit-time template on the row
                # (opsalert#2): reuse it VERBATIM, so the orphan lands under
                # the same identity the fire path would have produced.
                # Normalizing the rendered message is only for old rows that
                # never carried one — and for those, message == what the emit
                # path normalized, so the identities still agree.
                template = _context_str(context, TEMPLATE_CONTEXT_KEY) or normalize_message(
                    row.message or ""
                )
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


def _context_dict(context_json: str | None) -> dict:
    """Parse a stored context, tolerating anything.

    A context we cannot parse is a context with no readable keys: the
    occurrence lands in the unlabelled bucket (and falls back to the
    normalized message for identity) rather than being skipped.
    """
    if not context_json:
        return {}
    try:
        parsed = json.loads(context_json)
    except (ValueError, TypeError):
        logger.warning("opsalert: unparseable context on an orphan occurrence")
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _context_str(context: dict, key: str) -> str | None:
    """A non-empty string value out of a parsed context, or None."""
    value = context.get(key)
    return value if isinstance(value, str) and value else None


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
        prev_watermark = condition.stats_synced_through or 0
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
        # Fold release strings from occurrence context — only new rows.
        await _fold_release(
            session, condition, horizon=horizon, prev_watermark=prev_watermark,
        )
        # Only rows we actually counted move the watermark, and every one of
        # them is older than the lag horizon — so no in-flight row can be
        # stranded below it.
        condition.stats_synced_through = max(prev_watermark, row.max_id)
        updated += 1
        counted += row.n

    await session.flush()
    return updated, counted


async def _peak_15m_count(
    session, *, condition_id: int, before: datetime
) -> int:
    """Max occurrence count in any 15-minute bucket in the 24 h before ``before``.

    Buckets occurrences by 15-minute intervals and returns the highest count.
    Used to baseline the burst detection rule at ack time.
    """
    window_start = before - timedelta(hours=24)
    stamps = (
        await session.execute(
            select(Alert.created).where(
                Alert.condition_id == condition_id,
                Alert.created >= window_start,
                Alert.created <= before,
            )
        )
    ).scalars().all()

    if not stamps:
        return 0

    # Bucket by 15-minute intervals (epoch seconds // 900).
    buckets: dict[int, int] = {}
    for stamp in stamps:
        naive = _naive(stamp)
        if naive is None:
            continue
        bucket = int(naive.timestamp()) // 900
        buckets[bucket] = buckets.get(bucket, 0) + 1

    return max(buckets.values()) if buckets else 0


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


async def _fold_release(
    session, condition: AlertCondition, *, horizon: datetime, prev_watermark: int
) -> None:
    """Fold ``_release`` from occurrence context into the condition.

    Scans ONLY rows above ``prev_watermark`` (the stats_synced_through
    value before this sweep's fold) and before ``horizon``, so repeated
    sweeps never rescan the same rows. ``first_seen_release`` is set only
    when NULL; ``last_seen_release`` = the last non-null in the window.
    """
    rows = (
        await session.execute(
            select(Alert.context_json)
            .where(
                Alert.condition_id == condition.id,
                Alert.id > prev_watermark,
                Alert.created < horizon,
                Alert.context_json.is_not(None),
            )
            .order_by(Alert.id.asc())
        )
    ).all()

    if not rows:
        return

    first_release: str | None = None
    last_release: str | None = None
    for row in rows:
        ctx = _context_dict(row.context_json)
        release = _context_str(ctx, "_release")
        if release is None:
            continue
        if first_release is None:
            first_release = release
        last_release = release

    if first_release is not None and condition.first_seen_release is None:
        condition.first_seen_release = first_release
    if last_release is not None:
        condition.last_seen_release = last_release


# =============================================================================
# Automatic rules
# =============================================================================


async def apply_lifecycle_rules(session, *, now: datetime | None = None) -> dict:
    """Run the automatic transitions. Returns a per-rule count dict."""
    now = now or datetime.now(UTC)
    reopened = await _reopen_recurrences(session, now=now)
    auto_closed = await _auto_close_resolved(session, now=now)
    auto_staled = await _auto_stale_new(session, now=now)
    escalated = await _escalate_acknowledged(session, now=now)
    await session.flush()
    return {
        "reopened": reopened,
        "auto_closed": auto_closed,
        "auto_staled": auto_staled,
        "escalated": escalated,
    }


async def _escalate_acknowledged(session, *, now: datetime) -> int:
    """Bring an acknowledged condition back to ``new`` when it got worse.

    "Acknowledged" means a human has seen THIS episode and owns it — not
    "silent forever". Three independent triggers, checked in this order
    (first match wins) for every acknowledged condition:

    1. Severity escalation — an occurrence since the ack outranks the worst
       severity the condition had AT ack time.
    2. Burst — occurrences in the last :data:`ACK_BURST_WINDOW` are at least
       :data:`ACK_BURST_MIN` and that rate is more than
       :data:`ACK_BURST_MULTIPLE` times the condition's baseline rate at ack
       time (occurrence_count / age, floored at :data:`ACK_BASELINE_MIN_AGE`
       — see the module-level comment for why this is NOT the median).
    3. Lease expiry — ``acknowledged_until`` has passed AND the condition has
       fired since the ack (``last_seen > acknowledged_at``). A lease expiring
       on a condition that went quiet does nothing; the auto-close/auto-stale
       rules own a quiet condition.

    One aggregate query per acknowledged condition covers both occurrence
    rules — acknowledged conditions are few (dozens), so per-condition is
    fine, but a query per condition per rule is not.

    NULL ``acknowledged_severity``/``acknowledged_occurrence_count`` (rows
    acknowledged before this column existed) do NOT fall back to
    ``condition.severity``. The sweep runs :func:`sync_condition_stats`
    before this rule, and that fold already merges every post-ack occurrence
    into ``condition.severity`` — "worse than the worst-ever" could then
    never trip, silently disabling escalation for exactly the rows most in
    need of it. So NULL severity means the ack-time baseline is genuinely
    unknowable and rule 1 is SKIPPED for that condition (the host migration
    backfills the baseline for pre-existing rows, so this is transient).
    NULL count still means a baseline of 0 for rule 2, so any burst at or
    above ``ACK_BURST_MIN`` trips it — legacy acks made no rate promise.
    """
    conditions = (
        (
            await session.execute(
                select(AlertCondition).where(AlertCondition.status == STATUS_ACKNOWLEDGED)
            )
        )
        .scalars()
        .all()
    )
    if not conditions:
        return 0

    window_start = now - ACK_BURST_WINDOW
    escalated = 0
    for condition in conditions:
        if condition.disposition == DISPOSITION_COLLECT:
            # "wontfix" (acknowledged + collect): a human decided this is never
            # to be acted on. Reopening it would only churn reopened_count —
            # collect never wakes anyone either way, and the operator's
            # decision stands until they change the disposition.
            continue
        ack_at = condition.acknowledged_at
        if ack_at is None:
            # No ack timestamp at all — nothing to compare occurrences
            # against; leave it alone rather than guess.
            continue

        # One aggregate query covers both occurrence rules: the max severity
        # rank over every occurrence since ack (rule 1) and a count of just
        # the ones inside the burst window (rule 2), via a conditional
        # aggregate rather than two round trips.
        row = (
            await session.execute(
                select(
                    func.max(_ALERT_SEVERITY_RANK).label("max_rank_since_ack"),
                    func.count(case((Alert.created > window_start, 1))).label("burst_count"),
                ).where(Alert.condition_id == condition.id, Alert.created > ack_at)
            )
        ).one()
        max_rank_since_ack = row.max_rank_since_ack

        note = None

        ack_severity = condition.acknowledged_severity
        if ack_severity is not None:
            ack_rank = _SEVERITY_ORDER.get(ack_severity, 0)
            worst_since_rank = max_rank_since_ack or 0
            if worst_since_rank > ack_rank:
                worst_since = _RANK_TO_SEVERITY.get(worst_since_rank, "?")
                note = (
                    f"reopened: severity escalated {ack_severity} → {worst_since} "
                    "after acknowledgement"
                )
        if note is None:
            burst_count = row.burst_count or 0
            # Burst threshold: max(10, 1.5 * acknowledged_peak_15m).
            # NULL peak → treat as 0, so threshold = 10.
            peak = condition.acknowledged_peak_15m or 0
            burst_threshold = max(ACK_BURST_MIN, int(ACK_BURST_PEAK_MULTIPLE * peak))
            if burst_count > burst_threshold:
                window_minutes = int(ACK_BURST_WINDOW.total_seconds() // 60)
                note = (
                    f"reopened: burst {burst_count} in {window_minutes}m vs "
                    f"threshold {burst_threshold} (peak {peak}/15m) "
                    "after acknowledgement"
                )
        # Rule 3: subject spread — >= 5 new distinct subjects beyond ack baseline.
        if note is None:
            ack_subject_count = condition.acknowledged_subject_count
            if ack_subject_count is not None:
                current_subject_count = await distinct_subjects(
                    session, condition.id,
                    since=(now - timedelta(days=365 * 10)).date(),
                )
                new_subjects = current_subject_count - ack_subject_count
                if new_subjects >= ACK_SUBJECT_SPREAD:
                    note = (
                        f"reopened: {new_subjects} new subjects since acknowledgement "
                        f"(was {ack_subject_count}, now {current_subject_count})"
                    )
        # Rule 4: regression — last_seen_release != acknowledged_release.
        if note is None:
            ack_release = condition.acknowledged_release
            current_release = condition.last_seen_release
            if (
                ack_release is not None
                and current_release is not None
                and current_release != ack_release
            ):
                note = (
                    f"reopened: regression — release changed "
                    f"{ack_release} → {current_release} after acknowledgement"
                )
        # Rule 5: lease expiry.
        if note is None and condition.acknowledged_until is not None:
                until = _naive(condition.acknowledged_until)
                naive_now = _naive(now)
                last_seen = _naive(condition.last_seen)
                naive_ack_at = _naive(ack_at)
                if (
                    naive_now > until
                    and last_seen is not None
                    and last_seen > naive_ack_at
                ):
                    note = "reopened: acknowledgement lease expired while still firing"

        if note is None:
            continue

        reopen_condition(condition, now=now)
        condition.notes = _append_note(condition.notes, note)
        escalated += 1

    return escalated


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
# Subject tracking
# =============================================================================


def subject_upsert_statement(dialect: str, values: dict[str, Any]):
    """Build a dialect-appropriate INSERT IGNORE for alert_condition_subject.

    MySQL uses ``INSERT IGNORE`` (``on_duplicate_key_update`` with a no-op
    set so the row is silently skipped on conflict). SQLite uses
    ``ON CONFLICT DO NOTHING``. Exposed as a builder so O2/ingest can use
    it from a sync connection.
    """
    if dialect == "mysql":
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        return (
            mysql_insert(AlertConditionSubject)
            .values(**values)
            .on_duplicate_key_update(condition_id=values["condition_id"])
        )

    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    return (
        sqlite_insert(AlertConditionSubject)
        .values(**values)
        .on_conflict_do_nothing()
    )


async def record_subjects(
    session,
    condition_id: int,
    subjects: list[tuple[str, str]],
    day,
) -> None:
    """INSERT IGNORE / upsert-do-nothing rows into alert_condition_subject.

    ``subjects`` is a list of ``(kind, key)`` tuples.  ``day`` is the
    calendar date the subjects were observed on.
    """
    from opsalert.store import _dialect_name

    dialect = _dialect_name(session)
    for kind, key in subjects:
        values = {
            "condition_id": condition_id,
            "subject_kind": kind,
            "subject_key": key,
            "day": day,
        }
        stmt = subject_upsert_statement(dialect, values)
        await session.execute(stmt)


async def distinct_subjects(session, condition_id: int, *, since) -> int:
    """Count distinct (subject_kind, subject_key) pairs for a condition since ``since``."""
    result = await session.scalar(
        select(func.count())
        .select_from(
            select(AlertConditionSubject.subject_kind, AlertConditionSubject.subject_key)
            .where(
                AlertConditionSubject.condition_id == condition_id,
                AlertConditionSubject.day >= since,
            )
            .group_by(AlertConditionSubject.subject_kind, AlertConditionSubject.subject_key)
            .subquery()
        )
    )
    return result or 0


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
    acknowledged_until: datetime | None = None,
) -> AlertCondition:
    """Move a condition to ``status``, validating the transition and stamping it.

    Raises ``ValueError`` for an unknown or disallowed transition. This is an
    operator action on an admin surface, not the fire path — refusing it out
    loud is right; a silently-ignored acknowledgement would leave someone
    believing an alert was handled.

    Acknowledging (including re-acknowledging an already-acknowledged
    condition — the same-status path below still reaches this) stamps
    ``acknowledged_severity``/``acknowledged_occurrence_count`` from the
    condition's current state and, if given, ``acknowledged_until`` — a lease
    an operator uses to renew, or to re-baseline after a burst. ``now`` if the
    lease has already passed is refused: a lease in the past reopens on the
    very next sweep, which is not what "acknowledge with a lease" means.
    """
    now = now or datetime.now(UTC)
    if acknowledged_until is not None and _naive(acknowledged_until) <= _naive(now):
        raise ValueError("acknowledged_until must be in the future")
    condition = await _load(session, condition)

    if status not in STATUSES:
        raise ValueError(f"Unknown alert condition status {status!r}")
    current = condition.status or STATUS_NEW
    if status != current and status not in ALLOWED_TRANSITIONS.get(current, frozenset()):
        raise ValueError(f"Cannot move an alert condition from {current!r} to {status!r}")

    # Acknowledged = owned: require an issue URL unless this is a snooze
    # (acknowledged_until with no issue). The message string is the contract.
    if status == STATUS_ACKNOWLEDGED:
        has_issue = issue_url or condition.issue_url
        is_snooze = acknowledged_until is not None and not has_issue
        if not has_issue and not is_snooze:
            raise ValueError("ack_requires_issue")

    condition.status = status
    condition.status_changed_at = now

    if status == STATUS_ACKNOWLEDGED:
        condition.acknowledged_at = now
        condition.acknowledged_by = actor
        # ``occurrence_count``/``severity`` are stats-sweep counters, stale by
        # up to a sweep interval plus STATS_LAG_SECONDS — a condition acked
        # mid-burst could have occurrence_count still 0, which would make it
        # re-trip the burst rule on the very next sweep. Count and rank
        # directly off Alert rows created at or before this ack instead.
        ack_count, ack_max_rank = (
            await session.execute(
                select(
                    func.count(Alert.id),
                    func.max(_ALERT_SEVERITY_RANK),
                ).where(Alert.condition_id == condition.id, Alert.created <= now)
            )
        ).one()
        condition.acknowledged_occurrence_count = ack_count or 0
        occurrence_severity = _RANK_TO_SEVERITY.get(ack_max_rank or 0)
        condition.acknowledged_severity = worst_severity(condition.severity, occurrence_severity)
        condition.acknowledged_until = acknowledged_until

        # Stamp the peak 15-minute occurrence count in the 24 h before ack.
        condition.acknowledged_peak_15m = await _peak_15m_count(
            session, condition_id=condition.id, before=now,
        )
        # Stamp the distinct subject count at ack time.
        condition.acknowledged_subject_count = await distinct_subjects(
            session, condition.id, since=(now - timedelta(days=365 * 10)).date(),
        )
        # Stamp the release at ack time.
        condition.acknowledged_release = condition.last_seen_release
    elif status == STATUS_RESOLVED:
        condition.resolved_at = now
        condition.resolved_by = resolved_by or actor
    elif status == STATUS_CLOSED:
        condition.closed_at = now

    if status in (STATUS_RESOLVED, STATUS_CLOSED):
        # Clear the ack-time release baseline: the episode is over, and
        # a future ack will re-stamp from the new state.
        condition.acknowledged_release = None
        # Retire the undelivered backlog (opsalert#1). Resolving is a human
        # saying "I have seen this and dealt with it" — emailing the history
        # afterwards has no audience, and leaving it unnotified either
        # phantom-reopened the condition (before delivery's time predicate)
        # or would sit owed-but-undeliverable forever (after it). ``notified``
        # keeps meaning "no longer owed to anyone". Occurrences arriving
        # after ``now`` are untouched: they are the recurrence signal.
        await session.execute(
            update(Alert)
            .where(
                Alert.condition_id == condition.id,
                Alert.notified.is_(False),
                Alert.created <= now,
            )
            .values(notified=True)
        )
    elif status == STATUS_NEW:
        # Back to untriaged: the old acknowledgement no longer describes it.
        condition.acknowledged_at = None
        condition.acknowledged_by = None
        condition.acknowledged_severity = None
        condition.acknowledged_occurrence_count = None
        condition.acknowledged_until = None
        condition.acknowledged_peak_15m = None
        condition.acknowledged_subject_count = None
        condition.acknowledged_release = None
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
