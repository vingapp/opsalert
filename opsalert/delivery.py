"""Alert delivery — condition-aware notification with per-condition gating.

What gets emailed is decided per CONDITION, not per category:

- a resolved/closed condition that fired again is reopened INLINE, before any
  gating, so a recurrence can never be swallowed by the state it was left in
  (P4). Delivery does not wait for the lifecycle sweep to notice;
- ``collect`` marks its occurrences notified and sends nothing;
- an ``acknowledged`` condition gets the digest at most — somebody is already
  on it, and its occurrences keep accruing regardless (P5);
- ``immediate`` conditions are batched into ONE email per category per sweep
  whose body enumerates the conditions (P7), and the THROTTLE APPLIES TO THAT
  EMAIL: it goes out only if at least one member is unthrottled — never
  emailed inside the window, or just reopened — and it then carries every
  member with unnotified occurrences. A brand-new condition still wakes you;
  a category whose members have all been emailed once is silent for the rest
  of the window, whatever their occurrence timing (opsalert#5).

Occurrences with no condition (fire-time resolution failed, or rows older than
conditions) are delivered by the original category-grouped path, unchanged.

Every transport-accepted send commits its notified-marks before the next send
(P12) — a delivered email's mark must not be able to roll back.

Plain async functions — no scheduler dependency. The host app wraps these in
whatever scheduler it uses.
"""
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, case, func, or_, select, update

from opsalert._config import _resolve_setting, get_config
from opsalert.lifecycle import (
    DISPOSITION_COLLECT,
    DISPOSITION_DIGEST,
    DISPOSITION_IMMEDIATE,
    STATUS_ACKNOWLEDGED,
    STATUS_CLOSED,
    STATUS_RESOLVED,
    STATUSES,
    effective_disposition,
    reopen_condition,
)
from opsalert.model import Alert, AlertCondition, AlertDeliveryState
from opsalert.types import DIGEST_SEVERITIES, IMMEDIATE_SEVERITIES, AlertMessage, AlertSeverity

logger = logging.getLogger(__name__)

# Severity rank for proper MAX ordering
_SEVERITY_RANK = case(
    (Alert.severity == AlertSeverity.CRITICAL, 3),
    (Alert.severity == AlertSeverity.ERROR, 2),
    (Alert.severity == AlertSeverity.WARN, 1),
    else_=0,
)
_RANK_TO_SEVERITY = {
    3: AlertSeverity.CRITICAL.value,
    2: AlertSeverity.ERROR.value,
    1: AlertSeverity.WARN.value,
}

# In-Python severity ordering, for picking the worst severity in a batch.
_SEVERITY_ORDER = {
    AlertSeverity.CRITICAL.value: 3,
    AlertSeverity.ERROR.value: 2,
    AlertSeverity.WARN.value: 1,
}


def _worst_severity(severities) -> str:
    """The most severe of the given severity strings (warn if none are known)."""
    return max(
        severities,
        key=lambda s: _SEVERITY_ORDER.get(s, 0),
        default=AlertSeverity.WARN.value,
    )


# How many conditions an immediate email lists before it says "and N more".
_CONDITION_LIST_CAP = 10

# Marking is bounded by (condition, max occurrence id) pairs so a row that
# arrived DURING the sweep is never marked as notified by an email that could
# not have contained it. Pairs are applied in chunks to keep the statement sane.
_MARK_CHUNK = 200


@dataclass
class _ConditionBatch:
    """One condition's unnotified occurrences, as seen at the start of a sweep."""

    condition_id: int
    category: str
    template: str
    latest_message: str
    severity: str
    status: str
    disposition: str | None
    count: int
    max_id: int
    last_created: datetime | None
    # Fields for the Alertmanager payload (loaded from the condition row).
    signature_key: str = ""
    first_seen: datetime | None = None
    resolved_at: datetime | None = None
    issue_url: str | None = None
    source: str | None = None


@dataclass
class _DigestRow:
    """Row shape the digest renderer expects (category / message / count)."""

    category: str
    latest_message: str
    count: int


def _subject_prefix(environment: str | None) -> str:
    """``"[STAGING] "`` when an environment is configured, ``""`` when not.

    An alert that does not say which deployment it came from is a bug report
    with the machine name torn off. Consumers that never set `environment`
    keep byte-identical subjects.
    """
    return f"[{environment.upper()}] " if environment else ""


def _environment_html(environment: str | None) -> str:
    """Leading ``Environment: staging`` line for an email body, or nothing."""
    if not environment:
        return ""
    return (
        '<p style="margin: 0 0 12px 0; font-size: 14px; color: #666;">'
        f"Environment: <strong>{environment}</strong></p>"
    )


def _environment_text(environment: str | None) -> str:
    """Leading ``Environment: staging`` line for a plain-text body."""
    return f"Environment: {environment}\n" if environment else ""


def _alertmanager_payload(
    *,
    batches: list["_ConditionBatch"],
    status: str = "firing",
    environment: str | None = None,
) -> dict:
    """Build an Alertmanager v4 webhook payload from condition batches.

    Shape per spec: ``{"version":"4","status":"firing"|"resolved","alerts":[
    {"labels":{"alertname","severity","category","environment","release"},
    "annotations":{"summary","issue_url","emit_site","condition_id"},
    "startsAt":first_seen iso,"endsAt":resolved_at iso or "",
    "fingerprint":signature_key}]}``.

    ``kind`` does not exist until O2; use ``message_template`` for ``alertname``
    now, read ``kind`` when the attribute exists.
    """
    alerts = []
    for batch in batches:
        alertname = batch.template or batch.category
        labels: dict[str, str] = {
            "alertname": alertname,
            "severity": batch.severity,
            "category": batch.category,
        }
        if environment:
            labels["environment"] = environment
        annotations: dict[str, str] = {
            "summary": batch.latest_message,
            "condition_id": str(batch.condition_id),
            "issue_url": batch.issue_url or "",
            "emit_site": batch.source or "",
        }
        alert_item: dict = {
            "labels": labels,
            "annotations": annotations,
            "startsAt": (
                batch.first_seen.isoformat() if batch.first_seen else ""
            ),
            "endsAt": (
                batch.resolved_at.isoformat() if batch.resolved_at else ""
            ),
            "fingerprint": batch.signature_key,
        }
        alerts.append(alert_item)
    return {
        "version": "4",
        "status": status,
        "alerts": alerts,
    }


async def deliver_alerts(session) -> dict:
    """Deliver alert notification emails. Call from your scheduler.

    Returns stats dict with immediate_sent, immediate_throttled,
    immediate_throttled_conditions, digest_sent, digest_count, reopened,
    collected, skipped.

    ``immediate_throttled`` counts category EMAILS suppressed because every
    one of that category's conditions was inside its throttle window — the
    figure that says how much mail the throttle actually prevented.
    ``immediate_throttled_conditions`` is the finer number: conditions that
    were inside their window, whether they were held back with a suppressed
    email or rode along on one that went out anyway.
    """
    stats = {
        "immediate_sent": 0,
        "immediate_throttled": 0,
        "immediate_throttled_conditions": 0,
        "digest_sent": 0,
        "digest_count": 0,
        "reopened": 0,
        "collected": 0,
        "skipped": 0,
    }

    enabled = _resolve_setting("delivery_enabled", True)
    if not enabled:
        return stats

    to_email = _resolve_setting("delivery_to_email", "")
    from_email = _resolve_setting("delivery_from_email", "")
    from_name = _resolve_setting("delivery_from_name", "OpsAlert")
    throttle_minutes = _resolve_setting("delivery_throttle_minutes", 60)

    # FIRST, before anything is gated: a condition somebody closed out that is
    # firing again is a live problem, whatever its disposition says.
    reopened_ids = await _reopen_recurring(session)
    stats["reopened"] = len(reopened_ids)

    batches, skipped = await _load_condition_batches(session)
    stats["skipped"] += skipped

    immediate: list[_ConditionBatch] = []
    digest: list[_ConditionBatch] = []
    collect: list[_ConditionBatch] = []
    for batch in batches:
        if batch.condition_id in reopened_ids:
            # P4: a recurrence of something we thought was fixed goes out
            # immediately, whatever disposition it was parked under. The
            # disposition described the old episode.
            immediate.append(batch)
            continue
        disposition = effective_disposition(batch.severity, batch.disposition)
        if batch.status == STATUS_ACKNOWLEDGED and disposition == DISPOSITION_IMMEDIATE:
            # P5: acknowledged leaves the immediate line. Digest at most.
            disposition = DISPOSITION_DIGEST
        if disposition == DISPOSITION_COLLECT:
            collect.append(batch)
        elif disposition == DISPOSITION_DIGEST:
            digest.append(batch)
        else:
            immediate.append(batch)

    # Collected conditions are recorded, not announced: mark and move on.
    if collect:
        await _mark_notified(session, [(b.condition_id, b.max_id) for b in collect])
        await session.commit()
        stats["collected"] = sum(b.count for b in collect)

    cfg = get_config()
    if cfg.transport is None:
        return stats

    stats.update(
        await _deliver_immediate(
            session,
            immediate,
            to_email,
            from_email,
            from_name,
            throttle_minutes,
            reopened_ids,
        )
    )
    stats.update(await _deliver_digest(session, digest, to_email, from_email, from_name))

    total = stats["immediate_sent"] + stats["digest_sent"]
    if total > 0:
        logger.info(
            "Alert delivery: %d immediate, %d digest (%d warnings)",
            stats["immediate_sent"],
            stats["digest_sent"],
            stats["digest_count"],
        )

    return stats


# =============================================================================
# Condition-aware routing
# =============================================================================


async def _reopen_recurring(session) -> set[int]:
    """Reopen every resolved/closed condition that has recurred SINCE then.

    Returns the ids it reopened, which then bypass disposition gating below.

    This runs before disposition gating on purpose. If reopening were left to
    the maintenance sweep, a sweep order of "deliver, then apply rules" would
    let a recurrence on a collect-dispositioned closed condition be marked
    notified and silently swallowed — the alert nobody ever sees.

    A recurrence is an occurrence CREATED AFTER the condition left the living
    set — not merely an unnotified one. ``notified=False`` alone also matches
    the pre-resolution backlog, and resolving a condition that still held
    undelivered occurrences then reopened it on the very next delivery pass,
    for a problem that had not fired since before the fix (opsalert#1 —
    prod condition 52 reopened off a backlog 57 minutes older than its
    resolution). ``set_status`` now marks that backlog notified at
    resolve/close time; the time predicate here is the second line of
    defense, and the one that defines what "recurred" means.
    """
    condition_ids = (
        (
            await session.execute(
                select(Alert.condition_id)
                .join(AlertCondition, AlertCondition.id == Alert.condition_id)
                .where(
                    Alert.notified.is_(False),
                    AlertCondition.status.in_([STATUS_RESOLVED, STATUS_CLOSED]),
                    Alert.created
                    > func.coalesce(
                        AlertCondition.resolved_at,
                        AlertCondition.closed_at,
                        AlertCondition.status_changed_at,
                    ),
                )
                .distinct()
            )
        )
        .scalars()
        .all()
    )
    if not condition_ids:
        return set()

    now = datetime.now(UTC)
    reopened: set[int] = set()
    for condition_id in condition_ids:
        condition = await session.get(AlertCondition, condition_id)
        if condition is None:
            continue
        reopen_condition(condition, now=now)
        reopened.add(condition_id)

    # The reopen is a state change that must outlive whatever happens to the
    # rest of this sweep — commit it in place, like a sent mark.
    await session.commit()
    return reopened


async def _load_condition_batches(session) -> tuple[list[_ConditionBatch], int]:
    """Group unnotified occurrences by condition. Returns (batches, skipped)."""
    # The newest unnotified occurrence's text, per condition. The template is
    # the identity, but a subject line reading "Row <n> failed" is worse than
    # one reading "Row 42 failed" — the reader wants the instance.
    latest_message = (
        select(Alert.message)
        .where(Alert.condition_id == AlertCondition.id, Alert.notified.is_(False))
        .order_by(Alert.created.desc(), Alert.id.desc())
        .limit(1)
        .correlate(AlertCondition)
        .scalar_subquery()
    )

    rows = (
        await session.execute(
            select(
                AlertCondition.id.label("condition_id"),
                AlertCondition.category,
                AlertCondition.message_template,
                latest_message.label("latest_message"),
                AlertCondition.severity,
                AlertCondition.status,
                AlertCondition.disposition,
                func.count(Alert.id).label("count"),
                func.max(Alert.id).label("max_id"),
                func.max(Alert.created).label("last_created"),
                AlertCondition.signature_key,
                AlertCondition.first_seen,
                AlertCondition.resolved_at,
                AlertCondition.issue_url,
                AlertCondition.source,
            )
            .join(Alert, Alert.condition_id == AlertCondition.id)
            .where(Alert.notified.is_(False))
            .group_by(
                AlertCondition.id,
                AlertCondition.category,
                AlertCondition.message_template,
                AlertCondition.severity,
                AlertCondition.status,
                AlertCondition.disposition,
                AlertCondition.signature_key,
                AlertCondition.first_seen,
                AlertCondition.resolved_at,
                AlertCondition.issue_url,
                AlertCondition.source,
            )
        )
    ).all()

    batches: list[_ConditionBatch] = []
    skipped = 0
    for row in rows:
        # F3: one unreadable condition row must not take the sweep down with
        # it. Skip it loudly and deliver everything else.
        try:
            if row.status not in STATUSES:
                raise ValueError(f"unknown status {row.status!r}")
            if not row.category:
                raise ValueError("condition has no category")
            batches.append(
                _ConditionBatch(
                    condition_id=row.condition_id,
                    category=str(row.category),
                    template=str(row.message_template or ""),
                    latest_message=str(row.latest_message or row.message_template or ""),
                    severity=str(row.severity or ""),
                    status=str(row.status),
                    disposition=row.disposition,
                    count=int(row.count),
                    max_id=int(row.max_id),
                    last_created=row.last_created,
                    signature_key=str(row.signature_key or ""),
                    first_seen=row.first_seen,
                    resolved_at=row.resolved_at,
                    issue_url=row.issue_url,
                    source=row.source,
                )
            )
        except Exception as exc:
            skipped += 1
            logger.exception(
                "opsalert: skipping unusable condition %s during delivery", row.condition_id
            )
            _report_delivery_skip(row.condition_id, exc)
    return batches, skipped


def _report_delivery_skip(condition_id: object, exc: Exception) -> None:
    """Raise an alert about a condition delivery could not read.

    A row that delivery cannot interpret is silently un-deliverable — the
    exact failure mode that hides real alerts — so it becomes an alert of its
    own. Guarded, because alerting about a delivery failure must not be able
    to break delivery.
    """
    try:
        from opsalert._dispatch import error as _error

        _error(
            "alert_delivery",
            message="Unusable alert condition row skipped during delivery",
            source="opsalert.delivery",
            context={"condition_id": condition_id, "reason": str(exc)},
        )
    except Exception:
        logger.exception("opsalert: could not report unusable condition %s", condition_id)


def delivery_state_upsert_statement(dialect: str, now: datetime):
    """Build a dialect-appropriate upsert for alert_delivery_state.

    MySQL uses ``ON DUPLICATE KEY UPDATE``; SQLite uses
    ``ON CONFLICT DO UPDATE``. Exposed as a builder for testability
    and sync use.
    """
    if dialect == "mysql":
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        return (
            mysql_insert(AlertDeliveryState)
            .values(id=1, last_digest_sent_at=now)
            .on_duplicate_key_update(last_digest_sent_at=now)
        )

    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    return (
        sqlite_insert(AlertDeliveryState)
        .values(id=1, last_digest_sent_at=now)
        .on_conflict_do_update(
            index_elements=[AlertDeliveryState.id],
            set_={"last_digest_sent_at": now},
        )
    )


async def _update_digest_sent_at(session) -> None:
    """Record the current time as last_digest_sent_at in alert_delivery_state."""
    from opsalert.store import _dialect_name

    now = datetime.now(UTC)
    dialect = _dialect_name(session)
    stmt = delivery_state_upsert_statement(dialect, now)
    await session.execute(stmt)


async def _mark_notified(session, pairs: list[tuple[int, int]]) -> None:
    """Mark each condition's occurrences up to ``max_id`` as notified."""
    for start in range(0, len(pairs), _MARK_CHUNK):
        chunk = pairs[start : start + _MARK_CHUNK]
        await session.execute(
            update(Alert)
            .where(
                Alert.notified.is_(False),
                or_(
                    *[
                        and_(Alert.condition_id == cid, Alert.id <= max_id)
                        for cid, max_id in chunk
                    ]
                ),
            )
            .values(notified=True)
        )


async def _throttled_condition_ids(
    session, batches: list[_ConditionBatch], throttle_minutes: int
) -> set[int]:
    """Condition ids that were emailed inside the throttle window.

    Throttle state is read from notified occurrence rows (P12) rather than
    stored on the condition: rows are the record of what actually went out,
    and the window (60 min) is orders of magnitude shorter than retention
    (90 d), so the evidence is always there when it matters.
    """
    if throttle_minutes <= 0 or not batches:
        return set()

    cutoff = (datetime.now(UTC) - timedelta(minutes=throttle_minutes)).replace(tzinfo=None)
    rows = (
        await session.execute(
            select(Alert.condition_id, func.max(Alert.created).label("last_notified_at"))
            .where(
                Alert.notified.is_(True),
                Alert.condition_id.in_([b.condition_id for b in batches]),
            )
            .group_by(Alert.condition_id)
        )
    ).all()
    return {
        row.condition_id
        for row in rows
        if row.last_notified_at is not None
        and row.last_notified_at.replace(tzinfo=None) > cutoff
    }


async def _deliver_immediate(
    session,
    batches: list[_ConditionBatch],
    to_email: str,
    from_email: str,
    from_name: str,
    throttle_minutes: int,
    reopened_ids: set[int] | None = None,
) -> dict:
    """One email per category per sweep, enumerating that category's conditions.

    The throttle gates the EMAIL, not membership in it (opsalert#5). A
    category is mailed only when at least one of its conditions is unthrottled
    — never emailed inside the window, or just reopened. When it is mailed it
    carries every member with unnotified occurrences: the throttled ones ride
    along and are marked notified, because the mail is going out regardless and
    "was emailed about" is exactly what the window measures. When every member
    is throttled nothing is sent, nothing is marked, and the occurrences wait
    for a later sweep. Dropping the throttled condition from the list instead
    (the pre-fix behaviour) let any one fresh member re-send the whole
    category, so the effective interval was throttle/N, not throttle.
    """
    stats = {
        "immediate_sent": 0,
        "immediate_throttled": 0,
        "immediate_throttled_conditions": 0,
    }
    cfg = get_config()
    environment = cfg.environment

    # A reopen is a state change, not a repeat: it is never throttled by the
    # emails sent about the episode that was closed out.
    throttled = await _throttled_condition_ids(
        session, batches, throttle_minutes
    ) - (reopened_ids or set())
    by_category: dict[str, list[_ConditionBatch]] = {}
    for batch in batches:
        by_category.setdefault(batch.category, []).append(batch)

    for category, included in by_category.items():
        held = [b for b in included if b.condition_id in throttled]
        if len(held) == len(included):
            # Every member is inside its window: this email is suppressed.
            stats["immediate_throttled"] += 1
            stats["immediate_throttled_conditions"] += len(held)
            continue
        stats["immediate_throttled_conditions"] += len(held)

        included.sort(key=lambda b: b.count, reverse=True)
        worst = _worst_severity([b.severity for b in included])
        total = sum(b.count for b in included)
        headline = included[0].latest_message

        subject = (
            f"{_subject_prefix(environment)}"
            f"[{worst.upper()}] {category}: {headline[:60]}"
        )
        message = AlertMessage(
            subject=subject,
            html_body=_render_immediate_email(
                category=category,
                severity=worst,
                count=total,
                conditions=included,
                environment=environment,
            ),
            text_body=(
                f"{_environment_text(environment)}"
                f"{worst.upper()} — {category}: {len(included)} condition(s), "
                f"{total} occurrence(s)\n"
                + "".join(
                    f"- #{b.condition_id} {b.template} ×{b.count}\n"
                    for b in included[:_CONDITION_LIST_CAP]
                )
                + (
                    f"- and {len(included) - _CONDITION_LIST_CAP} more\n"
                    if len(included) > _CONDITION_LIST_CAP
                    else ""
                )
            ),
            severity=worst,
            category=category,
            alert_count=total,
            environment=environment,
            payload=_alertmanager_payload(
                batches=included,
                status="firing",
                environment=environment,
            ),
        )

        sent = cfg.transport.send(message, to=to_email, from_addr=from_email, from_name=from_name)

        if sent:
            await _mark_notified(session, [(b.condition_id, b.max_id) for b in included])
            # Commit the mark NOW, per email. The transport has already
            # delivered it; if the mark rode along to a single end-of-sweep
            # commit, any later failure (another category's send, the digest
            # queries, the commit itself) would roll it back and the next
            # sweep would re-email a message the recipient already has — and,
            # because the throttle window is computed from notified rows,
            # re-email it IMMEDIATELY. A sent notification must be unlosable
            # the moment it is sent.
            await session.commit()
            stats["immediate_sent"] += 1

    stats.update(
        await _deliver_immediate_legacy(
            session, to_email, from_email, from_name, throttle_minutes, stats
        )
    )
    return stats


async def _deliver_immediate_legacy(
    session,
    to_email: str,
    from_email: str,
    from_name: str,
    throttle_minutes: int,
    stats: dict,
) -> dict:
    """The original category-grouped path, for occurrences with no condition.

    Untouched in shape: an orphan occurrence is still delivered, still grouped
    by category, still throttled by that category's last notified row. That is
    what makes a fire-time resolution failure cost nothing but grouping (F1).
    """
    cfg = get_config()
    environment = cfg.environment
    immediate_severities = [s.value for s in IMMEDIATE_SEVERITIES]
    throttle_cutoff = datetime.now(UTC) - timedelta(minutes=throttle_minutes)

    last_notified = (
        select(
            Alert.category.label("cat"),
            func.max(Alert.created).label("last_notified_at"),
        )
        .where(
            Alert.notified.is_(True),
            Alert.severity.in_(immediate_severities),
            Alert.condition_id.is_(None),
        )
        .group_by(Alert.category)
        .subquery("last_notified")
    )

    ranked = (
        select(
            Alert.category,
            Alert.message,
            func.row_number()
            .over(partition_by=Alert.category, order_by=Alert.created.desc())
            .label("rn"),
        )
        .where(
            Alert.notified.is_(False),
            Alert.severity.in_(immediate_severities),
            Alert.condition_id.is_(None),
        )
        .cte("ranked")
    )

    query = (
        select(
            Alert.category,
            func.max(_SEVERITY_RANK).label("severity_rank"),
            func.count(Alert.id).label("count"),
            func.max(Alert.id).label("max_id"),
            ranked.c.message.label("latest_message"),
            last_notified.c.last_notified_at,
        )
        .where(
            Alert.notified.is_(False),
            Alert.severity.in_(immediate_severities),
            Alert.condition_id.is_(None),
        )
        .outerjoin(last_notified, Alert.category == last_notified.c.cat)
        .outerjoin(
            ranked,
            and_(Alert.category == ranked.c.category, ranked.c.rn == 1),
        )
        .group_by(
            Alert.category,
            ranked.c.message,
            last_notified.c.last_notified_at,
        )
    )

    result = await session.execute(query)
    out = {
        "immediate_sent": stats["immediate_sent"],
        "immediate_throttled": stats["immediate_throttled"],
        "immediate_throttled_conditions": stats["immediate_throttled_conditions"],
    }

    for row in result.all():
        if (
            throttle_minutes > 0
            and row.last_notified_at is not None
            and row.last_notified_at.replace(tzinfo=None) > throttle_cutoff.replace(tzinfo=None)
        ):
            out["immediate_throttled"] += 1
            continue

        worst_severity = _RANK_TO_SEVERITY.get(row.severity_rank, AlertSeverity.ERROR.value)
        latest_msg = row.latest_message or ""
        message = AlertMessage(
            subject=(
                f"{_subject_prefix(environment)}"
                f"[{worst_severity.upper()}] {row.category}: {latest_msg[:60]}"
            ),
            html_body=_render_legacy_email(
                category=row.category,
                severity=worst_severity,
                count=row.count,
                latest_message=latest_msg,
                environment=environment,
            ),
            text_body=(
                f"{_environment_text(environment)}"
                f"{worst_severity.upper()} — {row.category}: "
                f"{latest_msg} ({row.count} occurrences)"
            ),
            severity=worst_severity,
            category=row.category,
            alert_count=row.count,
            environment=environment,
        )

        sent = cfg.transport.send(message, to=to_email, from_addr=from_email, from_name=from_name)

        if sent:
            await session.execute(
                update(Alert)
                .where(
                    Alert.category == row.category,
                    Alert.severity.in_(immediate_severities),
                    Alert.notified.is_(False),
                    Alert.condition_id.is_(None),
                    Alert.id <= row.max_id,
                )
                .values(notified=True)
            )
            # Commit-per-send, same rule as above.
            await session.commit()
            out["immediate_sent"] += 1

    return out


async def _deliver_digest(
    session,
    batches: list[_ConditionBatch],
    to_email: str,
    from_email: str,
    from_name: str,
) -> dict:
    """One digest email covering digest-dispositioned conditions and orphans."""
    stats = {"digest_sent": 0, "digest_count": 0}
    cfg = get_config()
    environment = cfg.environment

    # Digest interval gating (#10): skip if the last digest was sent too recently.
    digest_interval_minutes = _resolve_setting("delivery_digest_interval_minutes", 360)
    if digest_interval_minutes > 0:
        state = await session.get(AlertDeliveryState, 1)
        if state and state.last_digest_sent_at is not None:
            last_sent = state.last_digest_sent_at
            now_naive = datetime.now(UTC).replace(tzinfo=None)
            last_naive = last_sent.replace(tzinfo=None) if last_sent.tzinfo else last_sent
            elapsed = now_naive - last_naive
            if elapsed < timedelta(minutes=digest_interval_minutes):
                return stats

    digest_severities = [s.value for s in DIGEST_SEVERITIES]

    # Orphan (no condition) warn occurrences still ride the legacy grouping.
    ranked = (
        select(
            Alert.category,
            Alert.message,
            func.row_number()
            .over(partition_by=Alert.category, order_by=Alert.created.desc())
            .label("rn"),
        )
        .where(
            Alert.notified.is_(False),
            Alert.severity.in_(digest_severities),
            Alert.condition_id.is_(None),
        )
        .cte("ranked_digest")
    )
    legacy_rows = (
        await session.execute(
            select(
                Alert.category,
                func.count(Alert.id).label("count"),
                func.max(Alert.id).label("max_id"),
                ranked.c.message.label("latest_message"),
            )
            .where(
                Alert.notified.is_(False),
                Alert.severity.in_(digest_severities),
                Alert.condition_id.is_(None),
            )
            .outerjoin(
                ranked,
                and_(Alert.category == ranked.c.category, ranked.c.rn == 1),
            )
            .group_by(Alert.category, ranked.c.message)
        )
    ).all()

    rows: dict[str, _DigestRow] = {}
    for batch in batches:
        row = rows.setdefault(
            batch.category, _DigestRow(batch.category, batch.latest_message, 0)
        )
        row.count += batch.count
        row.latest_message = batch.latest_message
    for legacy in legacy_rows:
        row = rows.setdefault(
            legacy.category, _DigestRow(legacy.category, legacy.latest_message or "", 0)
        )
        row.count += legacy.count

    if not rows:
        return stats

    categories = list(rows.values())
    total_count = sum(row.count for row in categories)
    stats["digest_count"] = total_count

    # P5 routes ACKNOWLEDGED error and critical conditions into this digest, so
    # it is not a warning digest — saying so misinforms about the one thing an
    # alert email exists to convey, and lies to any transport that keys off
    # ``message.severity`` (Slack colour, PagerDuty priority). Legacy orphan
    # rows are warn by construction (DIGEST_SEVERITIES); condition batches
    # carry their own severity.
    worst = _worst_severity([b.severity for b in batches])
    worst_suffix = (
        f" — worst: {worst.upper()}"
        if _SEVERITY_ORDER.get(worst, 0) > _SEVERITY_ORDER[AlertSeverity.WARN.value]
        else ""
    )

    message = AlertMessage(
        subject=(
            f"{_subject_prefix(environment)}[ALERT DIGEST] "
            f"{total_count} alert(s) across {len(categories)} categorie(s)"
            f"{worst_suffix}"
        ),
        html_body=_render_digest_email(categories, environment=environment),
        text_body=(
            f"{_environment_text(environment)}"
            f"Alert Digest: {total_count} alert(s) across {len(categories)} "
            f"categories{worst_suffix}"
        ),
        severity=worst,
        category="digest",
        alert_count=total_count,
        environment=environment,
    )

    sent = cfg.transport.send(message, to=to_email, from_addr=from_email, from_name=from_name)

    if sent:
        await _mark_notified(session, [(b.condition_id, b.max_id) for b in batches])
        if legacy_rows:
            await session.execute(
                update(Alert)
                .where(
                    Alert.severity.in_(digest_severities),
                    Alert.notified.is_(False),
                    Alert.condition_id.is_(None),
                    Alert.id <= max(r.max_id for r in legacy_rows),
                )
                .values(notified=True)
            )
        # Record when the digest was sent for interval gating (#10).
        await _update_digest_sent_at(session)
        # Same rule as immediate delivery: the digest email is out the door,
        # so its mark must not be able to roll back with the caller's
        # transaction. Commit it in place.
        await session.commit()
        stats["digest_sent"] = 1

    return stats


def _render_immediate_email(
    *,
    category: str,
    severity: str,
    count: int,
    conditions: list[_ConditionBatch],
    environment: str | None = None,
) -> str:
    """Render HTML for a category email listing its conditions."""
    color = "#dc3545" if severity == AlertSeverity.CRITICAL else "#fd7e14"
    cell = "padding: 8px; border-bottom: 1px solid #eee;"
    listed = conditions[:_CONDITION_LIST_CAP]
    rows = "".join(
        f"""
        <tr>
            <td style="{cell}">#{c.condition_id}</td>
            <td style="{cell}">{c.template}</td>
            <td style="{cell} text-align: center;">{c.count}</td>
        </tr>
        """
        for c in listed
    )
    remainder = len(conditions) - len(listed)
    more = (
        f'<p style="color: #666;">…and {remainder} more condition(s) in this category.</p>'
        if remainder > 0
        else ""
    )
    return f"""
    <div style="font-family: sans-serif; max-width: 600px;">{_environment_html(environment)}
        <h2 style="color: {color};">
            {severity.upper()} Alert — {category}
        </h2>
        <table style="border-collapse: collapse; width: 100%;">
            <thead>
                <tr style="background: #f8f9fa;">
                    <th style="padding: 8px; text-align: left;">Condition</th>
                    <th style="padding: 8px; text-align: left;">What is wrong</th>
                    <th style="padding: 8px; text-align: center;">New</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
        {more}
        <table style="border-collapse: collapse; margin-top: 12px;">
            <tr><td style="padding: 4px 12px 4px 0; color: #666;">Category:</td>
                <td>{category}</td></tr>
            <tr><td style="padding: 4px 12px 4px 0; color: #666;">Occurrences:</td>
                <td>{count}</td></tr>
        </table>
    </div>
    """


def _render_legacy_email(
    *,
    category: str,
    severity: str,
    count: int,
    latest_message: str,
    environment: str | None = None,
) -> str:
    """Render HTML for a category email of condition-less occurrences."""
    color = "#dc3545" if severity == AlertSeverity.CRITICAL else "#fd7e14"
    return f"""
    <div style="font-family: sans-serif; max-width: 600px;">{_environment_html(environment)}
        <h2 style="color: {color};">
            {severity.upper()} Alert — {category}
        </h2>
        <p style="font-size: 16px;">{latest_message}</p>
        <table style="border-collapse: collapse; margin-top: 12px;">
            <tr><td style="padding: 4px 12px 4px 0; color: #666;">Category:</td>
                <td>{category}</td></tr>
            <tr><td style="padding: 4px 12px 4px 0; color: #666;">Occurrences:</td>
                <td>{count}</td></tr>
        </table>
    </div>
    """


def _render_digest_email(categories, *, environment: str | None = None) -> str:
    """Render HTML for a digest email containing multiple warning categories."""
    rows = ""
    cell = "padding: 8px; border-bottom: 1px solid #eee;"
    for row in categories:
        msg = (row.latest_message or "")[:80]
        rows += f"""
        <tr>
            <td style="{cell}">{row.category}</td>
            <td style="{cell}">{msg}</td>
            <td style="{cell} text-align: center;">{row.count}</td>
        </tr>
        """

    total = sum(r.count for r in categories)
    return f"""
    <div style="font-family: sans-serif; max-width: 600px;">{_environment_html(environment)}
        <h2 style="color: #ffc107;">Alert Digest — {total} Warning(s)</h2>
        <table style="border-collapse: collapse; width: 100%;">
            <thead>
                <tr style="background: #f8f9fa;">
                    <th style="padding: 8px; text-align: left;">Category</th>
                    <th style="padding: 8px; text-align: left;">Latest Message</th>
                    <th style="padding: 8px; text-align: center;">Count</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
    </div>
    """
