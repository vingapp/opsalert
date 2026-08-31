"""Alert cleanup — TTL-based deletion of old occurrences.

A condition that ever HAD an occurrence is never deleted here. An occurrence
is a fact with a retention clock; the condition is the record of a problem,
and its counters (``occurrence_count``, ``first_seen``, ``last_seen``) are
exactly what makes pruning safe — the history survives the rows.

The one exception is a condition that never had an occurrence at all
(opsalert#2): a degraded fire can commit the condition row in its isolated
session and still fail to attach it, leaving a ``×0`` row that records
nothing. Those are reaped once they are old enough that no in-flight fire can
still be about to reference them.

Plain async functions — no scheduler dependency. The host app wraps
this in whatever scheduler it uses.
"""
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, or_, select

from opsalert._config import _resolve_setting
from opsalert.lifecycle import STATUS_NEW
from opsalert.model import Alert, AlertCondition

logger = logging.getLogger(__name__)


async def cleanup_alerts(session) -> dict:
    """Delete occurrences older than retention_max_age_days. Call from your scheduler.

    Age alone is not enough (P3). An occurrence may only go once it is
    provably counted: ``id <= its condition's stats_synced_through``. Deleting
    a row the stats sweeper had not folded in yet would silently decrement
    history — the count would be wrong and nothing would ever say so. A row
    above the watermark simply waits for the next sweep.

    Occurrences with no condition are deleted on age alone: there is no
    counter for them to be missing from. Adoption is unbounded and runs every
    sweep, so an orphan can only reach the retention edge uncounted if the
    maintenance sweeper was broken for the entire retention window — which is
    its own loud incident (F2).

    Zero-occurrence conditions are reaped in the same sweep — see
    :func:`_reap_empty_conditions`.

    Returns dict with 'deleted' and 'conditions_reaped' counts.
    """
    max_age_days = _resolve_setting("retention_max_age_days", 90)

    cutoff = datetime.now(UTC) - timedelta(days=max_age_days)
    counted_through = (
        select(AlertCondition.stats_synced_through)
        .where(AlertCondition.id == Alert.condition_id)
        .scalar_subquery()
    )
    result = await session.execute(
        delete(Alert).where(
            Alert.created < cutoff,
            or_(Alert.condition_id.is_(None), Alert.id <= counted_through),
        )
    )
    deleted = result.rowcount

    if deleted > 0:
        logger.info("Deleted %d alerts older than %d days", deleted, max_age_days)

    reaped = await _reap_empty_conditions(session)

    return {"deleted": deleted, "conditions_reaped": reaped}


async def _reap_empty_conditions(session, *, now: datetime | None = None) -> int:
    """Delete conditions that never had an occurrence (opsalert#2).

    A degraded fire (F1) can commit the condition row in its isolated session
    and still store the occurrence as an orphan; adoption then links the
    orphan by the template stored on the row, and the fire-time condition is
    left claiming ``×0`` forever. It records nothing, so it is the one kind
    of condition that is safe — and correct — to delete.

    Guards, all mandatory:

    - no occurrence references it (``NOT EXISTS``, checked live) and none was
      ever counted (``occurrence_count == 0`` — pruned occurrences leave their
      counters behind, so a condition whose rows were all pruned stays);
    - untouched by a human (still ``new``, no notes, no issue url) — an
      annotated row is a record even when empty;
    - older than ``condition_empty_reap_minutes`` (default 60), so a fire
      that just resolved this condition and is about to flush its occurrence
      cannot have the row deleted out from under it.
    """
    minutes = _resolve_setting("condition_empty_reap_minutes", 60)
    cutoff = (now or datetime.now(UTC)) - timedelta(minutes=minutes)

    has_occurrence = (
        select(Alert.id).where(Alert.condition_id == AlertCondition.id).exists()
    )
    result = await session.execute(
        delete(AlertCondition).where(
            AlertCondition.created < cutoff,
            AlertCondition.occurrence_count == 0,
            AlertCondition.status == STATUS_NEW,
            AlertCondition.notes.is_(None),
            AlertCondition.issue_url.is_(None),
            ~has_occurrence,
        )
    )
    reaped = result.rowcount

    if reaped > 0:
        logger.info(
            "Reaped %d empty condition(s) older than %d minutes", reaped, minutes
        )

    return reaped
