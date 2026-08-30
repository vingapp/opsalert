"""Alert cleanup — TTL-based deletion of old occurrences.

Conditions are never deleted here. An occurrence is a fact with a retention
clock; the condition is the record of a problem, and its counters
(``occurrence_count``, ``first_seen``, ``last_seen``) are exactly what makes
pruning safe — the history survives the rows.

Plain async function — no scheduler dependency. The host app wraps
this in whatever scheduler it uses.
"""
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, or_, select

from opsalert._config import _resolve_setting
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

    Returns dict with 'deleted' count.
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

    return {"deleted": deleted}
