"""Alert cleanup — TTL-based deletion of old alerts.

Plain async function — no scheduler dependency. The host app wraps
this in whatever scheduler it uses.
"""
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete

from opsalert._config import _resolve_setting
from opsalert.model import Alert

logger = logging.getLogger(__name__)


async def cleanup_alerts(session) -> dict:
    """Delete alerts older than retention_max_age_days. Call from your scheduler.

    Returns dict with 'deleted' count.
    """
    max_age_days = _resolve_setting("retention_max_age_days", 90)

    cutoff = datetime.now(UTC) - timedelta(days=max_age_days)
    result = await session.execute(
        delete(Alert).where(Alert.created < cutoff)
    )
    deleted = result.rowcount

    if deleted > 0:
        logger.info("Deleted %d alerts older than %d days", deleted, max_age_days)

    return {"deleted": deleted}
