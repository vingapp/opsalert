"""Alert delivery — batched notification with throttling.

Sends alert notification emails throttled by category:
- ERROR/CRITICAL: individual email per category (with count), throttled
- WARN: batched into periodic digest emails

Performance fix: single query with LEFT JOIN for throttle check
(replaces N+1 pattern of one throttle-check query per category).

This module provides plain async functions — no scheduler dependency.
The host app wraps these in whatever scheduler it uses.
"""
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, case, func, select, update

from opsalert._config import _resolve_setting, get_config
from opsalert.model import Alert
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


async def deliver_alerts(session) -> dict:
    """Deliver alert notification emails. Call from your scheduler.

    Returns stats dict with immediate_sent, immediate_throttled,
    digest_sent, digest_count.
    """
    stats = {
        "immediate_sent": 0,
        "immediate_throttled": 0,
        "digest_sent": 0,
        "digest_count": 0,
    }

    enabled = _resolve_setting("delivery_enabled", True)
    if not enabled:
        return stats

    to_email = _resolve_setting("delivery_to_email", "")
    from_email = _resolve_setting("delivery_from_email", "")
    from_name = _resolve_setting("delivery_from_name", "OpsAlert")
    throttle_minutes = _resolve_setting("delivery_throttle_minutes", 60)

    stats.update(
        await _deliver_immediate(session, to_email, from_email, from_name, throttle_minutes)
    )
    stats.update(
        await _deliver_digest(session, to_email, from_email, from_name)
    )

    total = stats["immediate_sent"] + stats["digest_sent"]
    if total > 0:
        logger.info(
            "Alert delivery: %d immediate, %d digest (%d warnings)",
            stats["immediate_sent"],
            stats["digest_sent"],
            stats["digest_count"],
        )

    return stats


async def _deliver_immediate(
    session, to_email: str, from_email: str, from_name: str, throttle_minutes: int
) -> dict:
    """Send one email per unnotified ERROR/CRITICAL category.

    Performance fix: single query with LEFT JOIN for throttle check,
    replacing N+1 per-category throttle queries.
    """
    stats = {"immediate_sent": 0, "immediate_throttled": 0}
    cfg = get_config()

    if cfg.transport is None:
        return stats

    immediate_severities = [s.value for s in IMMEDIATE_SEVERITIES]
    throttle_cutoff = datetime.now(UTC) - timedelta(minutes=throttle_minutes)

    # Subquery: latest notified alert per category (for throttle check)
    last_notified = (
        select(
            Alert.category.label("cat"),
            func.max(Alert.created).label("last_notified_at"),
        )
        .where(Alert.notified.is_(True), Alert.severity.in_(immediate_severities))
        .group_by(Alert.category)
        .subquery("last_notified")
    )

    # CTE: latest message per unnotified category
    ranked = (
        select(
            Alert.category,
            Alert.message,
            func.row_number()
            .over(partition_by=Alert.category, order_by=Alert.created.desc())
            .label("rn"),
        )
        .where(Alert.notified.is_(False), Alert.severity.in_(immediate_severities))
        .cte("ranked")
    )

    # Main query: unnotified categories with counts + throttle info in one pass
    query = (
        select(
            Alert.category,
            func.max(_SEVERITY_RANK).label("severity_rank"),
            func.count(Alert.id).label("count"),
            ranked.c.message.label("latest_message"),
            last_notified.c.last_notified_at,
        )
        .where(Alert.notified.is_(False), Alert.severity.in_(immediate_severities))
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
    categories = result.all()

    for row in categories:
        # Throttle: skip if notified recently.
        # Normalize to naive UTC for comparison (some DBs return naive datetimes).
        if (
            throttle_minutes > 0
            and row.last_notified_at is not None
            and row.last_notified_at.replace(tzinfo=None) > throttle_cutoff.replace(tzinfo=None)
        ):
            stats["immediate_throttled"] += 1
            continue

        worst_severity = _RANK_TO_SEVERITY.get(row.severity_rank, AlertSeverity.ERROR.value)
        latest_msg = row.latest_message or ""
        subject = f"[{worst_severity.upper()}] {row.category}: {latest_msg[:60]}"
        html = _render_immediate_email(
            category=row.category,
            severity=worst_severity,
            count=row.count,
            latest_message=latest_msg,
        )

        message = AlertMessage(
            subject=subject,
            html_body=html,
            text_body=(
                f"{worst_severity.upper()} — {row.category}: "
                f"{latest_msg} ({row.count} occurrences)"
            ),
            severity=worst_severity,
            category=row.category,
            alert_count=row.count,
        )

        sent = cfg.transport.send(message, to=to_email, from_addr=from_email, from_name=from_name)

        if sent:
            await session.execute(
                update(Alert)
                .where(
                    Alert.category == row.category,
                    Alert.severity.in_(immediate_severities),
                    Alert.notified.is_(False),
                )
                .values(notified=True)
            )
            stats["immediate_sent"] += 1

    return stats


async def _deliver_digest(
    session, to_email: str, from_email: str, from_name: str
) -> dict:
    """Send periodic digest email for WARN alerts."""
    stats = {"digest_sent": 0, "digest_count": 0}
    cfg = get_config()

    if cfg.transport is None:
        return stats

    digest_severities = [s.value for s in DIGEST_SEVERITIES]

    # CTE: latest message per unnotified warn category
    ranked = (
        select(
            Alert.category,
            Alert.message,
            func.row_number()
            .over(partition_by=Alert.category, order_by=Alert.created.desc())
            .label("rn"),
        )
        .where(Alert.notified.is_(False), Alert.severity.in_(digest_severities))
        .cte("ranked_digest")
    )

    # Unnotified warning categories with counts
    result = await session.execute(
        select(
            Alert.category,
            func.count(Alert.id).label("count"),
            ranked.c.message.label("latest_message"),
        )
        .where(Alert.notified.is_(False), Alert.severity.in_(digest_severities))
        .outerjoin(
            ranked,
            and_(Alert.category == ranked.c.category, ranked.c.rn == 1),
        )
        .group_by(Alert.category, ranked.c.message)
    )
    categories = result.all()

    if not categories:
        return stats

    total_count = sum(row.count for row in categories)
    stats["digest_count"] = total_count

    subject = f"[ALERT DIGEST] {total_count} warning(s) across {len(categories)} categorie(s)"
    html = _render_digest_email(categories)

    message = AlertMessage(
        subject=subject,
        html_body=html,
        text_body=f"Alert Digest: {total_count} warning(s) across {len(categories)} categories",
        severity="warn",
        category="digest",
        alert_count=total_count,
    )

    sent = cfg.transport.send(message, to=to_email, from_addr=from_email, from_name=from_name)

    if sent:
        await session.execute(
            update(Alert)
            .where(
                Alert.severity.in_(digest_severities),
                Alert.notified.is_(False),
            )
            .values(notified=True)
        )
        stats["digest_sent"] = 1

    return stats


def _render_immediate_email(
    *, category: str, severity: str, count: int, latest_message: str
) -> str:
    """Render HTML for an individual category alert email."""
    color = "#dc3545" if severity == AlertSeverity.CRITICAL else "#fd7e14"
    return f"""
    <div style="font-family: sans-serif; max-width: 600px;">
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


def _render_digest_email(categories) -> str:
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
    <div style="font-family: sans-serif; max-width: 600px;">
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
