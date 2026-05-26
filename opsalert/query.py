"""Query API — dashboard selectors, next-fix, aggregates, and delete.

Level 1 (default): GROUP BY category → count, severity, source, latest_message
Level 2 (?category=X): GROUP BY message within category → count, latest_created
Level 3 (?category=X&message=Y): Individual occurrences with context
Next-fix: Highest-priority group with aggregated debugging data
"""
import json
from typing import TYPE_CHECKING

from sqlalchemy import select, func, desc, case, delete

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from opsalert.model import Alert
from opsalert.types import AlertSeverity


# Map severity strings to numeric rank for proper MAX ordering.
# func.max() on strings is lexicographic — 'warn' > 'error' > 'critical'.
# We need 'critical' > 'error' > 'warn'.
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


async def query_categories(
    session: "AsyncSession",
    *,
    severity: str | None = None,
    source: str | None = None,
    search: str | None = None,
) -> list[dict]:
    """Level 1: GROUP BY category, returns summary per category.

    The previous implementation used a ROW_NUMBER() OVER (PARTITION BY
    category) window function CTE that MySQL materialised over the entire
    ``opsalert`` table; on a busy instance that overflowed the server's
    tmpdir ("table '#sql...' is full"). The aggregation CTE is bounded by
    the number of categories (small), and the latest message per category
    comes from a correlated scalar subquery that rides the
    ``(category, created)`` index — O(K log N) instead of an O(N) sort+spill.

    Returns list of dicts with: category, severity (worst), source, count,
    latest_message, latest_created.
    """
    # Aggregation: one row per category. Filters apply here.
    agg_query = select(
        Alert.category,
        func.max(_SEVERITY_RANK).label("severity_rank"),
        func.max(Alert.source).label("source"),
        func.count(Alert.id).label("count"),
        func.max(Alert.created).label("latest_created"),
    ).group_by(Alert.category)

    if severity:
        agg_query = agg_query.where(Alert.severity == severity)
    if source:
        agg_query = agg_query.where(Alert.source == source)
    if search:
        agg_query = agg_query.where(Alert.message.ilike(f"%{search}%"))

    agg_cte = agg_query.cte("agg")

    # Latest message per category — correlated scalar subquery. Filters
    # are mirrored so the message reflects what passed the filter set.
    latest_msg_subq = (
        select(Alert.message)
        .where(Alert.category == agg_cte.c.category)
    )
    if severity:
        latest_msg_subq = latest_msg_subq.where(Alert.severity == severity)
    if source:
        latest_msg_subq = latest_msg_subq.where(Alert.source == source)
    if search:
        latest_msg_subq = latest_msg_subq.where(Alert.message.ilike(f"%{search}%"))
    latest_msg_subq = (
        latest_msg_subq.order_by(Alert.created.desc()).limit(1).scalar_subquery()
    )

    final = (
        select(
            agg_cte.c.category,
            agg_cte.c.severity_rank,
            agg_cte.c.source,
            agg_cte.c.count,
            latest_msg_subq.label("latest_message"),
            agg_cte.c.latest_created,
        )
        .order_by(desc(agg_cte.c.latest_created))
    )

    result = await session.execute(final)
    return [
        {
            "category": row.category,
            "severity": _RANK_TO_SEVERITY.get(row.severity_rank, AlertSeverity.WARN.value),
            "source": row.source,
            "count": row.count,
            "latest_message": row.latest_message,
            "latest_created": row.latest_created,
        }
        for row in result.all()
    ]


async def query_messages(
    session: "AsyncSession",
    *,
    category: str,
    severity: str | None = None,
    search: str | None = None,
) -> list[dict]:
    """Level 2: GROUP BY message within a category.

    Returns list of dicts with: message, count, latest_created.
    """
    query = (
        select(
            Alert.message,
            func.count(Alert.id).label("count"),
            func.max(Alert.created).label("latest_created"),
        )
        .where(Alert.category == category)
        .group_by(Alert.message)
        .order_by(desc("latest_created"))
    )

    if severity:
        query = query.where(Alert.severity == severity)
    if search:
        query = query.where(Alert.message.ilike(f"%{search}%"))

    result = await session.execute(query)
    return [
        {
            "message": row.message,
            "count": row.count,
            "latest_created": row.latest_created,
        }
        for row in result.all()
    ]


async def query_occurrences(
    session: "AsyncSession",
    *,
    category: str | None = None,
    message: str | None = None,
    severity: str | None = None,
    source: str | None = None,
    search: str | None = None,
    sort: str = "-created",
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Level 3: Individual occurrences with pagination.

    Returns (items, total_count). Items are dicts (not ORM objects)
    so the host app doesn't need to import the model for serialization.
    """
    base_filters = []
    if category:
        base_filters.append(Alert.category == category)
    if message:
        base_filters.append(Alert.message == message)
    if severity:
        base_filters.append(Alert.severity == severity)
    if source:
        base_filters.append(Alert.source == source)
    if search:
        base_filters.append(Alert.message.ilike(f"%{search}%"))

    # Count
    count_query = select(func.count(Alert.id))
    for f in base_filters:
        count_query = count_query.where(f)
    total = (await session.execute(count_query)).scalar() or 0

    # Items
    query = select(Alert)
    for f in base_filters:
        query = query.where(f)

    # Sorting
    is_desc = sort.startswith("-")
    sort_field = sort.lstrip("-")
    sort_map = {
        "created": Alert.created,
        "severity": Alert.severity,
        "category": Alert.category,
        "message": Alert.message,
    }
    col = sort_map.get(sort_field, Alert.created)
    query = query.order_by(col.desc() if is_desc else col.asc())
    query = query.offset(offset).limit(limit)

    result = await session.execute(query)
    items = [
        {
            "id": a.id,
            "severity": a.severity,
            "category": a.category,
            "source": a.source,
            "message": a.message,
            "context_json": a.context_json,
            "notified": a.notified,
            "created": a.created,
        }
        for a in result.scalars().all()
    ]

    return items, total


async def query_by_trace_id(
    session: "AsyncSession",
    trace_id: str,
    *,
    limit: int = 50,
) -> list[dict]:
    """Find alerts whose context_json contains a specific _trace_id.

    Uses JSON_EXTRACT on the Text column (works in MySQL 5.7+ and SQLite 3.38+).
    """
    query = (
        select(Alert)
        .where(func.json_extract(Alert.context_json, "$._trace_id") == trace_id)
        .order_by(desc(Alert.created))
        .limit(limit)
    )
    result = await session.execute(query)
    return [
        {
            "id": a.id,
            "severity": a.severity,
            "category": a.category,
            "source": a.source,
            "message": a.message,
            "context_json": a.context_json,
            "created": a.created,
        }
        for a in result.scalars().all()
    ]


async def query_aggregates(session: "AsyncSession") -> dict:
    """Aggregate statistics for the alert dashboard.

    Returns dict with total count and by_severity breakdown.
    """
    result = await session.execute(
        select(func.count(Alert.id).label("total"))
    )
    total = result.scalar() or 0

    severity_result = await session.execute(
        select(
            Alert.severity,
            func.count(Alert.id),
        ).group_by(Alert.severity)
    )
    by_severity = {sev: count for sev, count in severity_result.all()}

    return {
        "total": total,
        "by_severity": by_severity,
    }


async def query_next_fix(
    session: "AsyncSession",
    *,
    max_samples: int = 5,
    max_occurrences: int = 200,
) -> dict | None:
    """Find highest-priority alert group with aggregated debugging data.

    Priority: CRITICAL > ERROR > WARN, then oldest first.
    Returns None if no alerts exist.

    Performance fix: LIMIT on occurrence fetch (was unbounded), selects only
    context_json column to minimize data transfer.

    The result includes unique code locations (_caller), exception signatures,
    tracebacks, and sample caller-provided contexts — everything a developer
    needs to diagnose and fix the issue.
    """
    # Query A: find the top-priority (category, message) group.
    top = (
        select(
            Alert.category,
            Alert.message,
            func.max(_SEVERITY_RANK).label("severity_rank"),
            func.count(Alert.id).label("count"),
            func.min(Alert.created).label("first_created"),
            func.max(Alert.created).label("latest_created"),
            func.max(Alert.source).label("source"),
        )
        .group_by(Alert.category, Alert.message)
        .order_by(
            desc("severity_rank"),
            "first_created",
        )
        .limit(1)
    )
    row = (await session.execute(top)).one_or_none()
    if row is None:
        return None

    # Query B: load context_json for occurrences (paginated, not unbounded).
    occ_result = await session.execute(
        select(Alert.context_json)
        .where(Alert.category == row.category, Alert.message == row.message)
        .order_by(Alert.created.desc())
        .limit(max_occurrences)
    )
    contexts = occ_result.scalars().all()

    # Aggregate debugging info from context_json.
    callers: set[str] = set()
    exc_sigs: set[str] = set()
    tracebacks: list[str] = []
    samples: list[dict] = []

    for ctx_json in contexts:
        if not ctx_json:
            continue
        try:
            ctx = json.loads(ctx_json)
        except (json.JSONDecodeError, TypeError):
            continue

        if "_caller" in ctx:
            callers.add(ctx["_caller"])

        sig = f"{ctx.get('_exc_type', '')}:{ctx.get('_exc_message', '')}"
        if sig != ":" and sig not in exc_sigs:
            exc_sigs.add(sig)
            if ctx.get("_traceback") and len(tracebacks) < 3:
                tracebacks.append(ctx["_traceback"])

        if len(samples) < max_samples:
            user_ctx = {k: v for k, v in ctx.items() if not k.startswith("_")}
            if user_ctx:
                samples.append(user_ctx)

    # Resolve fix hint from configured hints (defensive for unconfigured state)
    try:
        from opsalert._config import get_config
        cfg = get_config()
        fix_hint = cfg.fix_hints.get(row.category, cfg.default_fix_hint)
    except RuntimeError:
        fix_hint = "Examine the tracebacks and code locations above."

    return {
        "category": row.category,
        "message": row.message,
        "severity": _RANK_TO_SEVERITY.get(row.severity_rank, "warn"),
        "count": row.count,
        "source": row.source,
        "first_created": row.first_created,
        "latest_created": row.latest_created,
        "callers": sorted(callers),
        "exception_signatures": sorted(exc_sigs),
        "tracebacks": tracebacks,
        "sample_contexts": samples,
        "fix_hint": fix_hint,
    }


# =============================================================================
# Delete Operations
# =============================================================================


async def delete_by_category(
    session: "AsyncSession",
    *,
    category: str,
    message: str | None = None,
) -> int:
    """Delete alerts by category, optionally filtered by message.

    Returns the number of rows deleted.
    """
    stmt = delete(Alert).where(Alert.category == category)
    if message:
        stmt = stmt.where(Alert.message == message)

    result = await session.execute(stmt)
    return result.rowcount


async def delete_by_id(session: "AsyncSession", *, alert_id: int) -> bool:
    """Delete a single alert by ID. Returns True if found and deleted."""
    result = await session.execute(
        delete(Alert).where(Alert.id == alert_id)
    )
    return result.rowcount > 0
