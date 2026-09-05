"""Query API — dashboard selectors, next-fix, aggregates, and delete.

Level 1 (default): GROUP BY category → count, severity, source, latest_message
Level 2 (?category=X): GROUP BY message within category → count, latest_created
Level 3 (?category=X&message=Y): Individual occurrences with context
Next-fix: Highest-priority group with aggregated debugging data
"""
import base64
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import case, delete, desc, func, or_, select

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from opsalert.lifecycle import (
    STATUS_NEW,
    effective_disposition,
)
from opsalert.model import Alert, AlertCondition
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
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Level 2: GROUP BY message within a category.

    Returns (items, total_groups). Items are dicts with: message, count,
    latest_created — paginated by ``limit``/``offset``. ``total_groups`` is
    the number of distinct messages matching the filters (the full,
    unpaginated group count).

    Pagination is mandatory here: a category whose messages embed unique
    identifiers (e.g. task ids) produces one group per occurrence, so an
    unbounded GROUP BY can return tens of thousands of rows and stall the
    endpoint. See the #orphan-flood incident.
    """
    base_filters = [Alert.category == category]
    if severity:
        base_filters.append(Alert.severity == severity)
    if search:
        base_filters.append(Alert.message.ilike(f"%{search}%"))

    # Total number of distinct messages (groups) matching the filters.
    count_query = select(func.count(func.distinct(Alert.message)))
    for f in base_filters:
        count_query = count_query.where(f)
    total = (await session.execute(count_query)).scalar() or 0

    query = select(
        Alert.message,
        func.count(Alert.id).label("count"),
        func.max(Alert.created).label("latest_created"),
    )
    for f in base_filters:
        query = query.where(f)
    query = (
        query.group_by(Alert.message)
        .order_by(desc("latest_created"))
        .offset(offset)
        .limit(limit)
    )

    result = await session.execute(query)
    items = [
        {
            "message": row.message,
            "count": row.count,
            "latest_created": row.latest_created,
        }
        for row in result.all()
    ]
    return items, total


async def query_occurrences(
    session: "AsyncSession",
    *,
    category: str | None = None,
    message: str | None = None,
    severity: str | None = None,
    source: str | None = None,
    search: str | None = None,
    condition_id: int | None = None,
    sort: str = "-created",
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Level 3: Individual occurrences with pagination.

    Returns (items, total_count). Items are dicts (not ORM objects)
    so the host app doesn't need to import the model for serialization.

    ``condition_id`` is the drill-down from the conditions list: "show me the
    occurrences of THIS problem".
    """
    base_filters = []
    if condition_id is not None:
        base_filters.append(Alert.condition_id == condition_id)
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
            "condition_id": a.condition_id,
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

    Occurrences whose condition is acknowledged, resolved or closed are
    excluded (P11): "what should I fix next" must not keep handing back the
    thing somebody already picked up or already fixed. Occurrences with no
    condition behave exactly as they did before.
    """
    triageable = _open_condition_filter()

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
        .where(triageable)
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
        .where(Alert.category == row.category, Alert.message == row.message, triageable)
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
# Condition queries
# =============================================================================


def _open_condition_filter():
    """Occurrences that are still somebody's problem.

    True for orphans (no condition — legacy behaviour) and for occurrences of
    a condition still in ``new``.
    """
    return or_(
        Alert.condition_id.is_(None),
        Alert.condition_id.in_(
            select(AlertCondition.id).where(AlertCondition.status == STATUS_NEW)
        ),
    )


def _scoped_environment(environment: str | None) -> str | None:
    """Resolve the environment a condition query is scoped to (P9).

    Explicit argument wins; otherwise the configured deployment. A condition
    list that mixes staging and production is a list nobody can act on, and
    resolving the staging copy must never silence production.
    """
    if environment is not None:
        return environment
    try:
        from opsalert._config import get_config

        return get_config().environment
    except (RuntimeError, ImportError):
        # Not configured (a host app that only queries, a test suite): no
        # scope to apply. Any other failure is a real bug and must surface.
        return None


def _condition_dict(condition: AlertCondition) -> dict:
    return {
        "id": condition.id,
        "signature_key": condition.signature_key,
        "category": condition.category,
        "source": condition.source,
        "environment": condition.environment,
        "template": condition.message_template,
        "status": condition.status,
        "disposition": condition.disposition,
        "effective_disposition": effective_disposition(
            condition.severity, condition.disposition
        ),
        "severity": condition.severity,
        "latest_severity": condition.latest_severity,
        "issue_url": condition.issue_url,
        "resolved_by": condition.resolved_by,
        "notes": condition.notes,
        "occurrence_count": condition.occurrence_count,
        "reopened_count": condition.reopened_count,
        "median_interval_seconds": condition.median_interval_seconds,
        "first_seen": condition.first_seen,
        "last_seen": condition.last_seen,
        "acknowledged_at": condition.acknowledged_at,
        "acknowledged_by": condition.acknowledged_by,
        "acknowledged_severity": condition.acknowledged_severity,
        "acknowledged_occurrence_count": condition.acknowledged_occurrence_count,
        "acknowledged_until": condition.acknowledged_until,
        "status_changed_at": condition.status_changed_at,
        "resolved_at": condition.resolved_at,
        "closed_at": condition.closed_at,
        "created": condition.created,
    }


async def query_conditions(
    session: "AsyncSession",
    *,
    status: str | None = None,
    severity: str | None = None,
    category: str | None = None,
    search: str | None = None,
    environment: str | None = None,
    sort: str = "-last_seen",
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int, dict]:
    """The conditions list. Returns ``(items, total, aggregates)``.

    ``aggregates`` carries ``byStatus`` and ``bySeverity`` counts for the
    filter facets — computed over the environment scope and the non-facet
    filters, so the sidebar counts describe what clicking a facet would show.
    """
    scope = _scoped_environment(environment)

    base_filters = []
    if scope:
        base_filters.append(AlertCondition.environment == scope)
    if category:
        base_filters.append(AlertCondition.category == category)
    if search:
        base_filters.append(AlertCondition.message_template.ilike(f"%{search}%"))

    by_status_rows = (
        await session.execute(
            select(AlertCondition.status, func.count(AlertCondition.id))
            .where(*base_filters)
            .group_by(AlertCondition.status)
        )
    ).all()
    by_severity_rows = (
        await session.execute(
            select(AlertCondition.severity, func.count(AlertCondition.id))
            .where(*base_filters)
            .group_by(AlertCondition.severity)
        )
    ).all()
    aggregates = {
        "byStatus": {row[0]: row[1] for row in by_status_rows},
        "bySeverity": {row[0]: row[1] for row in by_severity_rows},
    }

    filters = list(base_filters)
    if status:
        filters.append(AlertCondition.status == status)
    if severity:
        filters.append(AlertCondition.severity == severity)

    total = (
        await session.scalar(select(func.count(AlertCondition.id)).where(*filters))
    ) or 0

    sort_map = {
        "last_seen": AlertCondition.last_seen,
        "first_seen": AlertCondition.first_seen,
        "occurrence_count": AlertCondition.occurrence_count,
        "category": AlertCondition.category,
        "status": AlertCondition.status,
        "severity": AlertCondition.severity,
        "created": AlertCondition.created,
    }
    is_desc = sort.startswith("-")
    col = sort_map.get(sort.lstrip("-"), AlertCondition.last_seen)

    rows = (
        (
            await session.execute(
                select(AlertCondition)
                .where(*filters)
                .order_by(col.desc() if is_desc else col.asc(), AlertCondition.id.desc())
                .offset(offset)
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    return [_condition_dict(c) for c in rows], total, aggregates


# The attention cursor is versioned and opaque: "2." + base64url of a JSON
# mark set. Version 1 was a bare integer occurrence watermark; it is still
# accepted for one call so a running watchdog upgrades on its next tick
# without a flood and without a lost wake.
_ATTENTION_CURSOR_V2_PREFIX = "2."


def _encode_attention_cursor(marks: dict[int, int | tuple[int, int]]) -> str:
    """Encode a {condition_id: (occurrence_count, reopened_count)} map as an opaque cursor.

    Accepts either int (legacy) or tuple values. Sorted by condition id so
    an unchanged mark set encodes to an identical string.
    """
    serialized = {}
    for cid, value in sorted(marks.items()):
        if isinstance(value, tuple):
            serialized[str(cid)] = list(value)
        else:
            # Legacy int-only mark — treat reopened_count as 0.
            serialized[str(cid)] = [value, 0]
    payload = json.dumps({"m": serialized}, separators=(",", ":"))
    encoded = base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")
    return _ATTENTION_CURSOR_V2_PREFIX + encoded.rstrip("=")


def _decode_attention_cursor(
    cursor: str | int | None,
) -> tuple[dict[int, tuple[int, int]] | None, int | None]:
    """Decode a cursor into ``(marks, legacy_watermark)``.

    Marks is ``{condition_id: (occurrence_count, reopened_count)}``.
    Exactly one of the two is not None, except on bootstrap (``None``/empty)
    where both are None. Anything that is neither a v2 string nor an integer
    raises ``ValueError`` — a malformed cursor must NOT degrade to a bootstrap,
    which would re-report the whole attention set as if it were new.
    """
    if cursor is None:
        return None, None
    if isinstance(cursor, bool):  # bool is an int; never a cursor
        raise ValueError(f"Not a valid attention cursor: {cursor!r}")
    if isinstance(cursor, int):
        return None, max(0, cursor)

    text = cursor.strip()
    if not text:
        return None, None

    try:
        return None, max(0, int(text))
    except ValueError:
        pass

    if not text.startswith(_ATTENTION_CURSOR_V2_PREFIX):
        raise ValueError(f"Not a valid attention cursor: {cursor!r}")

    body = text[len(_ATTENTION_CURSOR_V2_PREFIX) :]
    try:
        raw = base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 — every decode failure is the same
        raise ValueError(f"Not a valid attention cursor: {cursor!r}") from exc

    if not isinstance(payload, dict) or not isinstance(payload.get("m"), dict):
        raise ValueError(f"Not a valid attention cursor: {cursor!r}")

    marks: dict[int, tuple[int, int]] = {}
    for key, value in payload["m"].items():
        try:
            condition_id = int(key)
            if isinstance(value, list) and len(value) == 2:
                occ_count = int(value[0])
                reopen_count = int(value[1])
            else:
                # Legacy v2 cursor with int-only marks.
                occ_count = int(value)
                reopen_count = 0
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Not a valid attention cursor: {cursor!r}") from exc
        if occ_count < 0:
            raise ValueError(f"Not a valid attention cursor: {cursor!r}")
        marks[condition_id] = (occ_count, reopen_count)
    return marks, None


async def _count_users_24h(
    session: "AsyncSession", candidate_ids: list[int]
) -> dict[int, int]:
    """Count distinct _user_id values in context_json for each condition in the last 24h."""
    cutoff = datetime.now(UTC) - timedelta(hours=24)
    rows = (
        await session.execute(
            select(Alert.condition_id, Alert.context_json)
            .where(
                Alert.condition_id.in_(candidate_ids),
                Alert.created >= cutoff,
                Alert.context_json.is_not(None),
            )
        )
    ).all()

    # Parse user ids per condition.
    condition_users: dict[int, set[str]] = {}
    for row in rows:
        if not row.context_json:
            continue
        try:
            ctx = json.loads(row.context_json)
        except (ValueError, TypeError):
            continue
        user_id = ctx.get("_user_id")
        if user_id is not None:
            condition_users.setdefault(row.condition_id, set()).add(str(user_id))

    return {cid: len(users) for cid, users in condition_users.items()}


async def query_attention(
    session: "AsyncSession",
    *,
    cursor: str | int | None = None,
    environment: str | None = None,
    limit: int = 50,
) -> dict:
    """The watchdog's view: what should wake somebody up right now.

    Returns ``{"conditions": [...], "cursor": "<opaque string>"}``. The cursor
    is opaque — callers store it and hand it back, they never parse it.

    Only ``new`` conditions whose effective disposition is ``immediate`` are
    here (W2). Digest and collect never wake anyone — that is what choosing
    them means — and an acknowledged condition has already woken somebody.
    Conditions are returned in condition-id order.

    THE CURSOR IS THE SET OF CONDITIONS THE CALLER HAS BEEN TOLD ABOUT, EACH
    WITH HOW MANY OCCURRENCES IT HAD WHEN THEY WERE TOLD (opsalert#4). Given
    that cursor (or none), a candidate is reported when either:

    * it has **no entry in the cursor** — never reported, so it is reported
      once unconditionally, including a condition with zero occurrences; or
    * its **live occurrence count is above its recorded mark** — it fired
      again. ``count_since_cursor`` is that difference (the whole live count
      when there was no entry).

    Keying on condition identity rather than an occurrence-id watermark is what
    closes this issue: a condition that became visible late — created after a
    previous call read its candidates, adopted from orphan occurrences by a
    later sweep, or committed out of id order — has no entry, so it is reported
    regardless of where its occurrence ids fall. Keying the refire test on
    COUNT rather than max id matters for the same reason in the other
    direction: auto-increment order is not commit order, so a slow request's
    occurrence can land below ids already reported and must still count.

    Bookkeeping the returned cursor performs, all of it deliberate:

    * every candidate that was reported gets ``mark = live count``;
    * a candidate that qualified but was dropped by ``limit`` keeps the mark it
      came in with — so a never-reported one stays absent from the cursor
      entirely and is reported by the next call. Truncation skips nothing;
    * a candidate that did not qualify keeps its mark, lowered to the live
      count if occurrences were deleted underneath it;
    * marks for conditions that are no longer candidates are DROPPED, so the
      cursor cannot grow past the size of the attention set and a condition
      that leaves ``new`` and comes back (reopened, or re-dispositioned to
      ``immediate``) is reported again — that wake is wanted.

    Because marks are pruned to the current candidate set, and that set is
    scoped by ``environment``, a cursor belongs to the environment scope it was
    issued for: replaying it under a different scope prunes every mark and
    re-reports that scope's attention set. One cursor per watchdog per
    environment.

    "Nothing new" is an empty list and the caller's marks handed back. For a
    cursor this function issued, that string is byte-identical unless a mark
    was pruned or lowered — both changes in what the cursor means, so both
    have to be carried forward.

    Compatibility: a cursor that parses as an integer is read as the v1
    occurrence watermark (report candidates with an occurrence above it) for
    that one call, and a v2 cursor is returned. ``None`` or an empty string is
    a bootstrap: the current attention set plus a fresh cursor, never a flood
    of history. Anything else raises ``ValueError`` — a malformed cursor is
    never silently treated as a bootstrap, which would re-page the whole board.

    Residual, stated exactly: a mark is only reconciled against the live count
    when this function runs. If occurrences of a candidate are deleted — an
    admin purge, or retention cleanup ageing out its oldest rows — and it fires
    again before the next call, the live count can climb back toward the old
    mark without exceeding it; those refires are not reported. The call that
    sees the shortfall lowers the mark to the live count, and the next
    occurrence after that is reported. The masking is therefore bounded to one
    polling interval, not permanent.
    """
    marks_in, legacy_since = _decode_attention_cursor(cursor)
    marks: dict[int, tuple[int, int]] = dict(marks_in or {})

    scope = _scoped_environment(environment)

    filters = [AlertCondition.status == STATUS_NEW]
    if scope:
        filters.append(AlertCondition.environment == scope)

    rows = (
        (
            await session.execute(
                select(AlertCondition).where(*filters).order_by(AlertCondition.id)
            )
        )
        .scalars()
        .all()
    )

    # Candidates: every new condition EXCEPT collect on first sight (never
    # reopened). Collect after reopen IS included — a recurrence of something
    # parked may need re-evaluation. All dispositions are included (not just
    # immediate) per the updated attention spec (#12 / OPEN 2).
    candidates = [
        condition
        for condition in rows
        if not (
            effective_disposition(condition.severity, condition.disposition) == "collect"
            and (condition.reopened_count or 0) == 0
        )
    ]

    # One grouped query over the whole candidate set — the LIVE occurrence
    # count per condition, unfiltered by id.
    live: dict[int, int] = {}
    legacy_new: dict[int, int] = {}
    users_24h: dict[int, int] = {}
    if candidates:
        candidate_ids = [c.id for c in candidates]
        grouped = await session.execute(
            select(Alert.condition_id, func.count(Alert.id))
            .where(Alert.condition_id.in_(candidate_ids))
            .group_by(Alert.condition_id)
        )
        live = {row[0]: row[1] or 0 for row in grouped}
        if legacy_since is not None:
            grouped_since = await session.execute(
                select(Alert.condition_id, func.count(Alert.id))
                .where(
                    Alert.condition_id.in_(candidate_ids),
                    Alert.id > legacy_since,
                )
                .group_by(Alert.condition_id)
            )
            legacy_new = {row[0]: row[1] or 0 for row in grouped_since}

        # Count distinct users in the last 24h from _user_id in context_json.
        users_24h = await _count_users_24h(session, candidate_ids)

    next_marks: dict[int, tuple[int, int]] = {}
    qualifying: list[tuple[AlertCondition, int]] = []
    for condition in candidates:
        count = live.get(condition.id, 0)
        reopened = condition.reopened_count or 0
        if legacy_since is not None:
            since_count = legacy_new.get(condition.id, 0)
            if since_count > 0:
                qualifying.append((condition, since_count))
            else:
                next_marks[condition.id] = (count, reopened)
            continue

        mark = marks.get(condition.id)
        if mark is None:
            qualifying.append((condition, count))
        else:
            mark_count, mark_reopened = mark
            # A reopen re-reports regardless of occurrence count.
            if reopened > mark_reopened:
                qualifying.append((condition, count - min(mark_count, count)))
            elif count > mark_count:
                qualifying.append((condition, count - mark_count))
            else:
                next_marks[condition.id] = (min(mark_count, count), reopened)

    # Sort by users_24h desc, severity rank desc, age asc (first_seen asc).
    _severity_rank = {
        "critical": 3,
        "error": 2,
        "warn": 1,
    }
    qualifying.sort(
        key=lambda pair: (
            -(users_24h.get(pair[0].id, 0)),
            -_severity_rank.get(pair[0].severity, 0),
            pair[0].first_seen or pair[0].created,
        )
    )

    included = qualifying[:limit]
    included_ids = {condition.id for condition, _ in included}
    for condition, _ in qualifying:
        reopened = condition.reopened_count or 0
        if condition.id in included_ids:
            next_marks[condition.id] = (live.get(condition.id, 0), reopened)
        elif condition.id in marks:
            next_marks[condition.id] = marks[condition.id]

    conditions = [
        {
            "id": condition.id,
            "severity": condition.severity,
            "category": condition.category,
            "template": condition.message_template,
            "occurrence_count": condition.occurrence_count,
            "count_since_cursor": count_since,
            "last_seen": condition.last_seen,
            "reopened": (condition.reopened_count or 0) > 0,
            "users_24h": users_24h.get(condition.id, 0),
            "first_seen_release": condition.first_seen_release,
            "last_seen_release": condition.last_seen_release,
            "is_regression": (
                condition.last_seen_release is not None
                and condition.acknowledged_release is not None
                and condition.last_seen_release != condition.acknowledged_release
            ),
        }
        for condition, count_since in included
    ]

    return {"conditions": conditions, "cursor": _encode_attention_cursor(next_marks)}


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


async def delete_batch(
    session: "AsyncSession",
    *,
    category: str,
    message: str,
    before_id: int,
) -> int:
    """Delete one batch of identical alerts: exact category + message, id <= before_id.

    The narrow cousin of :func:`delete_by_category` for callers holding a
    scoped clear-batch permission rather than full delete rights: ``message``
    is required (no category-wide sweeps) and matched exactly (no patterns),
    and the ``before_id`` bound restricts the delete to occurrences the caller
    actually inspected — rows that arrive after the caller looked survive, so
    a live recurrence is never silently swallowed by its own cleanup.

    Returns the number of rows deleted.
    """
    result = await session.execute(
        delete(Alert).where(
            Alert.category == category,
            Alert.message == message,
            Alert.id <= before_id,
        )
    )
    return result.rowcount


async def delete_by_id(session: "AsyncSession", *, alert_id: int) -> bool:
    """Delete a single alert by ID. Returns True if found and deleted."""
    result = await session.execute(
        delete(Alert).where(Alert.id == alert_id)
    )
    return result.rowcount > 0
