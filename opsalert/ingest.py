"""Ingest — bounded in-process queue with a single writer thread.

Product rule: ``opsalert.warn/error/critical(...)`` never touches an event loop,
a session factory, the DB, or the caller's transaction. It enriches, builds an
event, appends it to a bounded in-process queue, and returns. One daemon thread
per process writes. Nothing is lost silently: every event is either a row, a
counted sample, or a counted drop, and every event is a JSON log line.

No event-loop imports in this module. The writer thread uses sync SQLAlchemy only.
"""
from __future__ import annotations

import atexit
import calendar
import collections
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger("opsalert.internal")
occurrence_logger = logging.getLogger("opsalert.occurrence")

# ---------------------------------------------------------------------------
# Event dataclass
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Event:
    """One alert occurrence, ready for the queue."""

    event_id: str  # 32-hex uuid4().hex
    ts: datetime  # aware UTC, set at fire time
    severity: str
    category: str
    message: str
    source: str | None
    context: dict[str, Any] | None  # already enriched + stamp_environment applied
    params: dict[str, Any] | None
    # Identity header — computed on the caller thread
    template: str
    environment: str | None
    signature_key: str
    # Sampling decision — stamped when the event is popped from the queue,
    # so replay never re-decides.
    sampled_in: bool = True


# ---------------------------------------------------------------------------
# FlushResult / DropRecord
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FlushResult:
    """Returned by ``flush()``."""

    written: int
    sampled_out: int
    dropped: int
    remaining: int


@dataclass
class DropRecord:
    """Tracks evicted or failed events per fingerprint."""

    count: int
    # Identity header fields needed for condition lookup-or-create
    category: str
    source: str | None
    environment: str | None
    template: str
    severity: str


# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_queue: collections.deque[Event] = collections.deque()
_lock = threading.Lock()
_condition = threading.Condition(_lock)
_per_fp: dict[str, int] = {}  # queued count per signature_key
_dropped: dict[str, DropRecord] = {}

# Sampling state: (signature_key, minute) -> count of events in that minute
_sample_state: dict[tuple[str, int], int] = {}

_thread: threading.Thread | None = None
_engine: Any = None  # sqlalchemy.Engine, created lazily
_error_logged = False  # "no database URL" logged once per process
_generation = 0  # incremented on reset to signal old threads to exit

# Cumulative counters for flush result
_written_total = 0
_sampled_out_total = 0
_dropped_total = 0


# ---------------------------------------------------------------------------
# Fork safety
# ---------------------------------------------------------------------------


def _reset() -> None:
    """Reset module state in a forked child. Called via os.register_at_fork."""
    global _thread, _engine, _error_logged, _generation
    global _written_total, _sampled_out_total, _dropped_total
    _generation += 1
    _queue.clear()
    _per_fp.clear()
    _dropped.clear()
    _sample_state.clear()
    _thread = None
    _engine = None
    _error_logged = False
    _written_total = 0
    _sampled_out_total = 0
    _dropped_total = 0


try:
    os.register_at_fork(after_in_child=_reset)
except AttributeError:
    pass  # platforms without fork


# ---------------------------------------------------------------------------
# JSON log line
# ---------------------------------------------------------------------------


def _log_occurrence(event: Event) -> None:
    """Emit one JSON log line on logger ``opsalert.occurrence``, level INFO."""
    ctx = event.context or {}
    data: dict[str, Any] = {
        "event_id": event.event_id,
        "ts": event.ts.isoformat(),
        "severity": event.severity,
        "category": event.category,
        "source": event.source,
        "environment": event.environment,
        "signature_key": event.signature_key,
        "template": event.template,
        "message": event.message,
    }
    # Optional context fields — omit Nones
    for key, ctx_key in [
        ("trace_id", "_trace_id"),
        ("user_id", "_user_id"),
        ("org_id", "_org_id"),
        ("exc_type", "_exc_type"),
        ("caller", "_caller"),
    ]:
        val = ctx.get(ctx_key)
        if val is not None:
            data[key] = val
    occurrence_logger.info(json.dumps(data, default=str))


# ---------------------------------------------------------------------------
# Enqueue
# ---------------------------------------------------------------------------


def enqueue(event: Event) -> None:
    """Append an event to the queue. Never raises to the caller."""
    try:
        _log_occurrence(event)

        from opsalert._config import get_config

        cfg = get_config()
        max_size = cfg.ingest_queue_max

        with _condition:
            if len(_queue) >= max_size:
                _evict_one()
            _queue.append(event)
            _per_fp[event.signature_key] = _per_fp.get(event.signature_key, 0) + 1
            _condition.notify()

        _start_thread()
    except BaseException:
        logger.exception("opsalert.ingest: enqueue failed")


def _evict_one() -> None:
    """Evict the victim from the fingerprint with the highest queued count.

    Must be called under the lock. Ties go to the oldest event of that
    fingerprint. O(n) scan is fine at 2,000.
    """
    if not _per_fp:
        return

    # Find the fingerprint with the highest queued count
    victim_fp = max(_per_fp, key=lambda fp: _per_fp[fp])

    # Find the oldest event with that fingerprint and remove it
    for i, ev in enumerate(_queue):
        if ev.signature_key == victim_fp:
            del _queue[i]
            _per_fp[victim_fp] -= 1
            if _per_fp[victim_fp] <= 0:
                del _per_fp[victim_fp]
            # Record the drop for the VICTIM's header
            _record_drop(ev)
            return


def _record_drop(event: Event) -> None:
    """Record a drop for the evicted event's fingerprint.

    Caller must hold the lock when modifying _dropped.
    """
    fp = event.signature_key
    if fp in _dropped:
        _dropped[fp].count += 1
    else:
        _dropped[fp] = DropRecord(
            count=1,
            category=event.category,
            source=event.source,
            environment=event.environment,
            template=event.template,
            severity=event.severity,
        )


# ---------------------------------------------------------------------------
# Writer thread
# ---------------------------------------------------------------------------


def _start_thread() -> None:
    """Start the writer thread lazily on first enqueue."""
    global _thread
    if _thread is not None and _thread.is_alive():
        return
    _thread = threading.Thread(target=_writer_loop, daemon=True, name="opsalert-ingest")
    _thread.start()


def _get_engine():
    """Create or return the sync SQLAlchemy engine. Lazily in the thread."""
    global _engine
    if _engine is not None:
        return _engine

    url = _resolve_url()
    if url is None:
        return None

    from sqlalchemy import create_engine

    _engine = create_engine(url, pool_size=1, max_overflow=0, pool_pre_ping=True)
    return _engine


def _resolve_url() -> str | None:
    """Resolve the sync database URL from config."""
    global _error_logged
    try:
        from opsalert._config import get_config

        cfg = get_config()
    except RuntimeError:
        return None

    if cfg.ingest_url:
        return cfg.ingest_url

    # Try to derive from session_factory
    from opsalert._config import derive_sync_url

    url = derive_sync_url()
    if url:
        return url

    if not _error_logged:
        _error_logged = True
        logger.error("opsalert.ingest: ingest has no database URL")
    return None


def _check_flush_done() -> None:
    """If a flush is pending and the queue is empty, signal completion.

    Only waits for the queue to drain. Pending drop records (_dropped) are
    eventual-consistency bookkeeping that will land when the DB is back;
    the flush result reports the drop count so the caller knows.

    Clears _flush_event so the writer thread does not busy-spin.
    """
    if not _flush_event.is_set():
        return
    with _condition:
        q_empty = len(_queue) == 0
    if q_empty:
        _flush_event.clear()
        _flush_done_event.set()


def _writer_loop() -> None:
    """Main loop of the writer thread. One iteration per batch."""
    global _written_total, _sampled_out_total, _dropped_total

    try:
        from opsalert._config import get_config

        cfg = get_config()
    except RuntimeError:
        return

    my_gen = _generation
    batch_size = cfg.ingest_batch_size
    sample_limit = cfg.ingest_sample_per_minute
    flush_interval = cfg.ingest_flush_interval_s
    max_retry_s = cfg.ingest_max_retry_s

    # Clear stale sample state from any prior generation.
    _sample_state.clear()

    while _generation == my_gen:
        batch: list[Event] = []
        drops_snapshot: dict[str, DropRecord] = {}
        try:
            # Check for pending flush before waiting
            _check_flush_done()

            # Wait for events or flush signal
            with _condition:
                if not _queue and not _flush_event.is_set():
                    _condition.wait(timeout=flush_interval)

            # Re-check flush after waking
            _check_flush_done()

            # Pop events and snapshot drops under the lock; stamp sampling
            # decisions outside the lock so enqueue() is never blocked by
            # the sampling bookkeeping.
            with _condition:
                n = min(batch_size, len(_queue))
                for _ in range(n):
                    ev = _queue.popleft()
                    batch.append(ev)
                    fp = ev.signature_key
                    count = _per_fp.get(fp, 1) - 1
                    if count <= 0:
                        _per_fp.pop(fp, None)
                    else:
                        _per_fp[fp] = count

                # Snapshot drops
                for fp, dr in list(_dropped.items()):
                    drops_snapshot[fp] = DropRecord(
                        count=dr.count,
                        category=dr.category,
                        source=dr.source,
                        environment=dr.environment,
                        template=dr.template,
                        severity=dr.severity,
                    )

            # Stamp sampling decisions outside the lock (no contention
            # with enqueue). _sample_state is only accessed by the writer
            # thread so no lock is needed.
            for ev in batch:
                fp = ev.signature_key
                minute_key = (fp, _ts_minute(ev.ts))
                current_count = _sample_state.get(minute_key, 0)
                _sample_state[minute_key] = current_count + 1
                ev.sampled_in = current_count < sample_limit

            # Prune sample state to last 2 minutes
            _prune_sample_state(datetime.now(UTC))

            if not batch and not drops_snapshot:
                _check_flush_done()
                continue

            engine = _get_engine()
            if engine is None:
                # No DB — count everything as dropped
                with _condition:
                    for ev in batch:
                        _record_drop(ev)
                    _dropped_total += len(batch)
                _check_flush_done()
                continue

            # Attempt to write the batch with retries
            cumulative_retry = 0.0
            backoff = 1.0

            while True:
                try:
                    with engine.connect() as conn:
                        result = write_batch(
                            conn, batch, drops_snapshot, datetime.now(UTC)
                        )
                        conn.commit()

                    # Success — update totals
                    _written_total += result.written
                    _sampled_out_total += result.sampled_out

                    # Subtract flushed drop counts (under lock)
                    with _condition:
                        for fp, dr in drops_snapshot.items():
                            if fp in _dropped:
                                _dropped[fp].count -= dr.count
                                if _dropped[fp].count <= 0:
                                    del _dropped[fp]

                    break  # Success — exit retry loop

                except Exception as exc:
                    from sqlalchemy.exc import (
                        DataError,
                        DBAPIError,
                        IntegrityError,
                        ProgrammingError,
                    )

                    # Retry transient DB errors (connection lost, lock timeout,
                    # etc.) but not permanent ones (bad SQL, constraint
                    # violation, data type mismatch).
                    is_retryable = isinstance(exc, DBAPIError) and not isinstance(
                        exc, (IntegrityError, ProgrammingError, DataError)
                    )

                    if is_retryable and cumulative_retry < max_retry_s and _generation == my_gen:
                        time.sleep(min(backoff, 30.0))
                        cumulative_retry += backoff
                        backoff = min(backoff * 2, 30.0)
                        continue

                    # Max retries exceeded or non-operational error — drop
                    logger.error(
                        "opsalert.ingest: batch write failed after %.1fs; "
                        "dropping %d events",
                        cumulative_retry,
                        len(batch),
                        exc_info=True,
                    )
                    with _condition:
                        for ev in batch:
                            _record_drop(ev)
                        _dropped_total += len(batch)
                    break  # Exit retry loop

            _check_flush_done()

        except BaseException:
            logger.exception("opsalert.ingest: writer thread dying")
            # Re-queue events at the front (respecting max)
            if batch:
                with _condition:
                    for ev in reversed(batch):
                        if len(_queue) >= cfg.ingest_queue_max:
                            _evict_one()
                        _queue.appendleft(ev)
                        _per_fp[ev.signature_key] = (
                            _per_fp.get(ev.signature_key, 0) + 1
                        )
            # Thread exits; next enqueue starts a new one
            _check_flush_done()
            break


# ---------------------------------------------------------------------------
# Batch write
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class BatchResult:
    """Result of a single write_batch call."""

    written: int
    sampled_out: int


def write_batch(
    conn: Any,
    events: list[Event],
    drops: dict[str, DropRecord],
    now: datetime,
) -> BatchResult:
    """Write a batch of events to the database in one transaction.

    Groups events by signature_key, resolves conditions, and inserts rows.
    Sampling decisions are pre-stamped on each ``Event.sampled_in`` at pop
    time so this function never mutates ``_sample_state`` -- replay is safe.
    When every insert in a group hits a duplicate (ambiguous commit replay),
    the group's counter UPDATEs are skipped entirely.
    """
    from sqlalchemy import case, update

    from opsalert.model import Alert, AlertCondition

    written = 0
    sampled_out_count = 0

    # Group events by signature_key
    groups: dict[str, list[Event]] = {}
    for ev in events:
        groups.setdefault(ev.signature_key, []).append(ev)

    for fp, group in groups.items():
        # Resolve condition id (lookup-or-create, sync).
        # Best-effort: a resolution failure stores the alert as an orphan
        # (condition_id=NULL), never loses it.
        try:
            condition_id = _resolve_condition_sync(conn, group[0])
        except Exception:
            logger.exception(
                "opsalert.ingest: condition resolution failed for category=%s",
                group[0].category,
            )
            condition_id = None

        last_inserted_id = None
        group_sampled = 0
        group_attempted = 0  # inserts attempted (sampled_in events)
        group_inserted = 0   # inserts that succeeded (not duplicate)
        max_ts = group[0].ts

        for ev in group:
            if ev.ts > max_ts:
                max_ts = ev.ts

            if ev.sampled_in:
                group_attempted += 1
                row_id = _insert_alert(conn, ev, condition_id)
                if row_id is not None:
                    last_inserted_id = row_id
                    written += 1
                    group_inserted += 1
                # row_id is None => duplicate (replay); don't count as new
            else:
                group_sampled += 1

        # Skip UPDATEs only when at least one insert was ATTEMPTED and
        # every attempted insert hit a duplicate — those UPDATEs were
        # already applied in the ambiguous prior commit. When NO insert
        # was attempted (all sampled_out), the condition UPDATEs must
        # still fire.
        if group_attempted > 0 and group_inserted == 0:
            sampled_out_count += group_sampled
            continue

        if group_sampled > 0:
            sampled_out_count += group_sampled
            if last_inserted_id is not None:
                conn.execute(
                    update(Alert)
                    .where(Alert.id == last_inserted_id)
                    .values(sampled_out=Alert.sampled_out + group_sampled)
                )

            if condition_id is not None:
                # last_seen = max(last_seen, max_ts) via CASE
                conn.execute(
                    update(AlertCondition)
                    .where(AlertCondition.id == condition_id)
                    .values(
                        sampled_out=AlertCondition.sampled_out + group_sampled,
                        last_seen=case(
                            (AlertCondition.last_seen.is_(None), max_ts),
                            (AlertCondition.last_seen < max_ts, max_ts),
                            else_=AlertCondition.last_seen,
                        ),
                    )
                )

    # Handle drops
    for fp, dr in drops.items():
        if dr.count <= 0:
            continue
        try:
            condition_id = _resolve_condition_sync_from_drop(conn, dr)
        except Exception:
            logger.exception(
                "opsalert.ingest: drop condition resolution failed for category=%s",
                dr.category,
            )
            condition_id = None
        if condition_id is not None:
            conn.execute(
                update(AlertCondition)
                .where(AlertCondition.id == condition_id)
                .values(dropped_count=AlertCondition.dropped_count + dr.count)
            )

    return BatchResult(written=written, sampled_out=sampled_out_count)


def _ts_minute(ts: datetime) -> int:
    """Return a minute key from a datetime."""
    return int(calendar.timegm(ts.utctimetuple())) // 60


def _prune_sample_state(now: datetime) -> None:
    """Remove sample state entries older than 2 minutes."""
    current_minute = _ts_minute(now)
    cutoff = current_minute - 2
    stale = [k for k in _sample_state if k[1] < cutoff]
    for k in stale:
        del _sample_state[k]


def _insert_alert(conn: Any, event: Event, condition_id: int | None) -> int | None:
    """Insert a single Alert row. Returns the row id or None."""
    from sqlalchemy.exc import IntegrityError

    from opsalert.model import Alert
    from opsalert.store import serialize_context

    try:
        result = conn.execute(
            Alert.__table__.insert().values(
                event_id=event.event_id,
                severity=event.severity,
                category=event.category,
                source=event.source,
                message=event.message,
                context_json=serialize_context(event.context),
                condition_id=condition_id,
                sampled_out=0,
                notified=False,
                created=event.ts,
            )
        )
        return result.inserted_primary_key[0]
    except IntegrityError:
        # Duplicate event_id — replay safety
        return None


def _resolve_condition_sync(conn: Any, event: Event) -> int | None:
    """Lookup-or-create a condition row synchronously."""
    from sqlalchemy import select

    from opsalert.model import AlertCondition
    from opsalert.store import upsert_statement

    fp = event.signature_key
    now = datetime.now(UTC)

    # SELECT first
    existing = conn.execute(
        select(AlertCondition.id).where(AlertCondition.signature_key == fp)
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    values = {
        "signature_key": fp,
        "category": event.category,
        "source": event.source,
        "environment": event.environment,
        "message_template": (event.template or "")[:500],
        "status": "new",
        "severity": event.severity,
        "latest_severity": event.severity,
        "status_changed_at": now,
        "first_seen": now,
        "created": now,
        "updated": now,
    }

    # Detect dialect
    dialect = conn.dialect.name

    try:
        stmt = upsert_statement(dialect, values)
        result = conn.execute(stmt)
        if dialect == "mysql":
            return result.lastrowid
        if dialect == "sqlite":
            return result.scalar_one()
        return result.inserted_primary_key[0]
    except Exception:
        # Fallback: select again
        return conn.execute(
            select(AlertCondition.id).where(AlertCondition.signature_key == fp)
        ).scalar_one_or_none()


def _resolve_condition_sync_from_drop(conn: Any, dr: DropRecord) -> int | None:
    """Resolve condition for a drop record."""
    from sqlalchemy import select

    from opsalert.model import AlertCondition
    from opsalert.signature import condition_signature
    from opsalert.store import upsert_statement

    fp = condition_signature(dr.category, dr.source, dr.environment, dr.template)

    # SELECT first
    existing = conn.execute(
        select(AlertCondition.id).where(AlertCondition.signature_key == fp)
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    now = datetime.now(UTC)
    values = {
        "signature_key": fp,
        "category": dr.category,
        "source": dr.source,
        "environment": dr.environment,
        "message_template": (dr.template or "")[:500],
        "status": "new",
        "severity": dr.severity,
        "latest_severity": dr.severity,
        "status_changed_at": now,
        "first_seen": now,
        "created": now,
        "updated": now,
    }

    dialect = conn.dialect.name
    try:
        stmt = upsert_statement(dialect, values)
        result = conn.execute(stmt)
        if dialect == "mysql":
            return result.lastrowid
        if dialect == "sqlite":
            return result.scalar_one()
        return result.inserted_primary_key[0]
    except Exception:
        return conn.execute(
            select(AlertCondition.id).where(AlertCondition.signature_key == fp)
        ).scalar_one_or_none()


# ---------------------------------------------------------------------------
# Flush
# ---------------------------------------------------------------------------

# Use a dedicated threading.Event pair for flush coordination.
# _flush_event: set by flush() to signal the writer.
# _flush_done_event: set by the writer when the queue is drained.
_flush_event = threading.Event()
_flush_done_event = threading.Event()


def flush(timeout: float = 5.0) -> FlushResult:
    """Sync flush -- wait for the queue to drain or timeout.

    Safe to call from any thread, from inside a running loop, and before the
    thread ever started. Never raises.
    """
    global _dropped_total

    try:
        # If there are queued events and no thread, start one so they drain.
        with _condition:
            has_work = bool(_queue)
        if has_work and (_thread is None or not _thread.is_alive()):
            _start_thread()

        if _thread is None or not _thread.is_alive():
            # Truly nothing to do (no thread, no queue).
            with _condition:
                total_dropped = _dropped_total
            return FlushResult(
                written=_written_total,
                sampled_out=_sampled_out_total,
                dropped=total_dropped,
                remaining=0,
            )

        # Signal the writer thread to flush
        _flush_done_event.clear()
        _flush_event.set()
        with _condition:
            _condition.notify_all()

        # Wait for the writer to signal completion
        _flush_done_event.wait(timeout=timeout)

        with _condition:
            remaining = len(_queue)
            total_dropped = _dropped_total

        return FlushResult(
            written=_written_total,
            sampled_out=_sampled_out_total,
            dropped=total_dropped,
            remaining=remaining,
        )
    except BaseException:
        with _condition:
            remaining = len(_queue)
            total_dropped = _dropped_total
        return FlushResult(
            written=_written_total,
            sampled_out=_sampled_out_total,
            dropped=total_dropped,
            remaining=remaining,
        )


# Register atexit flush
atexit.register(flush)
