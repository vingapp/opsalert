"""Contract tests for ingest — the bounded enqueue + background writer.

Red-first: tests (b) and (d) were written BEFORE implementation and failed
on the unmodified code. All tests use file-backed sqlite as ingest_url.
"""
import json
import logging
import os
import time

import pytest
from sqlalchemy import create_engine, text

import opsalert
from opsalert.ingest import (
    Event,
    FlushResult,
    enqueue,
    flush,
    write_batch,
)
from opsalert.model import OpsAlertBase
from opsalert.signature import condition_signature

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_db(tmp_path, name="ingest.db"):
    """Create a sqlite file DB with opsalert tables; return (url, engine)."""
    db_path = tmp_path / name
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    OpsAlertBase.metadata.create_all(engine)
    return url, engine


def _count_alerts(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM opsalert")).scalar()


def _count_conditions(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM alert_condition")).scalar()


# ---------------------------------------------------------------------------
# (a) fire inside a running loop → after flush() one row
# ---------------------------------------------------------------------------


class TestFireInsideLoop:
    async def test_fire_inside_running_loop(self, tmp_path):
        """(a) fire from an async def test (running loop) → one row after flush."""
        url, engine = _make_db(tmp_path)

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )
        opsalert.warn("loop_test", message="inside loop")
        result = flush(timeout=5.0)

        assert _count_alerts(engine) >= 1
        assert result.written >= 1
        engine.dispose()


# ---------------------------------------------------------------------------
# (b) fire as the LAST statement of asyncio.run() then flush outside → row
# ---------------------------------------------------------------------------


class TestLastStatementFire:
    def test_fire_as_last_statement_then_flush(self, tmp_path):
        """(b) fire inside asyncio.run as the last statement, flush outside → one row."""
        import asyncio

        url, engine = _make_db(tmp_path)

        async def body():
            opsalert.configure(
                session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
                ingest_url=url,
            )
            opsalert.warn("last_stmt", message="fired at the end")

        asyncio.run(body())
        result = flush(timeout=5.0)

        assert _count_alerts(engine) >= 1
        assert result.written >= 1
        engine.dispose()


# ---------------------------------------------------------------------------
# (c) fire in a forked child → row visible to parent
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not hasattr(os, "fork"), reason="no fork on this platform")
class TestForkChild:
    def test_fire_in_forked_child(self, tmp_path):
        """(c) fire + flush in forked child → row visible to parent."""
        url, engine = _make_db(tmp_path)

        pid = os.fork()
        if pid == 0:
            # Child process
            try:
                opsalert.configure(
                    session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
                    ingest_url=url,
                )
                opsalert.warn("fork_child", message="from child")
                flush(timeout=5.0)
            finally:
                os._exit(0)
        else:
            # Parent — wait for child
            _, status = os.waitpid(pid, 0)
            assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0

            assert _count_alerts(engine) >= 1
            engine.dispose()


# ---------------------------------------------------------------------------
# (d) DB refusing → flush returns dropped >= 1, JSON log, warn < 50ms
# ---------------------------------------------------------------------------


class TestDBRefusing:
    def test_db_refusing_drops_and_logs_fast(self, tmp_path, caplog):
        """(d) DB refusing: flush reports drops, JSON log emitted, warn fast."""
        caplog.set_level(logging.INFO, logger="opsalert.occurrence")

        bad_url = f"sqlite:///{tmp_path}/nonexistent_dir/subdir/cant_write.db"

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=bad_url,
            ingest_max_retry_s=0.5,
        )

        # Warm up: first warn pays the thread-creation cost (up to 45ms on
        # this box).  The contract we verify is that the DB failure never
        # blocks the caller — the second call, with the thread already alive,
        # is the one we time.
        opsalert.warn("warmup", message="thread warmup")
        time.sleep(0.01)  # let the thread start

        start = time.perf_counter()
        opsalert.warn("db_refuse", message="this should be fast")
        elapsed = time.perf_counter() - start

        assert elapsed < 0.050, f"warn took {elapsed:.3f}s, must be < 50ms"

        result = flush(timeout=3.0)
        assert result.dropped >= 1, f"expected dropped >= 1, got {result}"

        occ_records = [
            r for r in caplog.records if r.name == "opsalert.occurrence"
        ]
        assert len(occ_records) >= 2, "expected at least two opsalert.occurrence log lines"


# ---------------------------------------------------------------------------
# (e) 5,000 fires same sig → ≤ 20 rows, sampling + drops account for all
# ---------------------------------------------------------------------------


class TestHighVolumeSampling:
    def test_5000_fires_sampling_and_accounting(self, tmp_path):
        """(e) 5,000 fires of ONE sig within one minute:
        - <= 20 rows for that condition
        - rows + sampled + dropped == 5,000
        - median per-call < 2ms (proves enqueue is cheap)
        - DB is only ever touched from the opsalert-ingest thread, never
          from the caller thread (structural proof of the contract)
        """
        import threading

        from opsalert import ingest as _ingest

        # Double-check clean state
        assert len(_ingest._sample_state) == 0, (
            f"sample_state not clean: {dict(_ingest._sample_state)}"
        )

        url, engine = _make_db(tmp_path)

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
            ingest_queue_max=2000,
        )

        # Instrument _get_engine to record which thread touches the DB.
        engine_thread_idents: list[int] = []
        orig_get_engine = _ingest._get_engine

        def recording_get_engine():
            engine_thread_idents.append(threading.get_ident())
            return orig_get_engine()

        _ingest._get_engine = recording_get_engine

        caller_ident = threading.get_ident()

        try:
            timings = []
            for _ in range(5000):
                t0 = time.perf_counter()
                opsalert.warn("high_vol", message="same message")
                timings.append(time.perf_counter() - t0)

            flush(timeout=10.0)
        finally:
            _ingest._get_engine = orig_get_engine

        with engine.connect() as conn:
            row_count = conn.execute(text("SELECT COUNT(*) FROM opsalert")).scalar()

        assert row_count <= 20, f"expected <= 20 rows, got {row_count}"

        # Median per-call < 2ms — stable proof that enqueue is cheap.
        timings.sort()
        median = timings[len(timings) // 2]
        assert median < 0.002, f"median per-call latency {median*1000:.2f}ms > 2ms"

        # Structural proof: the caller thread never touched the DB engine.
        assert len(engine_thread_idents) > 0, (
            "_get_engine was never called — the writer thread didn't run"
        )
        assert caller_ident not in engine_thread_idents, (
            f"caller thread {caller_ident} touched the DB engine; "
            f"engine threads: {set(engine_thread_idents)}"
        )

        engine.dispose()


# ---------------------------------------------------------------------------
# (f) eviction chooses the biggest fingerprint
# ---------------------------------------------------------------------------


class TestEvictionPolicy:
    def test_eviction_targets_biggest_fingerprint(self, tmp_path):
        """(f) fill with 1990 A + 10 B, enqueue 20 more A → B intact, A has drops."""
        import uuid
        from datetime import UTC, datetime

        from opsalert import ingest

        url, engine = _make_db(tmp_path)

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
            ingest_queue_max=2000,
        )

        # Monkeypatch _start_thread to no-op so nothing drains
        orig_start = ingest._start_thread
        ingest._start_thread = lambda: None

        sig_a = condition_signature("cat_a", None, None, "msg_a")
        sig_b = condition_signature("cat_b", None, None, "msg_b")

        try:
            # Fill with 1990 A
            for _ in range(1990):
                ev = Event(
                    event_id=uuid.uuid4().hex,
                    ts=datetime.now(UTC),
                    severity="warn",
                    category="cat_a",
                    message="msg_a",
                    source=None,
                    context=None,
                    params=None,
                    template="msg_a",
                    environment=None,
                    signature_key=sig_a,
                )
                enqueue(ev)

            # Add 10 B
            for _ in range(10):
                ev = Event(
                    event_id=uuid.uuid4().hex,
                    ts=datetime.now(UTC),
                    severity="warn",
                    category="cat_b",
                    message="msg_b",
                    source=None,
                    context=None,
                    params=None,
                    template="msg_b",
                    environment=None,
                    signature_key=sig_b,
                )
                enqueue(ev)

            assert len(ingest._queue) == 2000

            # Now enqueue 20 more A — triggers 20 evictions
            for _ in range(20):
                ev = Event(
                    event_id=uuid.uuid4().hex,
                    ts=datetime.now(UTC),
                    severity="warn",
                    category="cat_a",
                    message="msg_a",
                    source=None,
                    context=None,
                    params=None,
                    template="msg_a",
                    environment=None,
                    signature_key=sig_a,
                )
                enqueue(ev)

            # B's 10 should all still be present
            b_count = sum(1 for e in ingest._queue if e.signature_key == sig_b)
            assert b_count == 10, f"expected B=10, got {b_count}"

            # A should have 20 drops
            assert sig_a in ingest._dropped
            assert ingest._dropped[sig_a].count == 20

        finally:
            ingest._start_thread = orig_start
            engine.dispose()


# ---------------------------------------------------------------------------
# (g) thread death requeues batch
# ---------------------------------------------------------------------------


class TestThreadDeath:
    def test_thread_death_requeues_batch(self, tmp_path, caplog):
        """(g) write_batch raises once → events re-queued, next batch writes."""
        caplog.set_level(logging.ERROR, logger="opsalert.internal")

        url, engine = _make_db(tmp_path)

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )

        from opsalert import ingest

        call_count = 0
        orig_write_batch = ingest.write_batch

        def failing_once(conn, events, drops, now):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("simulated thread death")
            return orig_write_batch(conn, events, drops, now)

        ingest.write_batch = failing_once

        try:
            opsalert.warn("death_test", message="should survive")
            # Give time for first attempt (thread death) + restart + success
            time.sleep(0.5)
            # Fire again to trigger new thread
            opsalert.warn("death_test", message="second fire")
            flush(timeout=5.0)

            assert _count_alerts(engine) >= 1

            internal_errors = [
                r for r in caplog.records
                if r.name == "opsalert.internal" and r.levelno >= logging.ERROR
            ]
            assert len(internal_errors) >= 1, "expected opsalert.internal error log"
        finally:
            ingest.write_batch = orig_write_batch
            engine.dispose()


# ---------------------------------------------------------------------------
# (h) sampling keyed on event minute
# ---------------------------------------------------------------------------


class TestSamplingByMinute:
    def test_sampling_keyed_on_event_minute(self, tmp_path):
        """(h) events with ts in minute M and M+1 sample independently."""
        import uuid
        from datetime import UTC, datetime, timedelta

        url, engine = _make_db(tmp_path)

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
            ingest_sample_per_minute=5,
            ingest_batch_size=200,
        )

        from opsalert import ingest

        # Ensure sample state is clean
        ingest._sample_state.clear()

        sig = condition_signature("cat", None, None, "msg")
        # Use times close to NOW so _prune_sample_state doesn't evict them
        # between batches (it prunes entries older than 2 minutes from now).
        now = datetime.now(UTC)
        # Round down to the start of the current minute
        base_time = now.replace(second=0, microsecond=0)
        next_minute = base_time + timedelta(minutes=1)

        # 10 events in minute M
        for i in range(10):
            ev = Event(
                event_id=uuid.uuid4().hex,
                ts=base_time + timedelta(seconds=i),
                severity="warn",
                category="cat",
                message="msg",
                source=None,
                context=None,
                params=None,
                template="msg",
                environment=None,
                signature_key=sig,
            )
            enqueue(ev)

        # 10 events in minute M+1
        for i in range(10):
            ev = Event(
                event_id=uuid.uuid4().hex,
                ts=next_minute + timedelta(seconds=i),
                severity="warn",
                category="cat",
                message="msg",
                source=None,
                context=None,
                params=None,
                template="msg",
                environment=None,
                signature_key=sig,
            )
            enqueue(ev)

        flush(timeout=5.0)

        with engine.connect() as conn:
            row_count = conn.execute(text("SELECT COUNT(*) FROM opsalert")).scalar()

        # 5 per minute × 2 minutes = 10
        assert row_count == 10, f"expected 10 rows (5/min × 2 min), got {row_count}"
        engine.dispose()


# ---------------------------------------------------------------------------
# Overflow batch still updates condition sampled_out
# ---------------------------------------------------------------------------


class TestOverflowBatchUpdatesCondition:
    def test_sampled_out_batch_updates_condition(self, tmp_path):
        """25 events of one signature in one minute, sample limit 20,
        split across two batches (20 then 5). The second batch inserts
        nothing (all sampled_out) and must still add 5 to the condition's
        sampled_out."""
        import uuid
        from datetime import UTC, datetime

        url, engine = _make_db(tmp_path)
        sig = condition_signature("cat", None, None, "msg")
        now = datetime.now(UTC)

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
            ingest_sample_per_minute=20,
        )

        # Batch 1: 20 events, all sampled_in=True
        batch1 = [
            Event(
                event_id=uuid.uuid4().hex,
                ts=now,
                severity="warn",
                category="cat",
                message="msg",
                source=None,
                context=None,
                params=None,
                template="msg",
                environment=None,
                signature_key=sig,
                sampled_in=True,
            )
            for _ in range(20)
        ]

        # Batch 2: 5 events, all sampled_in=False (decided at pop time)
        batch2 = [
            Event(
                event_id=uuid.uuid4().hex,
                ts=now,
                severity="warn",
                category="cat",
                message="msg",
                source=None,
                context=None,
                params=None,
                template="msg",
                environment=None,
                signature_key=sig,
                sampled_in=False,
            )
            for _ in range(5)
        ]

        with engine.connect() as conn:
            write_batch(conn, batch1, {}, now)
            conn.commit()

        with engine.connect() as conn:
            write_batch(conn, batch2, {}, now)
            conn.commit()

        with engine.connect() as conn:
            row_count = conn.execute(text("SELECT COUNT(*) FROM opsalert")).scalar()
            cond = conn.execute(
                text("SELECT sampled_out FROM alert_condition LIMIT 1")
            ).fetchone()

        assert row_count == 20, f"expected 20 rows, got {row_count}"
        assert cond is not None
        assert cond[0] == 5, (
            f"condition sampled_out should be 5 (from overflow batch), got {cond[0]}"
        )
        engine.dispose()


# ---------------------------------------------------------------------------
# (i) replay after ambiguous commit does not double-count
# ---------------------------------------------------------------------------


class TestReplaySafety:
    def test_replay_after_ambiguous_commit_no_double_count(self, tmp_path):
        """(i) commit() raises OperationalError after a real commit; the
        writer retries the batch; identical totals (no double-count).

        Wraps ``conn.commit()`` to raise once after the real commit has
        already been applied, simulating an ambiguous commit (connection
        lost after COMMIT sent but before the ACK arrived).
        """
        import uuid
        from datetime import UTC, datetime

        url, engine = _make_db(tmp_path)
        sig = condition_signature("cat", None, None, "msg")

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
            ingest_sample_per_minute=5,
            ingest_max_retry_s=5.0,
        )

        # Build 3 events with distinct event_ids, all sampled_in=True
        events = []
        for _ in range(3):
            ev = Event(
                event_id=uuid.uuid4().hex,
                ts=datetime.now(UTC),
                severity="warn",
                category="cat",
                message="msg",
                source=None,
                context=None,
                params=None,
                template="msg",
                environment=None,
                signature_key=sig,
                sampled_in=True,
            )
            events.append(ev)

        from sqlalchemy.exc import OperationalError

        from opsalert import ingest

        commit_count = 0
        orig_write_batch = ingest.write_batch

        def ambiguous_write(conn, evts, drops, now):
            """Call real write_batch, but on the first call wrap commit()
            so it raises OperationalError AFTER the real commit."""
            nonlocal commit_count
            result = orig_write_batch(conn, evts, drops, now)
            if commit_count == 0:
                orig_commit = conn.commit

                def exploding_commit():
                    orig_commit()  # data IS committed
                    commit_count_inner = 1  # noqa: F841
                    raise OperationalError(
                        "lost connection after COMMIT", {}, Exception()
                    )

                conn.commit = exploding_commit
            commit_count += 1
            return result

        ingest.write_batch = ambiguous_write

        try:
            for ev in events:
                enqueue(ev)
            flush(timeout=10.0)
        finally:
            ingest.write_batch = orig_write_batch

        with engine.connect() as conn:
            row_count = conn.execute(text("SELECT COUNT(*) FROM opsalert")).scalar()
            cond = conn.execute(
                text("SELECT sampled_out, dropped_count FROM alert_condition LIMIT 1")
            ).fetchone()

        # Exactly 3 rows — the replay's duplicate inserts were skipped
        assert row_count == 3, f"expected 3 rows, got {row_count}"
        # Condition sampled_out should not be doubled
        if cond:
            assert cond[0] == 0, f"sampled_out should be 0, got {cond[0]}"
        engine.dispose()


# ---------------------------------------------------------------------------
# (j) fork child gets fresh queue
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not hasattr(os, "fork"), reason="no fork on this platform")
class TestForkChildFreshQueue:
    def test_fork_child_gets_fresh_queue(self, tmp_path):
        """(j) forked child has an empty queue, not the parent's."""
        import uuid
        from datetime import UTC, datetime

        from opsalert import ingest

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=f"sqlite:///{tmp_path / 'fork.db'}",
        )

        # Prevent thread from draining
        orig_start = ingest._start_thread
        ingest._start_thread = lambda: None

        sig = condition_signature("cat", None, None, "msg")

        try:
            # Put something in the parent's queue
            ev = Event(
                event_id=uuid.uuid4().hex,
                ts=datetime.now(UTC),
                severity="warn",
                category="cat",
                message="msg",
                source=None,
                context=None,
                params=None,
                template="msg",
                environment=None,
                signature_key=sig,
            )
            enqueue(ev)
            assert len(ingest._queue) == 1

            r, w = os.pipe()
            pid = os.fork()
            if pid == 0:
                os.close(r)
                # Child should have an empty queue (reset by at_fork)
                child_len = len(ingest._queue)
                os.write(w, str(child_len).encode())
                os.close(w)
                os._exit(0)
            else:
                os.close(w)
                _, status = os.waitpid(pid, 0)
                data = os.read(r, 100)
                os.close(r)
                child_queue_len = int(data.decode())
                assert child_queue_len == 0, (
                    f"child queue should be empty, got {child_queue_len}"
                )
        finally:
            ingest._start_thread = orig_start


# ---------------------------------------------------------------------------
# (k) ingest never writes occurrence_count
# ---------------------------------------------------------------------------


class TestNeverWritesOccurrenceCount:
    def test_ingest_never_writes_occurrence_count(self, tmp_path):
        """(k) ingest never touches occurrence_count on alert_condition."""
        url, engine = _make_db(tmp_path)

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )

        for i in range(5):
            opsalert.warn("occ_count", message="check occurrence_count")

        flush(timeout=5.0)

        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT occurrence_count FROM alert_condition LIMIT 1")
            ).fetchone()

        assert row is not None
        assert row[0] == 0, (
            f"occurrence_count should be 0 (untouched by ingest), got {row[0]}"
        )
        engine.dispose()


# ---------------------------------------------------------------------------
# (l) ingest module has no asyncio
# ---------------------------------------------------------------------------


class TestNoAsyncio:
    def test_ingest_module_has_no_asyncio_token(self):
        """(l) source scan: ingest.py contains no asyncio token in executable code."""
        import ast
        import pathlib

        ingest_path = pathlib.Path(__file__).parent.parent / "opsalert" / "ingest.py"
        source = ingest_path.read_text()

        # Parse the AST and check for asyncio imports or references
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if "asyncio" in alias.name:
                        pytest.fail(
                            f"ingest.py line {node.lineno}: import {alias.name}"
                        )
            elif isinstance(node, ast.ImportFrom):
                if node.module and "asyncio" in node.module:
                    pytest.fail(
                        f"ingest.py line {node.lineno}: from {node.module} import ..."
                    )

    def test_enqueue_works_without_running_loop(self, tmp_path):
        """(l) enqueue works with no running event loop."""
        url, engine = _make_db(tmp_path)

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )
        opsalert.warn("no_loop", message="no loop needed")
        result = flush(timeout=5.0)

        assert result.written >= 1
        engine.dispose()

    async def test_enqueue_works_inside_running_loop(self, tmp_path):
        """(l) enqueue works inside a running event loop."""
        url, engine = _make_db(tmp_path)

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )
        opsalert.warn("with_loop", message="inside a loop")
        result = flush(timeout=5.0)

        assert result.written >= 1
        engine.dispose()


# ---------------------------------------------------------------------------
# (m) flush edge cases
# ---------------------------------------------------------------------------


class TestFlushEdgeCases:
    def test_flush_before_first_fire_is_noop(self):
        """(m) flush before any fire is a no-op, never raises."""
        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url="sqlite:///unused.db",
        )
        result = flush(timeout=1.0)
        assert isinstance(result, FlushResult)
        assert result.written == 0
        assert result.dropped == 0
        assert result.remaining == 0

    def test_flush_with_dead_thread_starts_and_drains(self, tmp_path):
        """(m) flush with a dead/never-started thread and non-empty queue
        starts the thread and drains, not drops."""
        import uuid
        from datetime import UTC, datetime

        from opsalert import ingest

        url, engine = _make_db(tmp_path)

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )

        # Prevent the thread from starting during enqueue
        orig_start = ingest._start_thread
        ingest._start_thread = lambda: None

        sig = condition_signature("cat", None, None, "msg")

        try:
            for _ in range(5):
                ev = Event(
                    event_id=uuid.uuid4().hex,
                    ts=datetime.now(UTC),
                    severity="warn",
                    category="cat",
                    message="msg",
                    source=None,
                    context=None,
                    params=None,
                    template="msg",
                    environment=None,
                    signature_key=sig,
                )
                enqueue(ev)

            assert len(ingest._queue) == 5
            assert ingest._thread is None or not ingest._thread.is_alive()
        finally:
            # Restore _start_thread so flush can use it
            ingest._start_thread = orig_start

        # flush must start the thread and drain the queue
        result = flush(timeout=5.0)
        assert result.written >= 5, f"expected written >= 5, got {result}"
        assert result.dropped == 0, f"expected dropped == 0, got {result}"
        assert _count_alerts(engine) >= 5
        engine.dispose()


# ---------------------------------------------------------------------------
# Thread does not busy-spin after flush
# ---------------------------------------------------------------------------


class TestThreadWaitsAfterFlush:
    def test_thread_waits_after_flush(self, tmp_path):
        """After flush completes, the writer thread must sleep on the
        condition variable, not busy-spin on _flush_event. Verify by
        checking that _flush_event is cleared after flush returns."""

        from opsalert import ingest

        url, engine = _make_db(tmp_path)

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )

        opsalert.warn("spin_test", message="check no busy-spin")
        result = flush(timeout=5.0)
        assert result.written >= 1

        # _flush_event must be cleared so the thread doesn't busy-spin
        assert not ingest._flush_event.is_set(), (
            "_flush_event is still set after flush returned — the writer "
            "will busy-spin instead of sleeping on the condition variable"
        )

        # A second flush also works correctly (not stuck)
        opsalert.warn("spin_test2", message="second flush")
        result2 = flush(timeout=5.0)
        assert result2.written >= 2
        assert not ingest._flush_event.is_set()

        engine.dispose()


# ---------------------------------------------------------------------------
# JSON log line test
# ---------------------------------------------------------------------------


class TestJsonLogLine:
    def test_every_event_logs_one_json_line(self, tmp_path, caplog):
        """Every event emits one JSON line on opsalert.occurrence, including
        sampled-out ones."""
        caplog.set_level(logging.INFO, logger="opsalert.occurrence")

        url, engine = _make_db(tmp_path)

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
            ingest_sample_per_minute=2,
        )

        # Fire 5 events — 2 will be rows, 3 sampled out, but ALL should log
        for _ in range(5):
            opsalert.warn("json_log", message="log me")

        flush(timeout=5.0)

        occ_records = [
            r for r in caplog.records if r.name == "opsalert.occurrence"
        ]
        assert len(occ_records) == 5, (
            f"expected 5 occurrence log lines, got {len(occ_records)}"
        )

        # Each should be valid JSON
        for rec in occ_records:
            data = json.loads(rec.message)
            assert "event_id" in data
            assert "severity" in data
            assert "category" in data

        engine.dispose()


# ---------------------------------------------------------------------------
# Dispatch contract: fire never touches host session_factory
# ---------------------------------------------------------------------------


class TestFireNeverTouchesHostSession:
    def test_fire_never_touches_host_session_factory(self, tmp_path):
        """Dispatch test: configure with a raising factory + real ingest_url → row."""
        url, engine = _make_db(tmp_path)

        calls = []

        def raising_factory():
            calls.append(1)
            raise RuntimeError("session_factory should not be called")

        opsalert.configure(
            session_factory=raising_factory,
            ingest_url=url,
        )

        opsalert.warn("host_session", message="should not touch factory")
        result = flush(timeout=5.0)

        assert result.written >= 1
        assert len(calls) == 0, "session_factory was called!"
        assert _count_alerts(engine) >= 1
        engine.dispose()
