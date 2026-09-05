"""Condition lifecycle — statistics, automatic rules, human transitions.

Covers A4 (acknowledged), A5 (auto-close/auto-stale/reopen) and the stats
half of A6 (the watermark must never run ahead of what was counted).
"""
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from opsalert.lifecycle import (
    ACK_BURST_MIN,
    AUTO_CLOSE_FLOOR,
    apply_lifecycle_rules,
    distinct_subjects,
    effective_disposition,
    record_subjects,
    set_disposition,
    set_status,
    sync_condition_stats,
)
from opsalert.model import Alert, AlertCondition
from opsalert.store import fire_alert

LONG_AGO = timedelta(minutes=10)


async def _fire_backdated(session, *, age=LONG_AGO, severity="error", category="cat",
                          message="boom", now=None):
    """Fire an occurrence and age it past the stats lag window."""
    alert = await fire_alert(session, severity=severity, category=category, message=message)
    alert.created = (now or datetime.now(UTC)) - age
    await session.flush()
    return alert


async def _the_condition(session) -> AlertCondition:
    return (await session.execute(select(AlertCondition))).scalars().first()


class TestStatsWatermark:
    async def test_counts_occurrences_into_the_condition(self, session):
        for _ in range(3):
            await _fire_backdated(session)
        await session.commit()

        stats = await sync_condition_stats(session)
        await session.commit()

        condition = await _the_condition(session)
        assert stats["occurrences_counted"] == 3
        assert condition.occurrence_count == 3
        assert condition.first_seen is not None
        assert condition.last_seen is not None

    async def test_counting_is_not_repeated_on_the_next_sweep(self, session):
        await _fire_backdated(session)
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        assert (await _the_condition(session)).occurrence_count == 1

    async def test_an_occurrence_inside_the_lag_window_is_not_counted_yet(self, session):
        """A6/P3: auto-increment order is not commit order.

        A row younger than the lag could have a lower id than one already
        committed; counting it now would let the watermark run past a row that
        is still in flight — and cleanup treats everything at or below the
        watermark as safe to delete.
        """
        await fire_alert(session, severity="error", category="cat", message="boom")
        await session.commit()

        stats = await sync_condition_stats(session)
        await session.commit()

        condition = await _the_condition(session)
        assert stats["occurrences_counted"] == 0
        assert condition.occurrence_count == 0
        assert condition.stats_synced_through == 0

    async def test_the_watermark_stops_below_the_uncounted_row(self, session):
        old = await _fire_backdated(session)
        fresh = await fire_alert(session, severity="error", category="cat", message="boom")
        await session.commit()

        await sync_condition_stats(session)
        await session.commit()

        condition = await _the_condition(session)
        assert condition.stats_synced_through == old.id
        assert condition.stats_synced_through < fresh.id
        assert condition.occurrence_count == 1

    async def test_worst_severity_is_kept_and_latest_severity_tracks_the_newest(
        self, session
    ):
        now = datetime.now(UTC)
        await _fire_backdated(session, severity="critical", age=timedelta(minutes=30), now=now)
        await _fire_backdated(session, severity="warn", age=timedelta(minutes=10), now=now)
        await session.commit()

        await sync_condition_stats(session)
        await session.commit()

        condition = await _the_condition(session)
        assert condition.severity == "critical"
        assert condition.latest_severity == "warn"

    async def test_median_interval_is_computed_from_the_gaps(self, session):
        now = datetime.now(UTC)
        for minutes in (40, 30, 20, 10):
            await _fire_backdated(session, age=timedelta(minutes=minutes), now=now)
        await session.commit()

        await sync_condition_stats(session, now=now)
        await session.commit()

        assert (await _the_condition(session)).median_interval_seconds == 600

    async def test_a_single_occurrence_has_no_median(self, session):
        await _fire_backdated(session)
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        assert (await _the_condition(session)).median_interval_seconds is None


class TestOrphanAdoption:
    async def test_orphans_are_adopted_and_counted(self, session):
        session.add(
            Alert(
                severity="error",
                category="cat",
                message="orphaned boom",
                created=datetime.now(UTC) - LONG_AGO,
            )
        )
        await session.commit()

        stats = await sync_condition_stats(session)
        await session.commit()

        assert stats["adopted"] == 1
        condition = await _the_condition(session)
        assert condition.message_template == "orphaned boom"
        assert condition.occurrence_count == 1
        orphan = (await session.execute(select(Alert))).scalar_one()
        assert orphan.condition_id == condition.id

    async def test_adoption_is_not_gated_by_the_stats_watermark(self, session):
        """P2: an orphan below the watermark is still adopted AND still counted.

        The watermark scan can never see it — that is exactly why adoption is
        unbounded and counts what it links. Gating the repair on the same
        bookkeeping that failed would make the loss permanent.

        The orphan carries v2 identity fields (as the ingest queue would
        stamp them) so adoption matches it to the v2 condition.
        """
        import json

        from opsalert.signature import (
            event_fingerprint_parts,
            normalize_message,
        )

        await _fire_backdated(session, message="boom")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        condition = await _the_condition(session)
        condition.stats_synced_through = 10_000  # far above any row we will make
        # Build the same v2 identity the ingest queue would stamp on a
        # condition-resolution failure (F1).
        tmpl = normalize_message("boom")
        fp_parts = event_fingerprint_parts(
            kind="cat.legacy",
            environment=None,
            exception_chain=[],
            origin_frame="",
            template=tmpl,
        )
        session.add(
            Alert(
                severity="error",
                category="cat",
                message="boom",
                created=datetime.now(UTC) - LONG_AGO,
                fingerprint_version=2,
                fingerprint_json=json.dumps(fp_parts),
                kind="cat.legacy",
            )
        )
        await session.commit()

        stats = await sync_condition_stats(session)
        await session.commit()

        await session.refresh(condition)
        assert stats["adopted"] == 1
        assert condition.occurrence_count == 2, (
            "an adopted orphan under the watermark was never counted — its "
            "occurrence is invisible in the condition's history forever"
        )

    async def test_environment_is_read_from_the_stored_context(self, session):
        session.add(
            Alert(
                severity="error",
                category="cat",
                message="boom",
                context_json='{"environment": "staging"}',
                created=datetime.now(UTC) - LONG_AGO,
            )
        )
        session.add(
            Alert(
                severity="error",
                category="cat",
                message="boom",
                context_json='{"environment": "production"}',
                created=datetime.now(UTC) - LONG_AGO,
            )
        )
        await session.commit()

        await sync_condition_stats(session)
        await session.commit()

        conditions = (await session.execute(select(AlertCondition))).scalars().all()
        assert {c.environment for c in conditions} == {"staging", "production"}

    async def test_unreadable_context_does_not_stop_adoption(self, session):
        session.add(
            Alert(
                severity="error",
                category="cat",
                message="boom",
                context_json="{not json at all",
                created=datetime.now(UTC) - LONG_AGO,
            )
        )
        await session.commit()

        stats = await sync_condition_stats(session)
        await session.commit()
        assert stats["adopted"] == 1


class TestAdoptionIdentity:
    """opsalert#2: an adopted orphan must land under the EMIT path's identity.

    Emit time uses the raw template as identity when ``params`` is passed;
    the occurrence row stores only the rendered text. Adoption must reuse the
    template the fire persisted on the row — normalizing the rendered message
    instead forks a second, permanently-empty condition.
    """

    async def test_degraded_params_fire_is_adopted_into_the_emit_condition(
        self, session, engine, monkeypatch
    ):
        """The headline: attachment degrades AFTER the condition committed.

        The sabotaged factory commits the condition and then blows up, which
        is F1's worst shape: the emit-identity condition exists (×0), and the
        occurrence is stored as an orphan. The sweep must adopt the orphan
        into THAT condition — one condition, populated — never mint a second
        one from the rendered message.
        """
        from contextlib import asynccontextmanager

        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

        from opsalert import store
        from opsalert.store import fire_alert

        maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        class _DropsAfterCommit:
            """Session whose connection 'dies' right after a successful commit."""

            def __init__(self, inner):
                self._inner = inner

            def __getattr__(self, name):
                return getattr(self._inner, name)

            async def commit(self):
                await self._inner.commit()
                raise RuntimeError("connection dropped after commit")

        @asynccontextmanager
        async def sabotaged_factory():
            async with maker() as inner:
                yield _DropsAfterCommit(inner)

        # Report a non-SQLite dialect so the isolated-session branch runs
        # (the same device the store's degradation tests use).
        monkeypatch.setattr(store, "_dialect_name", lambda _s: "postgresql")
        import opsalert

        opsalert.configure(session_factory=sabotaged_factory)

        template = "PUT /api/view/shares/{stub}/ exceeded its budget"
        alert = await fire_alert(
            session,
            severity="error",
            category="request_anomaly",
            message=template,
            params={"stub": "ChFICzP9VHlILNzd"},
        )
        await session.commit()
        assert alert.condition_id is None  # attachment degraded (F1)
        # The emit-identity condition committed before the "connection died".
        assert len((await session.execute(select(AlertCondition))).scalars().all()) == 1

        stats = await sync_condition_stats(session)
        await session.commit()

        assert stats["adopted"] == 1
        condition = (await session.execute(select(AlertCondition))).scalar_one()
        assert condition.message_template == template
        assert condition.occurrence_count == 0  # counted later, past the lag
        orphan = (await session.execute(select(Alert))).scalar_one()
        assert orphan.condition_id == condition.id

    async def test_old_style_orphan_falls_back_to_the_normalized_message(
        self, session
    ):
        """A row that predates the stored template still adopts, by normalizer."""
        from opsalert.signature import normalize_message

        session.add(
            Alert(
                severity="error",
                category="cat",
                message="boom 123",
                created=datetime.now(UTC) - LONG_AGO,
            )
        )
        await session.commit()

        stats = await sync_condition_stats(session)
        await session.commit()

        assert stats["adopted"] == 1
        condition = (await session.execute(select(AlertCondition))).scalar_one()
        assert condition.message_template == normalize_message("boom 123") == "boom <n>"


class TestAutomaticRules:
    async def _resolved_condition(self, session, *, silent_for, median=None):
        now = datetime.now(UTC)
        await _fire_backdated(session, age=silent_for, now=now)
        await session.commit()
        await sync_condition_stats(session, now=now)
        condition = await _the_condition(session)
        # Resolution happens after the last occurrence, so silence is measured
        # from last_seen — which is exactly ``silent_for`` ago.
        await set_status(session, condition, "resolved", actor="chris")
        condition.median_interval_seconds = median
        await session.commit()
        return condition

    async def test_resolved_and_silent_auto_closes(self, session):
        condition = await self._resolved_condition(session, silent_for=timedelta(hours=7))
        result = await apply_lifecycle_rules(session)
        await session.commit()

        assert result["auto_closed"] == 1
        assert condition.status == "closed"
        assert condition.closed_at is not None

    async def test_the_six_hour_floor_holds_for_a_chatty_condition(self, session):
        """A condition that fires every second must not close over lunch."""
        condition = await self._resolved_condition(
            session, silent_for=timedelta(hours=3), median=1
        )
        assert AUTO_CLOSE_FLOOR == timedelta(hours=6)
        await apply_lifecycle_rules(session)
        await session.commit()
        assert condition.status == "resolved"

    async def test_ten_times_the_median_governs_a_slow_condition(self, session):
        """Median of a day → six hours of silence proves nothing."""
        condition = await self._resolved_condition(
            session, silent_for=timedelta(hours=8), median=86_400
        )
        await apply_lifecycle_rules(session)
        await session.commit()
        assert condition.status == "resolved"

    async def test_an_untriaged_condition_goes_stale_after_thirty_days(self, session):
        now = datetime.now(UTC)
        await _fire_backdated(session, age=timedelta(days=31), now=now)
        await session.commit()
        await sync_condition_stats(session, now=now)
        await session.commit()

        result = await apply_lifecycle_rules(session, now=now)
        await session.commit()

        condition = await _the_condition(session)
        assert result["auto_staled"] == 1
        assert condition.status == "closed"
        assert "auto-closed" in (condition.notes or "")

    async def test_a_recent_new_condition_is_left_alone(self, session):
        now = datetime.now(UTC)
        await _fire_backdated(session, age=timedelta(days=29), now=now)
        await session.commit()
        await sync_condition_stats(session, now=now)
        await session.commit()

        await apply_lifecycle_rules(session, now=now)
        await session.commit()
        assert (await _the_condition(session)).status == "new"

    async def test_a_closed_condition_reopens_when_it_happens_again(self, session):
        """A5: mis-closure is self-correcting — recurrence brings it back."""
        now = datetime.now(UTC)
        # The condition went stale and was closed an hour ago.
        closed_at = now - timedelta(hours=1)
        await _fire_backdated(session, age=timedelta(days=31), now=now)
        await session.commit()
        await sync_condition_stats(session, now=closed_at)
        await apply_lifecycle_rules(session, now=closed_at)
        await session.commit()
        condition = await _the_condition(session)
        assert condition.status == "closed"

        await _fire_backdated(session, age=LONG_AGO)
        await session.commit()
        await sync_condition_stats(session)
        result = await apply_lifecycle_rules(session)
        await session.commit()

        assert result["reopened"] == 1
        assert condition.status == "new"
        assert condition.reopened_count == 1
        assert condition.closed_at is None


class TestHumanTransitions:
    async def _condition(self, session):
        await _fire_backdated(session)
        await session.commit()
        return await _the_condition(session)

    async def test_acknowledging_stamps_who_and_when(self, session):
        """P8 — a state change nobody can attribute is not an audit trail."""
        condition = await self._condition(session)
        before = datetime.now(UTC)
        await set_status(session, condition, "acknowledged", actor="chris", issue_url="https://github.com/test/1")
        await session.commit()

        assert condition.status == "acknowledged"
        assert condition.acknowledged_by == "chris"
        assert condition.acknowledged_at is not None
        assert condition.acknowledged_at.replace(tzinfo=None) >= before.replace(tzinfo=None)
        assert condition.status_changed_at is not None

    async def test_resolving_records_the_issue_and_the_person(self, session):
        condition = await self._condition(session)
        await set_status(
            session,
            condition,
            "resolved",
            actor="chris",
            issue_url="https://github.com/vingapp/vingapi/pull/144",
        )
        await session.commit()

        assert condition.resolved_at is not None
        assert condition.resolved_by == "chris"
        assert condition.issue_url.endswith("/144")

    async def test_reopening_clears_a_stale_acknowledgement(self, session):
        """The person who acknowledged the last episode has not seen this one."""
        condition = await self._condition(session)
        await set_status(session, condition, "acknowledged", actor="chris", issue_url="https://github.com/test/1")
        await set_status(session, condition, "new", actor="chris")
        await session.commit()

        assert condition.acknowledged_at is None
        assert condition.acknowledged_by is None

    async def test_an_impossible_transition_is_refused(self, session):
        condition = await self._condition(session)
        await set_status(session, condition, "closed", actor="chris")
        with pytest.raises(ValueError):
            await set_status(session, condition, "resolved", actor="chris")

    async def test_an_unknown_status_is_refused(self, session):
        condition = await self._condition(session)
        with pytest.raises(ValueError):
            await set_status(session, condition, "wontfix", actor="chris")

    async def test_status_can_be_set_by_id(self, session):
        condition = await self._condition(session)
        await set_status(session, condition.id, "acknowledged", actor="chris", issue_url="https://github.com/test/1")
        assert condition.status == "acknowledged"

    async def test_an_unknown_disposition_is_refused(self, session):
        condition = await self._condition(session)
        with pytest.raises(ValueError):
            await set_disposition(session, condition, "loudly")

    async def test_disposition_can_be_set_and_cleared(self, session):
        condition = await self._condition(session)
        await set_disposition(session, condition, "collect", actor="chris")
        assert condition.disposition == "collect"
        await set_disposition(session, condition, None, actor="chris")
        assert condition.disposition is None


class TestAckEscalation:
    """opsalert#7: an acknowledged condition returns to ``new`` when it gets
    worse or a lease expires — "acknowledged" must not mean "silent forever".
    """

    async def _synced_condition(self, session, *, occurrences=1, severity="warn", now=None):
        now = now or datetime.now(UTC)
        for _ in range(occurrences):
            await _fire_backdated(session, severity=severity, now=now)
        await session.commit()
        await sync_condition_stats(session, now=now)
        await session.commit()
        return await _the_condition(session)

    async def test_ack_stamps_severity_count_and_lease(self, session):
        """If ack stops stamping the baseline, escalation has nothing to
        compare the current state against and can never fire."""
        now = datetime.now(UTC)
        condition = await self._synced_condition(session, occurrences=3, severity="warn", now=now)
        await set_status(session, condition, "acknowledged", actor="chris", now=now, issue_url="https://github.com/test/1")
        await session.commit()

        assert condition.acknowledged_severity == "warn"
        assert condition.acknowledged_occurrence_count == 3
        assert condition.acknowledged_until is None

    async def test_ack_rejects_lease_in_the_past(self, session):
        """A lease already in the past would reopen on the very next sweep
        — that isn't a lease, it's a no-op acknowledgement."""
        now = datetime.now(UTC)
        condition = await self._synced_condition(session, now=now)
        with pytest.raises(ValueError):
            await set_status(
                session,
                condition,
                "acknowledged",
                actor="chris",
                now=now,
                acknowledged_until=now - timedelta(minutes=1),
            )

    async def test_reack_restamps_baseline(self, session):
        """Re-acknowledging must re-baseline, or a renewed ack keeps
        comparing new occurrences against a stale count/severity forever."""
        now = datetime.now(UTC)
        condition = await self._synced_condition(session, occurrences=1, severity="warn", now=now)
        await set_status(session, condition, "acknowledged", actor="chris", now=now, issue_url="https://github.com/test/1")
        await session.commit()
        assert condition.acknowledged_occurrence_count == 1
        assert condition.acknowledged_severity == "warn"

        later = now + timedelta(hours=1)
        await _fire_backdated(session, severity="error", age=timedelta(minutes=5), now=later)
        await session.commit()
        await set_status(session, condition, "acknowledged", actor="chris", now=later, issue_url="https://github.com/test/1")
        await session.commit()

        assert condition.acknowledged_occurrence_count == 2
        assert condition.acknowledged_severity == "error"

    async def test_escalate_severity_reopens_acknowledged(self, session):
        """If escalation doesn't fire, a condition that got strictly worse
        after acknowledgement stays silently acknowledged."""
        now = datetime.now(UTC)
        condition = await self._synced_condition(session, occurrences=2, severity="warn", now=now)
        await set_status(session, condition, "acknowledged", actor="chris", now=now, issue_url="https://github.com/test/1")
        await session.commit()

        later = now + timedelta(minutes=10)
        await _fire_backdated(session, severity="error", age=timedelta(minutes=1), now=later)
        await session.commit()

        result = await apply_lifecycle_rules(session, now=later + timedelta(minutes=5))
        await session.commit()

        assert result["escalated"] == 1
        assert condition.status == "new"
        assert condition.reopened_count == 1
        assert "severity escalated" in (condition.notes or "")
        assert condition.acknowledged_at is None
        assert condition.acknowledged_severity is None
        assert condition.acknowledged_occurrence_count is None
        assert condition.acknowledged_until is None

    async def test_escalate_severity_ignores_pre_ack_occurrences(self, session):
        """Rule 1 must only look at occurrences since the ack, or an old
        error from before the ack would reopen every warn-level condition."""
        now = datetime.now(UTC)
        await _fire_backdated(session, severity="error", age=timedelta(minutes=30), now=now)
        await session.commit()
        await sync_condition_stats(session, now=now)
        await session.commit()
        condition = await _the_condition(session)
        await set_status(session, condition, "acknowledged", actor="chris", now=now, issue_url="https://github.com/test/1")
        await session.commit()
        assert condition.acknowledged_severity == "error"

        later = now + timedelta(minutes=10)
        await _fire_backdated(session, severity="warn", age=timedelta(minutes=1), now=later)
        await session.commit()

        result = await apply_lifecycle_rules(session, now=later + timedelta(minutes=5))
        await session.commit()

        assert result["escalated"] == 0
        assert condition.status == "acknowledged"

    async def test_burst_reopens_acknowledged(self, session):
        """If burst detection doesn't fire, a convoy-shaped condition — a
        near-instant median even at a modest long-run rate — never re-wakes
        the watch."""
        now = datetime.now(UTC)
        for h in range(24):
            await _fire_backdated(session, severity="warn", age=timedelta(hours=24 - h), now=now)
        await session.commit()
        await sync_condition_stats(session, now=now)
        await session.commit()
        condition = await _the_condition(session)
        assert condition.occurrence_count == 24
        await set_status(session, condition, "acknowledged", actor="chris", now=now, issue_url="https://github.com/test/1")
        await session.commit()

        later = now + timedelta(hours=2)
        for _ in range(12):
            await _fire_backdated(session, severity="warn", age=timedelta(minutes=1), now=later)
        await session.commit()

        result = await apply_lifecycle_rules(session, now=later + timedelta(minutes=1))
        await session.commit()

        assert result["escalated"] == 1
        assert condition.status == "new"
        assert "burst" in (condition.notes or "")

    async def test_burst_below_minimum_does_not_reopen(self, session):
        """ACK_BURST_MIN exists so a handful of extra occurrences — normal
        noise — never trips a reopen; only a real burst should."""
        now = datetime.now(UTC)
        for h in range(24):
            await _fire_backdated(session, severity="warn", age=timedelta(hours=24 - h), now=now)
        await session.commit()
        await sync_condition_stats(session, now=now)
        await session.commit()
        condition = await _the_condition(session)
        await set_status(session, condition, "acknowledged", actor="chris", now=now, issue_url="https://github.com/test/1")
        await session.commit()

        later = now + timedelta(hours=2)
        for _ in range(9):
            await _fire_backdated(session, severity="warn", age=timedelta(minutes=1), now=later)
        await session.commit()

        result = await apply_lifecycle_rules(session, now=later + timedelta(minutes=1))
        await session.commit()

        assert result["escalated"] == 0
        assert condition.status == "acknowledged"

    async def test_burst_within_baseline_does_not_reopen(self, session):
        """The burst rule compares against max(10, 1.5 * pre-ack peak) —
        a condition whose pre-ack peak was high enough does not reopen
        for a post-ack burst that is within 1.5x of that peak."""
        now = datetime.now(UTC)
        await _fire_backdated(session, severity="warn", age=timedelta(minutes=30), now=now)
        await session.commit()
        await sync_condition_stats(session, now=now)
        await session.commit()
        condition = await _the_condition(session)
        await set_status(session, condition, "acknowledged", actor="chris", now=now, issue_url="https://github.com/test/1")
        # Pre-ack peak of 20 → threshold = max(10, 30) = 30.
        condition.acknowledged_peak_15m = 20
        await session.commit()

        later = now + timedelta(hours=2)
        # 15 in 15m < threshold of 30 → no reopen.
        for _ in range(15):
            await _fire_backdated(session, severity="warn", age=timedelta(minutes=1), now=later)
        await session.commit()

        result = await apply_lifecycle_rules(session, now=later + timedelta(minutes=1))
        await session.commit()

        assert result["escalated"] == 0
        assert condition.status == "acknowledged"

    async def test_ack_mid_burst_baseline_uses_live_occurrences_not_stale_counter(
        self, session
    ):
        """Amendment: ``occurrence_count`` lags a sweep + STATS_LAG_SECONDS.
        Acking mid-burst while that column is still 0 must not baseline at
        0 (which would re-trip on the very next sweep) — the ack-time count
        is read directly off Alert rows created at or before the ack."""
        now = datetime.now(UTC)
        for _ in range(12):
            await _fire_backdated(session, severity="warn", age=timedelta(minutes=1), now=now)
        await session.commit()
        condition = await _the_condition(session)
        assert condition.occurrence_count == 0  # never synced — the stale value

        await set_status(session, condition, "acknowledged", actor="chris", now=now, issue_url="https://github.com/test/1")
        await session.commit()
        assert condition.acknowledged_occurrence_count == 12

        # A sweep one minute later, nothing new fired: must not reopen.
        result = await apply_lifecycle_rules(session, now=now + timedelta(minutes=1))
        await session.commit()
        assert result["escalated"] == 0
        assert condition.status == "acknowledged"

        # 12 more within baseline (baseline = 12/hour floored to 1h -> 3/15m;
        # 12 in 15m is under 5x -> still no reopen).
        later = now + timedelta(minutes=10)
        for _ in range(12):
            await _fire_backdated(session, severity="warn", age=timedelta(minutes=1), now=later)
        await session.commit()
        result = await apply_lifecycle_rules(session, now=later + timedelta(minutes=1))
        await session.commit()
        assert result["escalated"] == 0
        assert condition.status == "acknowledged"

        # A real burst well past 5x baseline reopens it.
        even_later = later + timedelta(minutes=10)
        for _ in range(20):
            await _fire_backdated(
                session, severity="warn", age=timedelta(minutes=1), now=even_later
            )
        await session.commit()
        result = await apply_lifecycle_rules(session, now=even_later + timedelta(minutes=1))
        await session.commit()
        assert result["escalated"] == 1
        assert condition.status == "new"

    async def test_collect_disposition_is_exempt_from_escalation(self, session):
        """acknowledged + collect is the spelling of wontfix: a human decided
        this is never acted on. If escalation reopened it, every deploy-time
        burst on a wontfix condition would churn reopened_count and notes for
        nobody — collect never wakes anyone regardless of status."""
        now = datetime.now(UTC)
        condition = await self._synced_condition(session, occurrences=1, severity="warn", now=now)
        await set_status(session, condition, "acknowledged", actor="chris", now=now, issue_url="https://github.com/test/1")
        await set_disposition(session, condition, "collect", actor="chris")
        await session.commit()

        later = now + timedelta(minutes=5)
        for _ in range(ACK_BURST_MIN + 5):
            await _fire_backdated(session, severity="critical", now=later)
        await session.commit()
        await sync_condition_stats(session, now=later + timedelta(minutes=2))
        result = await apply_lifecycle_rules(session, now=later + timedelta(minutes=2))
        await session.commit()

        assert result["escalated"] == 0
        assert condition.status == "acknowledged"

    async def test_lease_expiry_reopens_when_still_firing(self, session):
        """If lease expiry doesn't fire, a time-boxed ack becomes a
        permanent one the moment the operator forgets to follow up."""
        now = datetime.now(UTC)
        condition = await self._synced_condition(session, occurrences=1, severity="warn", now=now)
        await set_status(
            session,
            condition,
            "acknowledged",
            actor="chris",
            now=now,
            acknowledged_until=now + timedelta(hours=1),
        )
        await session.commit()

        fire_time = now + timedelta(minutes=30)
        await _fire_backdated(session, severity="warn", age=timedelta(minutes=1), now=fire_time)
        await session.commit()

        sweep_now = now + timedelta(hours=2)
        await sync_condition_stats(session, now=sweep_now)
        await session.commit()

        result = await apply_lifecycle_rules(session, now=sweep_now)
        await session.commit()

        assert result["escalated"] == 1
        assert condition.status == "new"
        assert "lease expired" in (condition.notes or "")

    async def test_lease_expiry_quiet_condition_stays_acknowledged(self, session):
        """Expiry with no occurrence since ack must be a no-op — the
        existing auto-close/auto-stale rules own a condition that went
        quiet, not this one."""
        now = datetime.now(UTC)
        condition = await self._synced_condition(session, occurrences=1, severity="warn", now=now)
        await set_status(
            session,
            condition,
            "acknowledged",
            actor="chris",
            now=now,
            acknowledged_until=now + timedelta(hours=1),
        )
        await session.commit()

        result = await apply_lifecycle_rules(session, now=now + timedelta(hours=2))
        await session.commit()

        assert result["escalated"] == 0
        assert condition.status == "acknowledged"

    async def test_lease_not_expired_no_reopen(self, session):
        """A lease still inside its window must not reopen the condition,
        or 'acknowledge with a lease' just means 'acknowledge, then
        immediately reopen'."""
        now = datetime.now(UTC)
        condition = await self._synced_condition(session, occurrences=1, severity="warn", now=now)
        await set_status(
            session,
            condition,
            "acknowledged",
            actor="chris",
            now=now,
            acknowledged_until=now + timedelta(hours=1),
        )
        await session.commit()

        fire_time = now + timedelta(minutes=10)
        await _fire_backdated(session, severity="warn", age=timedelta(minutes=1), now=fire_time)
        await session.commit()
        sweep_now = now + timedelta(minutes=30)
        await sync_condition_stats(session, now=sweep_now)
        await session.commit()

        result = await apply_lifecycle_rules(session, now=sweep_now)
        await session.commit()

        assert result["escalated"] == 0
        assert condition.status == "acknowledged"

    async def test_legacy_ack_without_baseline(self, session):
        """Amendment: a legacy NULL ``acknowledged_severity`` must SKIP rule
        1 rather than fall back to ``condition.severity`` — by the time this
        rule runs, ``sync_condition_stats`` has already folded any post-ack
        occurrence into ``condition.severity``, so "worse than worst-ever"
        could never trip and a fallback would silently disable escalation
        for exactly the legacy rows that need it most."""
        now = datetime.now(UTC)
        await _fire_backdated(session, severity="error", age=timedelta(minutes=30), now=now)
        await session.commit()
        await sync_condition_stats(session, now=now)
        await session.commit()
        condition = await _the_condition(session)
        await set_status(session, condition, "acknowledged", actor="chris", now=now, issue_url="https://github.com/test/1")
        # Simulate a pre-migration row: baseline unknowable.
        condition.acknowledged_severity = None
        condition.acknowledged_occurrence_count = None
        await session.commit()

        # A brand-new critical occurrence (worse than anything ever seen)
        # must NOT escalate via rule 1 while severity baseline is NULL.
        later = now + timedelta(minutes=10)
        await _fire_backdated(session, severity="critical", age=timedelta(minutes=1), now=later)
        await session.commit()
        await sync_condition_stats(session, now=later + timedelta(minutes=5))
        await session.commit()

        result = await apply_lifecycle_rules(session, now=later + timedelta(minutes=5))
        await session.commit()
        assert result["escalated"] == 0
        assert condition.status == "acknowledged"

        # NULL peak baselines at 0 — threshold = max(10, 0) = 10, so any
        # burst > 10 reopens even with severity escalation unavailable.
        condition.acknowledged_peak_15m = None
        await session.commit()
        even_later = later + timedelta(minutes=20)
        for _ in range(11):
            await _fire_backdated(
                session, severity="warn", age=timedelta(minutes=1), now=even_later
            )
        await session.commit()
        result2 = await apply_lifecycle_rules(session, now=even_later + timedelta(minutes=1))
        await session.commit()
        assert result2["escalated"] == 1
        assert condition.status == "new"
        assert "burst" in (condition.notes or "")


class TestEffectiveDisposition:
    @pytest.mark.parametrize(
        "severity,expected", [("critical", "immediate"), ("error", "immediate"), ("warn", "digest")]
    )
    def test_default_derives_from_severity(self, severity, expected):
        assert effective_disposition(severity, None) == expected

    def test_an_explicit_override_wins(self):
        assert effective_disposition("critical", "collect") == "collect"
        assert effective_disposition("warn", "immediate") == "immediate"

    def test_an_unknown_stored_disposition_falls_back_to_the_default(self):
        assert effective_disposition("error", "garbage") == "immediate"


class TestAckRequiresIssue:
    """Acknowledged = owned: ack without issue_url is refused unless snoozing."""

    async def _condition(self, session):
        await _fire_backdated(session)
        await session.commit()
        return await _the_condition(session)

    async def test_ack_without_issue_raises_ack_requires_issue(self, session):
        """If ack does not require an issue, acknowledged conditions have no
        owner on record and the ack is operationally meaningless."""
        condition = await self._condition(session)
        with pytest.raises(ValueError, match="ack_requires_issue"):
            await set_status(session, condition, "acknowledged", actor="chris")

    async def test_ack_with_issue_url_ok(self, session):
        """Providing an issue at ack time satisfies the requirement."""
        condition = await self._condition(session)
        await set_status(
            session, condition, "acknowledged", actor="chris",
            issue_url="https://github.com/vingapp/opsalert/issues/7",
        )
        await session.commit()
        assert condition.status == "acknowledged"
        assert condition.issue_url == "https://github.com/vingapp/opsalert/issues/7"

    async def test_ack_with_existing_issue_url_ok(self, session):
        """A condition that already has an issue_url can be acked without passing one."""
        condition = await self._condition(session)
        condition.issue_url = "https://github.com/vingapp/opsalert/issues/7"
        await session.flush()
        await set_status(session, condition, "acknowledged", actor="chris")
        await session.commit()
        assert condition.status == "acknowledged"

    async def test_ack_with_acknowledged_until_and_no_issue_ok_snooze(self, session):
        """acknowledged_until with no issue = snooze, not a full ack."""
        now = datetime.now(UTC)
        condition = await self._condition(session)
        await set_status(
            session, condition, "acknowledged", actor="chris",
            now=now,
            acknowledged_until=now + timedelta(hours=1),
        )
        await session.commit()
        assert condition.status == "acknowledged"
        assert condition.acknowledged_until is not None


class TestBurstPreAckPeak:
    """Burst detection uses pre-ack 15-min peak instead of lifetime-mean."""

    async def _synced_condition(self, session, *, occurrences=1, severity="warn", now=None):
        now = now or datetime.now(UTC)
        for _ in range(occurrences):
            await _fire_backdated(session, severity=severity, now=now)
        await session.commit()
        await sync_condition_stats(session, now=now)
        await session.commit()
        return await _the_condition(session)

    async def test_burst_uses_pre_ack_peak(self, session):
        """Build 24h of occurrences with a known 15m peak; a post-ack window
        at 1.4x does not trip, 1.6x does.

        Threshold = max(10, 1.5 * acknowledged_peak_15m).
        Peak of 20 → threshold = max(10, 30) = 30.
        28 in 15m does not trip (28 <= 30); 31 does (31 > 30).
        """
        now = datetime.now(UTC)
        # Build a known peak: 20 occurrences in a single 15-minute window.
        # Background: 1 per hour for 24 hours.
        for h in range(24):
            await _fire_backdated(
                session, severity="warn",
                age=timedelta(hours=24 - h), now=now,
            )
        # A burst of 20 in a 15-minute window 12 hours ago = the peak.
        for _ in range(20):
            await _fire_backdated(
                session, severity="warn",
                age=timedelta(hours=12, minutes=5), now=now,
            )
        await session.commit()
        await sync_condition_stats(session, now=now)
        await session.commit()
        condition = await _the_condition(session)
        await set_status(
            session, condition, "acknowledged", actor="chris", now=now,
            issue_url="https://github.com/test/1",
        )
        await session.commit()
        # acknowledged_peak_15m should be >= 20
        assert condition.acknowledged_peak_15m is not None
        assert condition.acknowledged_peak_15m >= 20

        # 28 in 15m: should NOT trip (28 <= 30)
        later = now + timedelta(hours=2)
        for _ in range(28):
            await _fire_backdated(
                session, severity="warn", age=timedelta(minutes=1), now=later,
            )
        await session.commit()
        result = await apply_lifecycle_rules(session, now=later + timedelta(minutes=1))
        await session.commit()
        assert result["escalated"] == 0
        assert condition.status == "acknowledged"

        # The pre-ack peak can include a background occurrence landing in the
        # same 15-min bucket, so the actual peak may be 21 (20+1). Threshold =
        # max(10, floor(1.5*21)) = 31. Add enough to reach 32 total = 4 more.
        for _ in range(4):
            await _fire_backdated(
                session, severity="warn", age=timedelta(minutes=1), now=later,
            )
        await session.commit()
        result2 = await apply_lifecycle_rules(session, now=later + timedelta(minutes=1))
        await session.commit()
        assert result2["escalated"] == 1
        assert condition.status == "new"

    async def test_burst_below_10_never_trips(self, session):
        """Even with a peak of 0 (NULL), <= 10 in 15m never trips."""
        now = datetime.now(UTC)
        condition = await self._synced_condition(session, occurrences=1, severity="warn", now=now)
        await set_status(
            session, condition, "acknowledged", actor="chris", now=now,
            issue_url="https://github.com/test/1",
        )
        await session.commit()

        later = now + timedelta(hours=1)
        for _ in range(9):
            await _fire_backdated(
                session, severity="warn", age=timedelta(minutes=1), now=later,
            )
        await session.commit()
        result = await apply_lifecycle_rules(session, now=later + timedelta(minutes=1))
        await session.commit()
        assert result["escalated"] == 0
        assert condition.status == "acknowledged"


class TestSubjectReopen:
    """Reopens when >= 5 new distinct subjects appear since ack."""

    async def _synced_and_acked(self, session, *, now=None, n_subjects=3):
        now = now or datetime.now(UTC)
        await _fire_backdated(session, severity="warn", now=now)
        await session.commit()
        await sync_condition_stats(session, now=now)
        await session.commit()
        condition = await _the_condition(session)
        # Seed some subjects before ack.
        today = now.date()
        await record_subjects(
            session, condition.id,
            [("user", f"u{i}") for i in range(n_subjects)],
            today,
        )
        await session.commit()
        await set_status(
            session, condition, "acknowledged", actor="chris", now=now,
            issue_url="https://github.com/test/1",
        )
        await session.commit()
        return condition, now

    async def test_subjects_reopen_at_5_new_not_4(self, session):
        """4 new subjects since ack must not reopen; 5 must."""
        condition, now = await self._synced_and_acked(session, now=datetime.now(UTC), n_subjects=3)
        assert condition.acknowledged_subject_count == 3

        # Add 4 new subjects — should not reopen.
        later = now + timedelta(hours=1)
        today = later.date()
        await record_subjects(
            session, condition.id,
            [("user", f"new{i}") for i in range(4)],
            today,
        )
        await session.commit()
        result = await apply_lifecycle_rules(session, now=later)
        await session.commit()
        assert result["escalated"] == 0
        assert condition.status == "acknowledged"

        # Add 1 more (total 5 new) — SHOULD reopen.
        await record_subjects(
            session, condition.id,
            [("user", "new4")],
            today,
        )
        await session.commit()
        result2 = await apply_lifecycle_rules(session, now=later + timedelta(minutes=1))
        await session.commit()
        assert result2["escalated"] == 1
        assert condition.status == "new"
        assert "subjects" in (condition.notes or "")


class TestRegressionReopen:
    """Reopens when last_seen_release differs from acknowledged_release."""

    async def _synced_condition(self, session, *, severity="warn", now=None):
        now = now or datetime.now(UTC)
        await _fire_backdated(session, severity=severity, now=now)
        await session.commit()
        await sync_condition_stats(session, now=now)
        await session.commit()
        return await _the_condition(session)

    async def test_regression_reopens(self, session):
        """A condition whose last_seen_release is newer than
        acknowledged_release is a regression and must reopen."""
        now = datetime.now(UTC)
        condition = await self._synced_condition(session, now=now)
        condition.last_seen_release = "v1.0"
        await session.flush()
        await set_status(
            session, condition, "acknowledged", actor="chris", now=now,
            issue_url="https://github.com/test/1",
        )
        await session.commit()
        assert condition.acknowledged_release == "v1.0"

        # A newer release appears.
        condition.last_seen_release = "v2.0"
        await session.flush()
        await session.commit()

        result = await apply_lifecycle_rules(session, now=now + timedelta(hours=1))
        await session.commit()
        assert result["escalated"] == 1
        assert condition.status == "new"
        assert "regression" in (condition.notes or "").lower()

    async def test_null_release_skipped(self, session):
        """NULL on either side skips the regression rule."""
        now = datetime.now(UTC)
        condition = await self._synced_condition(session, now=now)
        await set_status(
            session, condition, "acknowledged", actor="chris", now=now,
            issue_url="https://github.com/test/1",
        )
        await session.commit()
        # Both NULL — should not reopen.
        assert condition.acknowledged_release is None
        assert condition.last_seen_release is None

        result = await apply_lifecycle_rules(session, now=now + timedelta(hours=1))
        await session.commit()
        assert result["escalated"] == 0
        assert condition.status == "acknowledged"


class TestRecordAndDistinctSubjects:
    """Tests for record_subjects and distinct_subjects helpers."""

    async def test_record_subjects_inserts_and_distinct_counts(self, session):
        """record_subjects inserts rows; distinct_subjects counts since a date."""
        # Create a condition first.
        await _fire_backdated(session)
        await session.commit()
        condition = await _the_condition(session)

        today = datetime.now(UTC).date()
        await record_subjects(
            session, condition.id,
            [("user", "u1"), ("user", "u2"), ("org", "o1")],
            today,
        )
        await session.commit()

        count = await distinct_subjects(session, condition.id, since=today)
        assert count == 3

    async def test_record_subjects_is_idempotent(self, session):
        """Duplicate inserts do not raise or double-count."""
        await _fire_backdated(session)
        await session.commit()
        condition = await _the_condition(session)

        today = datetime.now(UTC).date()
        await record_subjects(
            session, condition.id,
            [("user", "u1"), ("user", "u1")],
            today,
        )
        await session.commit()
        # Again — no error.
        await record_subjects(
            session, condition.id,
            [("user", "u1")],
            today,
        )
        await session.commit()

        count = await distinct_subjects(session, condition.id, since=today)
        assert count == 1


class TestSyncRelease:
    """sync_condition_stats folds first_seen_release/last_seen_release."""

    async def test_release_folded_from_occurrence_context(self, session):
        """If occurrences carry _release in context, it folds into the condition."""
        import json

        now = datetime.now(UTC)
        a1 = Alert(
            severity="error", category="cat", message="boom",
            context_json=json.dumps({"_release": "v1.0"}),
            created=now - timedelta(hours=2),
        )
        session.add(a1)
        await session.flush()
        # Create condition for it.
        from opsalert.store import fire_alert
        a2 = await fire_alert(
            session, severity="error", category="cat", message="boom",
            context={"_release": "v2.0"},
        )
        a2.created = now - timedelta(minutes=10)
        # Also link a1 to the same condition.
        a1.condition_id = a2.condition_id
        await session.flush()
        await session.commit()

        await sync_condition_stats(session, now=now)
        await session.commit()

        condition = await _the_condition(session)
        assert condition.first_seen_release == "v1.0"
        assert condition.last_seen_release == "v2.0"


class TestReopenedDeliveredImmediately:
    """#11: a reopened condition is delivered immediately, regardless of
    severity or disposition."""

    async def test_reopened_warn_digest_delivered_immediately(
        self, session, session_factory
    ):
        """A WARN condition with digest disposition that reopens must still
        deliver immediately — a recurrence of something thought fixed is not
        a digest item."""
        import opsalert
        from opsalert.delivery import deliver_alerts
        from opsalert.types import AlertMessage

        class _TrackingTransport(opsalert.Transport):
            def __init__(self):
                self.sent: list[AlertMessage] = []

            def send(self, message, *, to, from_addr, from_name):
                self.sent.append(message)
                return True

        transport = _TrackingTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=0,
        )

        # Create a warn condition with digest disposition, resolve it.
        await _fire_backdated(session, severity="warn")
        await session.commit()
        await sync_condition_stats(session)
        condition = await _the_condition(session)
        await set_disposition(session, condition, "digest")
        await set_status(
            session, condition, "resolved", actor="chris",
        )
        # Mark existing occurrences as notified.
        from sqlalchemy import update
        await session.execute(update(Alert).values(notified=True))
        await session.commit()

        # It recurs — fires again.
        await fire_alert(session, severity="warn", category="cat", message="boom")
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()

        assert stats["reopened"] == 1
        # The key assertion: it was sent as immediate, not swallowed by digest.
        assert stats["immediate_sent"] >= 1


class TestSubjectUpsertDialects:
    """Compile tests for subject_upsert_statement on both dialects."""

    def test_mysql_compiles_to_on_duplicate_key(self):
        from opsalert.lifecycle import subject_upsert_statement

        stmt = subject_upsert_statement("mysql", {
            "condition_id": 1,
            "subject_kind": "user",
            "subject_key": "u1",
            "day": datetime.now(UTC).date(),
        })
        from sqlalchemy.dialects import mysql
        compiled = str(stmt.compile(dialect=mysql.dialect()))
        assert "ON DUPLICATE KEY" in compiled

    def test_sqlite_compiles_to_on_conflict(self):
        from opsalert.lifecycle import subject_upsert_statement

        stmt = subject_upsert_statement("sqlite", {
            "condition_id": 1,
            "subject_kind": "user",
            "subject_key": "u1",
            "day": datetime.now(UTC).date(),
        })
        from sqlalchemy.dialects import sqlite
        compiled = str(stmt.compile(dialect=sqlite.dialect()))
        assert "ON CONFLICT" in compiled


class TestDeliveryStateUpsertDialects:
    """Compile tests for delivery_state_upsert_statement on both dialects."""

    def test_mysql_compiles_to_on_duplicate_key(self):
        from opsalert.delivery import delivery_state_upsert_statement

        stmt = delivery_state_upsert_statement("mysql", datetime.now(UTC))
        from sqlalchemy.dialects import mysql
        compiled = str(stmt.compile(dialect=mysql.dialect()))
        assert "ON DUPLICATE KEY" in compiled

    def test_sqlite_compiles_to_on_conflict(self):
        from opsalert.delivery import delivery_state_upsert_statement

        stmt = delivery_state_upsert_statement("sqlite", datetime.now(UTC))
        from sqlalchemy.dialects import sqlite
        compiled = str(stmt.compile(dialect=sqlite.dialect()))
        assert "ON CONFLICT" in compiled


class TestFoldReleaseNoRescan:
    """_fold_release only scans rows above the previous watermark."""

    async def test_second_sweep_no_new_rows_skips_release_query(self, session):
        """A second sweep with no new occurrences issues no release query.

        We verify by checking that first_seen_release and last_seen_release
        do not change after a second sweep when there are no new rows.
        """
        now = datetime.now(UTC)
        a = await fire_alert(
            session, severity="error", category="cat", message="boom",
            context={"_release": "v1.0"},
        )
        a.created = now - timedelta(minutes=10)
        await session.flush()
        await session.commit()

        await sync_condition_stats(session, now=now)
        await session.commit()

        condition = await _the_condition(session)
        assert condition.first_seen_release == "v1.0"
        assert condition.last_seen_release == "v1.0"
        watermark_after_first = condition.stats_synced_through

        # Second sweep — no new rows.
        await sync_condition_stats(session, now=now)
        await session.commit()

        await session.refresh(condition)
        # Watermark unchanged since no new rows above it.
        assert condition.stats_synced_through == watermark_after_first
        # Release values unchanged.
        assert condition.first_seen_release == "v1.0"
        assert condition.last_seen_release == "v1.0"

    async def test_new_release_folds_without_overwriting_first(self, session):
        """A new occurrence with a different release updates last_seen_release
        but not first_seen_release."""
        now = datetime.now(UTC)
        a = await fire_alert(
            session, severity="error", category="cat", message="boom",
            context={"_release": "v1.0"},
        )
        a.created = now - timedelta(minutes=20)
        await session.flush()
        await session.commit()

        await sync_condition_stats(session, now=now)
        await session.commit()

        condition = await _the_condition(session)
        assert condition.first_seen_release == "v1.0"
        assert condition.last_seen_release == "v1.0"

        # New occurrence with different release.
        b = await fire_alert(
            session, severity="error", category="cat", message="boom",
            context={"_release": "v2.0"},
        )
        b.created = now - timedelta(minutes=5)
        await session.flush()
        await session.commit()

        await sync_condition_stats(session, now=now)
        await session.commit()

        await session.refresh(condition)
        assert condition.first_seen_release == "v1.0"
        assert condition.last_seen_release == "v2.0"


class TestRegressionFullFlow:
    """Full-flow test: ack at release A, occurrences at release B,
    lifecycle reopens, attention shows is_regression=True."""

    async def test_ack_at_a_occurrences_at_b_attention_is_regression(
        self, session, session_factory
    ):
        import opsalert
        from opsalert.query import query_attention

        opsalert.configure(session_factory=session_factory)
        now = datetime.now(UTC)

        # Fire and sync — condition gets last_seen_release = "v1.0".
        a = await fire_alert(
            session, severity="error", category="cat", message="regressor",
            context={"_release": "v1.0"},
        )
        a.created = now - timedelta(minutes=20)
        await session.flush()
        await session.commit()
        await sync_condition_stats(session, now=now)
        await session.commit()

        condition = (
            await session.execute(
                select(AlertCondition).where(
                    AlertCondition.message_template == "regressor"
                )
            )
        ).scalar_one()
        assert condition.last_seen_release == "v1.0"

        # Ack — stamps acknowledged_release = "v1.0".
        await set_status(
            session, condition, "acknowledged", actor="chris", now=now,
            issue_url="https://github.com/test/1",
        )
        await session.commit()
        assert condition.acknowledged_release == "v1.0"

        # New occurrence at release B.
        later = now + timedelta(hours=1)
        b = await fire_alert(
            session, severity="error", category="cat", message="regressor",
            context={"_release": "v2.0"},
        )
        b.created = later - timedelta(minutes=5)
        await session.flush()
        await session.commit()
        await sync_condition_stats(session, now=later)
        await session.commit()

        await session.refresh(condition)
        assert condition.last_seen_release == "v2.0"

        # Lifecycle detects the regression and reopens.
        result = await apply_lifecycle_rules(session, now=later + timedelta(minutes=1))
        await session.commit()
        assert result["escalated"] == 1
        await session.refresh(condition)
        assert condition.status == "new"
        assert "regression" in (condition.notes or "").lower()
        # acknowledged_release is KEPT through reopen.
        assert condition.acknowledged_release == "v1.0"

        # Attention shows is_regression=True.
        attention = await query_attention(session)
        match = [c for c in attention["conditions"] if c["template"] == "regressor"]
        assert len(match) == 1
        assert match[0]["is_regression"] is True
