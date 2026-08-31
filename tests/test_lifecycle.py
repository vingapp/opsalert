"""Condition lifecycle — statistics, automatic rules, human transitions.

Covers A4 (acknowledged), A5 (auto-close/auto-stale/reopen) and the stats
half of A6 (the watermark must never run ahead of what was counted).
"""
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from opsalert.lifecycle import (
    AUTO_CLOSE_FLOOR,
    apply_lifecycle_rules,
    effective_disposition,
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
        """
        await _fire_backdated(session, message="boom")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        condition = await _the_condition(session)
        condition.stats_synced_through = 10_000  # far above any row we will make
        session.add(
            Alert(
                severity="error",
                category="cat",
                message="boom",
                created=datetime.now(UTC) - LONG_AGO,
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
        await set_status(session, condition, "acknowledged", actor="chris")
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
        await set_status(session, condition, "acknowledged", actor="chris")
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
        await set_status(session, condition.id, "acknowledged", actor="chris")
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
