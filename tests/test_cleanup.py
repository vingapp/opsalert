"""Tests for TTL cleanup."""
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

import opsalert
from opsalert.cleanup import cleanup_alerts
from opsalert.lifecycle import sync_condition_stats
from opsalert.model import Alert, AlertCondition
from opsalert.store import fire_alert


class TestCleanupAlerts:
    """Test age-based alert deletion."""

    async def test_deletes_old_alerts(self, session, session_factory):
        """Alerts older than max_age_days are deleted."""
        opsalert.configure(session_factory=session_factory, retention_max_age_days=30)

        # Old alert (45 days)
        old = Alert(
            severity="warn",
            category="cat",
            message="old",
            created=datetime.now(UTC) - timedelta(days=45),
        )
        # Recent alert (5 days)
        recent = Alert(
            severity="warn",
            category="cat",
            message="recent",
            created=datetime.now(UTC) - timedelta(days=5),
        )
        session.add_all([old, recent])
        await session.commit()

        result = await cleanup_alerts(session)
        await session.commit()

        assert result["deleted"] == 1

        remaining = (await session.execute(select(Alert))).scalars().all()
        assert len(remaining) == 1
        assert remaining[0].message == "recent"

    async def test_respects_max_age_setting(self, session, session_factory):
        """Uses the configured retention_max_age_days."""
        opsalert.configure(session_factory=session_factory, retention_max_age_days=7)

        # 10-day-old alert (should be deleted with 7-day retention)
        alert = Alert(
            severity="warn",
            category="cat",
            message="m",
            created=datetime.now(UTC) - timedelta(days=10),
        )
        session.add(alert)
        await session.commit()

        result = await cleanup_alerts(session)
        await session.commit()

        assert result["deleted"] == 1

    async def test_no_deletions_when_all_recent(self, session, session_factory):
        """Returns deleted=0 when all alerts are within retention window."""
        opsalert.configure(session_factory=session_factory, retention_max_age_days=90)

        alert = Alert(
            severity="warn",
            category="cat",
            message="m",
            created=datetime.now(UTC) - timedelta(days=30),
        )
        session.add(alert)
        await session.commit()

        result = await cleanup_alerts(session)
        assert result["deleted"] == 0

    async def test_empty_db(self, session, session_factory):
        """Returns deleted=0 when no alerts exist."""
        opsalert.configure(session_factory=session_factory)
        result = await cleanup_alerts(session)
        assert result["deleted"] == 0


class TestCleanupPreservesConditionHistory:
    """P3/A6 — pruning occurrences must not quietly rewrite history."""

    async def _counted_condition(self, session, *, age_days):
        """One condition with an old, already-counted occurrence."""
        alert = await fire_alert(session, severity="error", category="cat", message="boom")
        alert.created = datetime.now(UTC) - timedelta(days=age_days)
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()
        condition = (await session.execute(select(AlertCondition))).scalar_one()
        return condition, alert

    async def test_counters_survive_the_occurrences_they_counted(
        self, session, session_factory
    ):
        opsalert.configure(session_factory=session_factory, retention_max_age_days=30)
        condition, _ = await self._counted_condition(session, age_days=45)
        first_seen, last_seen = condition.first_seen, condition.last_seen

        result = await cleanup_alerts(session)
        await session.commit()

        assert result["deleted"] == 1
        assert (await session.execute(select(Alert))).scalars().all() == []
        assert condition.occurrence_count == 1
        assert condition.first_seen == first_seen
        assert condition.last_seen == last_seen
        # Conditions are never auto-deleted — the record of the problem stays.
        assert (await session.execute(select(AlertCondition))).scalar_one() is condition

    async def test_an_uncounted_occurrence_is_not_deleted_however_old(
        self, session, session_factory
    ):
        """The dangerous case: old enough to prune, never folded into a counter.

        Deleting it would decrement history silently — the count would simply
        be wrong, and nothing would ever say so. It waits for the sweeper.
        """
        opsalert.configure(session_factory=session_factory, retention_max_age_days=30)
        condition, old = await self._counted_condition(session, age_days=45)

        stranded = await fire_alert(
            session, severity="error", category="cat", message="boom"
        )
        stranded.created = datetime.now(UTC) - timedelta(days=44)
        await session.commit()
        assert stranded.id > condition.stats_synced_through

        result = await cleanup_alerts(session)
        await session.commit()

        assert result["deleted"] == 1
        remaining = (await session.execute(select(Alert))).scalars().all()
        assert [a.id for a in remaining] == [stranded.id]

    async def test_a_counted_occurrence_is_deleted_once_the_watermark_covers_it(
        self, session, session_factory
    ):
        opsalert.configure(session_factory=session_factory, retention_max_age_days=30)
        condition, _ = await self._counted_condition(session, age_days=45)
        stranded = await fire_alert(
            session, severity="error", category="cat", message="boom"
        )
        stranded.created = datetime.now(UTC) - timedelta(days=44)
        await session.commit()

        # The sweeper catches up, and only then may cleanup take it.
        await sync_condition_stats(session)
        await session.commit()
        assert condition.occurrence_count == 2

        result = await cleanup_alerts(session)
        await session.commit()
        assert result["deleted"] == 2  # both, now that both are counted
        assert (await session.execute(select(Alert))).scalars().all() == []
        assert condition.occurrence_count == 2

    async def test_an_orphan_is_deleted_on_age_alone(self, session, session_factory):
        """P2's bounded exception, stated: a NULL-condition row has no counter.

        Adoption is unbounded and runs every sweep, so reaching the retention
        edge unadopted means the maintenance sweeper was broken for the whole
        window — its own loud incident (F2), not a silent one here.
        """
        opsalert.configure(session_factory=session_factory, retention_max_age_days=30)
        session.add(
            Alert(
                severity="error",
                category="cat",
                message="orphan",
                created=datetime.now(UTC) - timedelta(days=45),
            )
        )
        await session.commit()

        result = await cleanup_alerts(session)
        await session.commit()
        assert result["deleted"] == 1


def _empty_condition(**overrides) -> AlertCondition:
    """A condition row as a degraded fire leaves it: committed, never attached."""
    values = dict(
        signature_key=overrides.pop("signature_key", "sig-" + str(id(overrides))),
        category="cat",
        message_template="boom {n}",
        status="new",
        severity="error",
        created=datetime.now(UTC) - timedelta(hours=2),
    )
    values.update(overrides)
    return AlertCondition(**values)


class TestReapEmptyConditions:
    """opsalert#2: the lying ×0 rows a degraded attachment leaves behind."""

    async def test_an_old_empty_condition_is_reaped(self, session, session_factory):
        opsalert.configure(session_factory=session_factory)
        session.add(_empty_condition(signature_key="s1"))
        await session.commit()

        result = await cleanup_alerts(session)
        await session.commit()

        assert result["conditions_reaped"] == 1
        assert (await session.execute(select(AlertCondition))).scalars().all() == []

    async def test_a_fresh_empty_condition_is_left_alone(self, session, session_factory):
        """It may belong to a fire that is still in flight."""
        opsalert.configure(session_factory=session_factory)
        session.add(
            _empty_condition(
                signature_key="s1", created=datetime.now(UTC) - timedelta(minutes=5)
            )
        )
        await session.commit()

        result = await cleanup_alerts(session)
        await session.commit()

        assert result["conditions_reaped"] == 0
        assert (await session.execute(select(AlertCondition))).scalar_one() is not None

    async def test_a_populated_condition_is_never_reaped(self, session, session_factory):
        opsalert.configure(session_factory=session_factory)
        alert = await fire_alert(session, severity="error", category="cat", message="boom")
        condition = (await session.execute(select(AlertCondition))).scalar_one()
        condition.created = datetime.now(UTC) - timedelta(hours=2)
        await session.commit()

        result = await cleanup_alerts(session)
        await session.commit()

        assert result["conditions_reaped"] == 0
        survivor = (await session.execute(select(AlertCondition))).scalar_one()
        assert survivor.id == alert.condition_id

    async def test_counters_outlive_pruned_occurrences_and_block_the_reap(
        self, session, session_factory
    ):
        """All its rows pruned, count > 0: history, not garbage."""
        opsalert.configure(session_factory=session_factory, retention_max_age_days=30)
        alert = await fire_alert(session, severity="error", category="cat", message="boom")
        alert.created = datetime.now(UTC) - timedelta(days=45)
        await session.commit()
        await sync_condition_stats(session)
        condition = (await session.execute(select(AlertCondition))).scalar_one()
        condition.created = datetime.now(UTC) - timedelta(days=45)
        await session.commit()
        assert condition.occurrence_count == 1

        result = await cleanup_alerts(session)
        await session.commit()

        assert result["deleted"] == 1  # the occurrence goes on retention
        assert result["conditions_reaped"] == 0  # the record of it stays
        assert (await session.execute(select(AlertCondition))).scalar_one() is condition

    async def test_a_human_touched_empty_condition_is_kept(self, session, session_factory):
        """Annotated or triaged rows are records even when empty."""
        opsalert.configure(session_factory=session_factory)
        session.add_all(
            [
                _empty_condition(signature_key="s1", status="acknowledged"),
                _empty_condition(signature_key="s2", notes="looking into it"),
                _empty_condition(signature_key="s3", issue_url="https://x/1"),
            ]
        )
        await session.commit()

        result = await cleanup_alerts(session)
        await session.commit()

        assert result["conditions_reaped"] == 0
        kept = (await session.execute(select(AlertCondition))).scalars().all()
        assert {c.signature_key for c in kept} == {"s1", "s2", "s3"}

    async def test_reap_age_is_configurable(self, session, session_factory):
        opsalert.configure(
            session_factory=session_factory, condition_empty_reap_minutes=10
        )
        session.add(
            _empty_condition(
                signature_key="s1", created=datetime.now(UTC) - timedelta(minutes=30)
            )
        )
        await session.commit()

        result = await cleanup_alerts(session)
        await session.commit()

        assert result["conditions_reaped"] == 1
