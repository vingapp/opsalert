"""Delivery under the condition lifecycle (A3, A4/P5, P7, F3).

The hard case these tests exist for: delivery must decide correctly using
only what it can see at the moment it runs. It cannot assume the maintenance
sweep ran first, in the right order, or at all.
"""
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

import opsalert
from opsalert.delivery import _CONDITION_LIST_CAP, deliver_alerts
from opsalert.lifecycle import set_disposition, set_status, sync_condition_stats
from opsalert.model import Alert, AlertCondition
from opsalert.store import fire_alert
from opsalert.types import AlertMessage


class _TrackingTransport(opsalert.Transport):
    def __init__(self):
        self.sent: list[AlertMessage] = []

    def send(self, message, *, to, from_addr, from_name):
        self.sent.append(message)
        return True


def _configure(session_factory, transport, **kwargs):
    opsalert.configure(
        session_factory=session_factory,
        transport=transport,
        delivery_to_email="ops@test.com",
        delivery_from_email="alert@test.com",
        delivery_throttle_minutes=kwargs.pop("throttle", 0),
        **kwargs,
    )


async def _condition(session) -> AlertCondition:
    return (await session.execute(select(AlertCondition))).scalars().first()


async def _fire_old(session, **kwargs):
    """An occurrence old enough for the stats sweep to count."""
    alert = await fire_alert(session, **kwargs)
    alert.created = datetime.now(UTC) - timedelta(minutes=10)
    await session.flush()
    return alert


class TestReopenOnTheDeliveryPath:
    """A3/P4 — reopen is detected by delivery itself, before any gating."""

    async def test_a_collected_resolved_condition_still_emails_on_recurrence(
        self, session, session_factory
    ):
        """The worst case, run in the worst order: delivery BEFORE the sweep.

        The condition was resolved and parked on ``collect``. It fires again.
        If reopening were left to ``apply_lifecycle_rules``, this sweep would
        mark the recurrence notified under the collect rule and nobody would
        ever hear about it — the occurrence would be gone from the unnotified
        set by the time the lifecycle sweep noticed.
        """
        transport = _TrackingTransport()
        _configure(session_factory, transport)

        await _fire_old(session, severity="error", category="cat", message="boom")
        await session.commit()
        await sync_condition_stats(session)
        condition = await _condition(session)
        await set_status(session, condition, "resolved", actor="chris")
        await set_disposition(session, condition, "collect")
        await session.execute(  # the first episode was already delivered
            Alert.__table__.update().values(notified=True)
        )
        await session.commit()

        # It comes back — the same problem, so the same condition.
        recurrence = await fire_alert(
            session, severity="error", category="cat", message="boom"
        )
        await session.commit()

        stats = await deliver_alerts(session)  # NO lifecycle sweep first
        await session.commit()

        assert stats["reopened"] == 1
        assert condition.status == "new"
        assert condition.reopened_count == 1
        assert stats["immediate_sent"] == 1, "a recurrence was swallowed by its disposition"
        assert "cat" in transport.sent[0].subject

        await session.refresh(recurrence)
        assert recurrence.notified is True

    async def test_reopen_survives_a_later_failure_in_the_sweep(
        self, session, session_factory
    ):
        """The reopen is committed where it happens, not at the end."""

        class _Exploding(opsalert.Transport):
            def send(self, message, *, to, from_addr, from_name):
                raise RuntimeError("transport exploded")

        _configure(session_factory, _Exploding())

        await _fire_old(session, severity="error", category="cat", message="boom")
        await session.commit()
        await sync_condition_stats(session)
        condition = await _condition(session)
        await set_status(session, condition, "resolved", actor="chris")
        await session.execute(Alert.__table__.update().values(notified=True))
        await session.commit()
        condition_id = condition.id

        await fire_alert(session, severity="error", category="cat", message="boom")
        await session.commit()

        try:
            await deliver_alerts(session)
        except RuntimeError:
            pass
        await session.rollback()

        async with session_factory() as fresh:
            reloaded = (
                await fresh.execute(
                    select(AlertCondition).where(AlertCondition.id == condition_id)
                )
            ).scalar_one()
            assert reloaded.status == "new"
            assert reloaded.reopened_count == 1


class TestAcknowledged:
    """A4/P5 — acknowledged leaves the immediate line, keeps accruing."""

    async def test_acknowledged_gets_the_digest_at_most(self, session, session_factory):
        transport = _TrackingTransport()
        _configure(session_factory, transport)

        await _fire_old(session, severity="error", category="cat", message="boom")
        await session.commit()
        await sync_condition_stats(session)
        condition = await _condition(session)
        await set_status(session, condition, "acknowledged", actor="chris")
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()

        assert stats["immediate_sent"] == 0
        assert stats["digest_sent"] == 1
        assert transport.sent[0].category == "digest"

    async def test_occurrences_keep_accruing_while_acknowledged(
        self, session, session_factory
    ):
        transport = _TrackingTransport()
        _configure(session_factory, transport)

        await _fire_old(session, severity="error", category="cat", message="boom")
        await session.commit()
        await sync_condition_stats(session)
        condition = await _condition(session)
        await set_status(session, condition, "acknowledged", actor="chris")
        await session.commit()

        await _fire_old(session, severity="error", category="cat", message="boom")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        assert condition.occurrence_count == 2
        assert condition.status == "acknowledged"


class TestCollect:
    async def test_collected_conditions_are_marked_but_never_emailed(
        self, session, session_factory
    ):
        transport = _TrackingTransport()
        _configure(session_factory, transport)

        await _fire_old(session, severity="error", category="cat", message="boom")
        await session.commit()
        await sync_condition_stats(session)
        condition = await _condition(session)
        await set_disposition(session, condition, "collect")
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()

        assert stats["collected"] == 1
        assert stats["immediate_sent"] == 0
        assert transport.sent == []
        occurrence = (await session.execute(select(Alert))).scalar_one()
        assert occurrence.notified is True

    async def test_collect_works_without_a_transport(self, session, session_factory):
        """Recording a collected occurrence must not depend on email at all."""
        _configure(session_factory, None)

        await _fire_old(session, severity="error", category="cat", message="boom")
        await session.commit()
        await sync_condition_stats(session)
        await set_disposition(session, await _condition(session), "collect")
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()
        assert stats["collected"] == 1


class TestOneEmailPerCategory:
    """P7 — cadence unchanged, inclusion per condition."""

    async def test_many_conditions_in_one_category_send_one_email(
        self, session, session_factory
    ):
        transport = _TrackingTransport()
        _configure(session_factory, transport)

        kinds = ["pool exhausted", "disk full", "token rejected", "quota exceeded"]
        for kind in kinds:
            await fire_alert(session, severity="error", category="cat", message=kind)
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()

        conditions = (await session.execute(select(AlertCondition))).scalars().all()
        assert len(conditions) == len(kinds)
        assert stats["immediate_sent"] == 1
        assert len(transport.sent) == 1
        body = transport.sent[0].text_body
        for kind in kinds:
            assert kind in body

    async def test_the_condition_list_is_capped_and_says_how_many_are_missing(
        self, session, session_factory
    ):
        transport = _TrackingTransport()
        _configure(session_factory, transport)

        total = _CONDITION_LIST_CAP + 3
        for n in range(total):
            # Distinct WORDS: distinct numbers would normalize to one template.
            await fire_alert(
                session, severity="error", category="cat", message="failure " + "z" * n
            )
        await session.commit()

        await deliver_alerts(session)
        await session.commit()

        body = transport.sent[0].text_body
        assert f"and {total - _CONDITION_LIST_CAP} more" in body
        assert body.count("- #") == _CONDITION_LIST_CAP

    async def test_separate_categories_still_get_separate_emails(
        self, session, session_factory
    ):
        transport = _TrackingTransport()
        _configure(session_factory, transport)

        await fire_alert(session, severity="error", category="cat_a", message="a")
        await fire_alert(session, severity="error", category="cat_b", message="b")
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()
        assert stats["immediate_sent"] == 2
        assert {m.category for m in transport.sent} == {"cat_a", "cat_b"}


class TestOrphansStillDeliver:
    """F1 — a fire-time resolution failure costs grouping, nothing else."""

    async def test_condition_less_occurrences_use_the_legacy_category_path(
        self, session, session_factory
    ):
        transport = _TrackingTransport()
        _configure(session_factory, transport)

        session.add(Alert(severity="error", category="orphan_cat", message="no condition"))
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()

        assert stats["immediate_sent"] == 1
        assert "no condition" in transport.sent[0].subject
        orphan = (await session.execute(select(Alert))).scalar_one()
        assert orphan.notified is True

    async def test_orphan_warnings_join_the_digest(self, session, session_factory):
        transport = _TrackingTransport()
        _configure(session_factory, transport)

        session.add(Alert(severity="warn", category="orphan_cat", message="no condition"))
        await _fire_old(session, severity="warn", category="cat", message="a warning")
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()

        assert stats["digest_sent"] == 1
        assert stats["digest_count"] == 2
        assert (
            len(
                (await session.execute(select(Alert).where(Alert.notified.is_(False))))
                .scalars()
                .all()
            )
            == 0
        )


class TestCorruptConditionIsolation:
    """F3 — one unusable row must not take the sweep down with it."""

    async def test_a_broken_condition_is_skipped_and_the_rest_deliver(
        self, session, session_factory
    ):
        transport = _TrackingTransport()
        _configure(session_factory, transport)

        await fire_alert(session, severity="error", category="good", message="deliver me")
        broken_alert = await fire_alert(
            session, severity="error", category="bad", message="unreadable"
        )
        await session.commit()

        broken = await session.get(AlertCondition, broken_alert.condition_id)
        broken.status = "???"  # a value no transition can produce
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()

        assert stats["skipped"] == 1
        assert stats["immediate_sent"] == 1
        assert {m.category for m in transport.sent} == {"good"}
        # The skipped condition's occurrence is left unnotified — it was not
        # delivered, so it must not be marked as if it had been.
        still_waiting = (
            await session.execute(select(Alert).where(Alert.notified.is_(False)))
        ).scalars().all()
        assert "unreadable" in [a.message for a in still_waiting]
        # ...and the skip itself became an alert of its own, rather than a log
        # line nobody reads (F3).
        assert "Unusable alert condition row skipped during delivery" in [
            a.message for a in still_waiting
        ]
