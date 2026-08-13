"""Tests for alert delivery — immediate, throttled, and digest."""
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

import opsalert
from opsalert.delivery import deliver_alerts
from opsalert.model import Alert
from opsalert.store import fire_alert
from opsalert.types import AlertMessage


class _TrackingTransport(opsalert.Transport):
    """Transport that records all sent messages for test assertions."""

    def __init__(self):
        self.sent: list[AlertMessage] = []

    def send(self, message, *, to, from_addr, from_name):
        self.sent.append(message)
        return True


class _FailTransport(opsalert.Transport):
    """Transport that always fails."""

    def send(self, message, *, to, from_addr, from_name):
        return False


class TestDeliverImmediate:
    """Test immediate delivery for ERROR/CRITICAL alerts."""

    async def test_sends_for_error_alerts(self, session, session_factory):
        """ERROR alerts trigger immediate email delivery."""
        transport = _TrackingTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=0,
        )

        await fire_alert(session, severity="error", category="sendgrid", message="500 error")
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()

        assert stats["immediate_sent"] == 1
        assert len(transport.sent) == 1
        assert "sendgrid" in transport.sent[0].subject

    async def test_sends_for_critical_alerts(self, session, session_factory):
        """CRITICAL alerts trigger immediate delivery."""
        transport = _TrackingTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=0,
        )

        await fire_alert(session, severity="critical", category="infra", message="DB down")
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()

        assert stats["immediate_sent"] == 1
        assert "CRITICAL" in transport.sent[0].subject

    async def test_marks_alerts_as_notified(self, session, session_factory):
        """After delivery, alerts are marked notified=True."""
        transport = _TrackingTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=0,
        )

        await fire_alert(session, severity="error", category="cat", message="m")
        await session.commit()

        await deliver_alerts(session)
        await session.commit()

        result = await session.execute(select(Alert).where(Alert.category == "cat"))
        alert = result.scalar_one()
        assert alert.notified is True

    async def test_throttles_recently_notified(self, session, session_factory):
        """Skips category if notified within throttle window."""
        transport = _TrackingTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=60,
        )

        # Create an already-notified alert (recent)
        alert = Alert(
            severity="error",
            category="cat",
            message="old",
            notified=True,
            created=datetime.now(UTC) - timedelta(minutes=5),
        )
        session.add(alert)

        # Create a new unnotified alert
        await fire_alert(session, severity="error", category="cat", message="new")
        await session.commit()

        stats = await deliver_alerts(session)
        assert stats["immediate_throttled"] == 1
        assert stats["immediate_sent"] == 0

    async def test_does_not_throttle_old_notifications(self, session, session_factory):
        """Sends if last notification was outside the throttle window."""
        transport = _TrackingTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=60,
        )

        # Old notified alert (outside throttle window)
        alert = Alert(
            severity="error",
            category="cat",
            message="old",
            notified=True,
            created=datetime.now(UTC) - timedelta(hours=2),
        )
        session.add(alert)

        await fire_alert(session, severity="error", category="cat", message="new")
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()
        assert stats["immediate_sent"] == 1

    async def test_does_not_resend_notified(self, session, session_factory):
        """Already notified alerts don't trigger new emails."""
        transport = _TrackingTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=0,
        )

        # All already notified
        alert = Alert(severity="error", category="cat", message="m", notified=True)
        session.add(alert)
        await session.commit()

        stats = await deliver_alerts(session)
        assert stats["immediate_sent"] == 0
        assert len(transport.sent) == 0

    async def test_no_send_on_transport_failure(self, session, session_factory):
        """If transport fails, alerts stay un-notified."""
        opsalert.configure(
            session_factory=session_factory,
            transport=_FailTransport(),
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=0,
        )

        await fire_alert(session, severity="error", category="cat", message="m")
        await session.commit()

        stats = await deliver_alerts(session)
        assert stats["immediate_sent"] == 0

        # Alert should still be un-notified
        result = await session.execute(select(Alert).where(Alert.category == "cat"))
        alert = result.scalar_one()
        assert alert.notified is False


class TestDeliverDigest:
    """Test digest delivery for WARN alerts."""

    async def test_sends_digest_for_warns(self, session, session_factory):
        """WARN alerts are batched into a single digest email."""
        transport = _TrackingTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=0,
        )

        await fire_alert(session, severity="warn", category="validation", message="bad param")
        await fire_alert(session, severity="warn", category="validation", message="bad param")
        await fire_alert(session, severity="warn", category="sms", message="rate limit")
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()

        assert stats["digest_sent"] == 1
        assert stats["digest_count"] == 3

    async def test_digest_marks_notified(self, session, session_factory):
        """After digest, warn alerts are marked notified."""
        transport = _TrackingTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=0,
        )

        await fire_alert(session, severity="warn", category="cat", message="m")
        await session.commit()

        await deliver_alerts(session)
        await session.commit()

        result = await session.execute(select(Alert))
        alert = result.scalar_one()
        assert alert.notified is True

    async def test_no_digest_when_none_unnotified(self, session, session_factory):
        """No digest sent when all warns are already notified."""
        transport = _TrackingTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
        )

        alert = Alert(severity="warn", category="cat", message="m", notified=True)
        session.add(alert)
        await session.commit()

        stats = await deliver_alerts(session)
        assert stats["digest_sent"] == 0


class TestDeliverDisabled:
    """Test delivery when disabled."""

    async def test_disabled_returns_empty_stats(self, session, session_factory):
        """When delivery_enabled=False, returns zero stats."""
        opsalert.configure(
            session_factory=session_factory,
            transport=_TrackingTransport(),
            delivery_enabled=False,
        )

        await fire_alert(session, severity="error", category="cat", message="m")
        await session.commit()

        stats = await deliver_alerts(session)
        assert stats["immediate_sent"] == 0
        assert stats["digest_sent"] == 0


class TestDeliverNoTransport:
    """Test delivery when no transport is configured."""

    async def test_no_transport_returns_zero(self, session, session_factory):
        """When transport is None, nothing is sent."""
        opsalert.configure(session_factory=session_factory, transport=None)

        await fire_alert(session, severity="error", category="cat", message="m")
        await session.commit()

        stats = await deliver_alerts(session)
        assert stats["immediate_sent"] == 0
        assert stats["digest_sent"] == 0


class _ExplodeAfterFirstTransport(opsalert.Transport):
    """Delivers the first message, then blows up — simulates a mid-sweep
    failure AFTER at least one email is irrevocably out the door."""

    def __init__(self):
        self.sent: list[AlertMessage] = []

    def send(self, message, *, to, from_addr, from_name):
        if self.sent:
            raise RuntimeError("transport exploded mid-sweep")
        self.sent.append(message)
        return True


class TestSentMarkDurability:
    """A delivered notification's notified-mark must be unlosable.

    The marks used to ride the caller's single end-of-sweep commit; a failure
    anywhere later in the sweep rolled back marks for emails the recipient
    already had, and the next sweep re-sent them immediately (the throttle
    window is computed FROM notified rows). Delivery now commits each mark
    the moment its send succeeds.
    """

    async def test_mark_survives_failure_later_in_the_sweep(
        self, session, session_factory
    ):
        transport = _ExplodeAfterFirstTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=0,
        )

        await fire_alert(session, severity="error", category="cat_a", message="a")
        await fire_alert(session, severity="error", category="cat_b", message="b")
        await session.commit()

        with pytest.raises(RuntimeError):
            await deliver_alerts(session)
        # The caller's transaction dies with the exception — roll it back
        # WITHOUT committing, as the real sweeper's failure path would.
        await session.rollback()

        assert len(transport.sent) == 1
        delivered_category = transport.sent[0].category

        # Read through a FRESH session: the delivered category's mark must
        # have been committed before the sweep blew up.
        async with session_factory() as fresh:
            result = await fresh.execute(
                select(Alert).where(Alert.category == delivered_category)
            )
            for alert in result.scalars():
                assert alert.notified is True, (
                    f"notified mark for delivered category {delivered_category!r} "
                    "was lost — this alert will be re-emailed every sweep"
                )
