"""Tests for alert delivery — immediate, throttled, and digest."""
import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

import opsalert
from opsalert.delivery import deliver_alerts
from opsalert.model import Alert, AlertDeliveryState
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
        """Sends nothing when every condition in the category is inside the window.

        Throttle state is per CONDITION, but it gates the category EMAIL: this
        category's only member was emailed five minutes ago, so no mail goes
        out and the new occurrence waits (opsalert#5).
        """
        transport = _TrackingTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=60,
        )

        # An occurrence of this condition was emailed five minutes ago.
        emailed = await fire_alert(session, severity="error", category="cat", message="boom")
        emailed.notified = True
        emailed.created = datetime.now(UTC) - timedelta(minutes=5)

        # The same condition fires again.
        await fire_alert(session, severity="error", category="cat", message="boom")
        await session.commit()

        stats = await deliver_alerts(session)
        assert stats["immediate_throttled"] == 1
        assert stats["immediate_sent"] == 0

    async def test_new_condition_is_not_shadowed_by_a_throttled_sibling(
        self, session, session_factory
    ):
        """A never-emailed condition goes out even when a category-mate is throttled.

        A brand-new problem is never silenced by its category-mates' throttle.
        The throttled mate rides along on the email that was going out anyway
        and is marked notified with it (opsalert#5): no email was suppressed,
        so ``immediate_throttled`` — which counts suppressed emails — is 0,
        and the finer per-condition figure records the one that was inside
        its window.
        """
        transport = _TrackingTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=60,
        )

        noisy = await fire_alert(session, severity="error", category="cat", message="known boom")
        noisy.notified = True
        noisy.created = datetime.now(UTC) - timedelta(minutes=5)
        await fire_alert(session, severity="error", category="cat", message="known boom")
        await fire_alert(
            session, severity="error", category="cat", message="never seen before"
        )
        await session.commit()

        stats = await deliver_alerts(session)
        await session.commit()

        assert stats["immediate_sent"] == 1
        assert stats["immediate_throttled"] == 0
        assert stats["immediate_throttled_conditions"] == 1
        body = transport.sent[0].text_body
        assert "never seen before" in body
        assert "known boom" in body

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

        # Same condition, emailed two hours ago — outside the window.
        emailed = await fire_alert(session, severity="error", category="cat", message="boom")
        emailed.notified = True
        emailed.created = datetime.now(UTC) - timedelta(hours=2)

        await fire_alert(session, severity="error", category="cat", message="boom")
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


class TestDigestInterval:
    """#10: _deliver_digest sends only when enough time has passed."""

    async def test_digest_respects_interval_across_two_sweeps(
        self, session, session_factory
    ):
        """Two consecutive sweeps with a short interval: the second is suppressed
        because last_digest_sent_at is too recent."""
        transport = _TrackingTransport()
        opsalert.configure(
            session_factory=session_factory,
            transport=transport,
            delivery_to_email="ops@test.com",
            delivery_from_email="alert@test.com",
            delivery_throttle_minutes=0,
            delivery_digest_interval_minutes=60,
        )

        await fire_alert(session, severity="warn", category="cat", message="warning 1")
        await session.commit()

        stats1 = await deliver_alerts(session)
        await session.commit()
        assert stats1["digest_sent"] == 1

        # Immediately fire another warn — digest should NOT send again.
        await fire_alert(session, severity="warn", category="cat", message="warning 2")
        await session.commit()

        stats2 = await deliver_alerts(session)
        await session.commit()
        assert stats2["digest_sent"] == 0

        # Verify delivery state persisted.
        state = await session.get(AlertDeliveryState, 1)
        assert state is not None
        assert state.last_digest_sent_at is not None


class TestPayloadShape:
    """Alertmanager webhook payload on AlertMessage."""

    async def test_payload_shape_validated_against_literal_expected_dict(
        self, session, session_factory
    ):
        """The immediate delivery path constructs an Alertmanager-compatible
        payload dict on AlertMessage."""
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

        await deliver_alerts(session)
        await session.commit()

        assert len(transport.sent) == 1
        msg = transport.sent[0]
        assert msg.payload is not None
        payload = msg.payload
        assert payload["version"] == "4"
        assert payload["status"] == "firing"
        assert isinstance(payload["alerts"], list)
        assert len(payload["alerts"]) >= 1
        alert_item = payload["alerts"][0]
        # Labels per spec: alertname, severity, category
        labels = alert_item["labels"]
        assert "alertname" in labels
        assert labels["severity"] == "error"
        assert labels["category"] == "sendgrid"
        # Annotations per spec: summary, issue_url, emit_site, condition_id
        annotations = alert_item["annotations"]
        assert "summary" in annotations
        assert "issue_url" in annotations
        assert "emit_site" in annotations
        assert "condition_id" in annotations
        # startsAt = first_seen iso (not last_created)
        assert "startsAt" in alert_item
        # endsAt = resolved_at or ""
        assert alert_item["endsAt"] == ""
        # fingerprint = signature_key (not condition id)
        assert "fingerprint" in alert_item
        assert alert_item["fingerprint"] != ""
        # signature_key is a hex hash, not a numeric id
        assert not alert_item["fingerprint"].isdigit()

    async def test_webhook_posts_payload_as_is(self, session, session_factory):
        """WebhookTransport.send POSTs the payload dict directly when present."""
        import urllib.request

        captured = {}

        class _FakeOpener:
            def __init__(self, *_a, **_kw):
                pass

            def __enter__(self):
                class _Resp:
                    status = 200
                return _Resp()

            def __exit__(self, *_a):
                pass

        original_urlopen = urllib.request.urlopen

        def fake_urlopen(req, **kwargs):
            captured["data"] = json.loads(req.data)
            captured["url"] = req.full_url
            return _FakeOpener()

        urllib.request.urlopen = fake_urlopen
        try:
            from opsalert.transport import WebhookTransport
            transport = WebhookTransport("http://localhost:9093/api/v2/alerts")
            payload = {
                "version": "4",
                "status": "firing",
                "alerts": [{"labels": {"alertname": "test"}}],
            }
            msg = AlertMessage(
                subject="test",
                html_body="<p>test</p>",
                text_body="test",
                severity="error",
                category="cat",
                payload=payload,
            )
            result = transport.send(msg, to="ops@test.com", from_addr="a@b.com", from_name="test")
            assert result is True
            assert captured["data"] == payload
        finally:
            urllib.request.urlopen = original_urlopen
