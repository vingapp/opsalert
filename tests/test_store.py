"""Tests for fire_alert store operation."""
import json

from sqlalchemy import select

from opsalert.model import Alert
from opsalert.store import CONTEXT_MAX_BYTES, fire_alert, serialize_context
from opsalert.types import AlertSeverity


class TestFireAlert:
    """Test the fire_alert store function directly."""

    async def test_creates_row(self, session):
        """fire_alert creates a single row with all fields set."""
        alert = await fire_alert(
            session,
            severity=AlertSeverity.ERROR,
            category="sendgrid_delivery",
            message="SendGrid returned 500",
            source="email",
            context={"status_code": 500, "mail_id": 123},
        )
        await session.commit()

        assert alert.id is not None
        assert alert.severity == "error"
        assert alert.category == "sendgrid_delivery"
        assert alert.message == "SendGrid returned 500"
        assert alert.source == "email"
        assert alert.notified is False
        assert alert.created is not None

        ctx = json.loads(alert.context_json)
        assert ctx["status_code"] == 500
        assert ctx["mail_id"] == 123

    async def test_none_context(self, session):
        """fire_alert with no context stores NULL context_json."""
        alert = await fire_alert(
            session,
            severity=AlertSeverity.WARN,
            category="test",
            message="no context",
        )
        await session.commit()

        assert alert.context_json is None

    async def test_none_source(self, session):
        """fire_alert with no source stores NULL source."""
        alert = await fire_alert(
            session,
            severity=AlertSeverity.WARN,
            category="test",
            message="no source",
        )
        await session.commit()

        assert alert.source is None

    async def test_each_call_creates_new_row(self, session):
        """No dedup — each call produces a distinct row."""
        ids = []
        for _ in range(5):
            alert = await fire_alert(
                session,
                severity=AlertSeverity.WARN,
                category="same",
                message="same",
            )
            ids.append(alert.id)
        await session.commit()

        assert len(set(ids)) == 5

    async def test_context_with_nested_data(self, session):
        """Context can contain nested dicts and lists."""
        context = {
            "errors": [{"field": "name", "msg": "required"}],
            "metadata": {"request_id": "abc-123"},
        }
        alert = await fire_alert(
            session,
            severity=AlertSeverity.ERROR,
            category="validation",
            message="Validation failed",
            context=context,
        )
        await session.commit()

        stored = json.loads(alert.context_json)
        assert stored["errors"][0]["field"] == "name"
        assert stored["metadata"]["request_id"] == "abc-123"

    async def test_long_message_stored(self, session):
        """Messages up to 500 chars are stored."""
        long_msg = "x" * 500
        alert = await fire_alert(
            session,
            severity=AlertSeverity.WARN,
            category="test",
            message=long_msg,
        )
        await session.commit()

        assert alert.message == long_msg

    async def test_severity_values(self, session):
        """All three severity values are accepted."""
        for sev in AlertSeverity:
            alert = await fire_alert(
                session,
                severity=sev,
                category="test",
                message=f"severity {sev}",
            )
            assert alert.severity == sev.value

    async def test_notified_defaults_false(self, session):
        """New alerts default to notified=False."""
        alert = await fire_alert(
            session,
            severity=AlertSeverity.ERROR,
            category="test",
            message="test",
        )
        await session.commit()

        # Re-query to verify DB-level default
        result = await session.execute(select(Alert).where(Alert.id == alert.id))
        loaded = result.scalar_one()
        assert loaded.notified is False


class TestContextCap:
    """``Alert.context_json`` is TEXT (65535 bytes). Oversized contexts are
    capped so a fat context costs detail, never the whole alert."""

    def test_small_context_is_untouched(self):
        context = {"status_code": 500, "mail_id": 123}
        assert serialize_context(context) == json.dumps(context)

    def test_empty_context_is_null(self):
        assert serialize_context(None) is None
        assert serialize_context({}) is None

    def test_oversized_value_is_truncated_and_marked(self):
        context = {
            "trace": "x" * (CONTEXT_MAX_BYTES + 10_000),
            "route": "/api/thing/",
        }
        stored = json.loads(serialize_context(context))

        assert len(json.dumps(stored).encode()) <= CONTEXT_MAX_BYTES
        assert stored["_truncated"] == ["trace"]
        assert stored["_original_bytes"] > CONTEXT_MAX_BYTES
        assert stored["route"] == "/api/thing/", "small values keep full fidelity"
        assert stored["trace"].startswith("xxx")

    def test_many_oversized_values_all_get_truncated(self):
        context = {f"blob{i}": "y" * 20_000 for i in range(10)}
        stored = json.loads(serialize_context(context))

        assert len(json.dumps(stored).encode()) <= CONTEXT_MAX_BYTES
        assert set(stored["_truncated"]) <= set(context)
        assert stored["_truncated"], "at least one key must be reported truncated"

    def test_structure_heavy_context_falls_back_to_key_sample(self):
        # Thousands of small keys: no single value can be shrunk enough, so the
        # shape is kept and the data dropped rather than raising. The key list
        # is itself sampled — dumping 20k key names would blow the column too.
        context = {f"k{i}": f"v{i}" for i in range(20_000)}
        stored = json.loads(serialize_context(context))

        assert len(json.dumps(stored).encode()) <= CONTEXT_MAX_BYTES
        assert stored["_dropped"] is True
        assert stored["_key_count"] == 20_000
        assert 0 < len(stored["_truncated"]) < 20_000, "key list must be sampled, not dumped"
        assert stored["_original_bytes"] > CONTEXT_MAX_BYTES

    def test_multibyte_truncation_stays_valid_utf8(self):
        context = {"note": "é" * (CONTEXT_MAX_BYTES + 5_000)}
        stored = serialize_context(context)

        json.loads(stored)  # must round-trip; a split UTF-8 sequence would break
        assert len(stored.encode()) <= CONTEXT_MAX_BYTES

    async def test_oversized_context_still_persists_the_alert(self, session):
        alert = await fire_alert(
            session,
            severity=AlertSeverity.ERROR,
            category="client_crash",
            message="Huge crash",
            context={"stack": "z" * (CONTEXT_MAX_BYTES + 50_000)},
        )
        await session.commit()

        result = await session.execute(select(Alert).where(Alert.id == alert.id))
        loaded = result.scalar_one()
        assert loaded.id is not None, "the alert must survive an oversized context"
        assert json.loads(loaded.context_json)["_truncated"] == ["stack"]
