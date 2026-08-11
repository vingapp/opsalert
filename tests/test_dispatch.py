"""Tests for the fire API — warn/error/critical dispatch."""
import pytest
from sqlalchemy import select

import opsalert
from opsalert._dispatch import _fire
from opsalert.model import Alert
from opsalert.types import AlertSeverity


class TestFireAPI:
    """Test the public warn/error/critical functions."""

    async def test_warn_creates_alert(self, session, session_factory):
        """opsalert.warn() creates an alert row with WARN severity."""
        opsalert.configure(session_factory=session_factory)

        await _fire(
            AlertSeverity.WARN, "test_category", "test message", "test_source", {"key": "value"}
        )

        result = await session.execute(select(Alert))
        alert = result.scalar_one()
        assert alert.severity == "warn"
        assert alert.category == "test_category"
        assert alert.message == "test message"
        assert alert.source == "test_source"
        assert '"key"' in alert.context_json

    async def test_error_creates_alert(self, session, session_factory):
        """opsalert.error() creates an alert with ERROR severity."""
        opsalert.configure(session_factory=session_factory)

        await _fire(AlertSeverity.ERROR, "test_category", "error msg", None, None)

        result = await session.execute(select(Alert))
        alert = result.scalar_one()
        assert alert.severity == "error"
        assert alert.context_json is None

    async def test_critical_creates_alert(self, session, session_factory):
        """opsalert.critical() creates an alert with CRITICAL severity."""
        opsalert.configure(session_factory=session_factory)

        await _fire(AlertSeverity.CRITICAL, "infra", "DB pool exhausted", None, {"pool_size": 10})

        result = await session.execute(select(Alert))
        alert = result.scalar_one()
        assert alert.severity == "critical"
        assert alert.category == "infra"

    async def test_multiple_fires_create_separate_rows(self, session, session_factory):
        """Each fire call creates a separate row — no dedup at write time."""
        opsalert.configure(session_factory=session_factory)

        for i in range(3):
            await _fire(AlertSeverity.WARN, "cat", f"msg {i}", None, None)

        result = await session.execute(select(Alert))
        alerts = result.scalars().all()
        assert len(alerts) == 3


class TestTestingMode:
    """Test that testing=True suppresses all alert fires."""

    def test_testing_mode_noop(self, session_factory):
        """When testing=True, _fire_sync is a no-op."""
        opsalert.configure(session_factory=session_factory, testing=True)

        # This should not raise or create anything
        opsalert.warn("cat", message="should be suppressed")

    async def test_testing_mode_no_rows(self, session, session_factory):
        """Testing mode doesn't create any rows."""
        opsalert.configure(session_factory=session_factory, testing=True)

        opsalert.warn("cat", message="suppressed")

        result = await session.execute(select(Alert))
        assert result.scalars().all() == []


class TestConfigRequired:
    """Test that configure() must be called before use."""

    def test_fire_without_configure_is_noop(self):
        """Calling warn/error/critical without configure() silently no-ops."""
        # Should not raise — dispatch functions never disrupt caller
        opsalert.warn("cat", message="test")
        opsalert.error("cat", message="test")
        opsalert.critical("cat", message="test")

    async def test_get_config_without_configure_raises(self):
        """get_config() raises RuntimeError if not configured."""
        with pytest.raises(RuntimeError, match="opsalert.configure"):
            opsalert.get_config()


class TestConfigTraceProvider:
    """Test that configure() accepts and stores trace_provider."""

    def test_configure_stores_trace_provider(self, session_factory):
        """configure(trace_provider=fn) stores the callable on config."""
        def provider():
            return ("id", "origin")

        opsalert.configure(session_factory=session_factory, trace_provider=provider)

        cfg = opsalert.get_config()
        assert cfg.trace_provider is provider

    def test_configure_trace_provider_defaults_to_none(self, session_factory):
        """Without trace_provider, config defaults to None."""
        opsalert.configure(session_factory=session_factory)

        cfg = opsalert.get_config()
        assert cfg.trace_provider is None


class TestEnrichment:
    """Test that auto-enrichment adds debugging info."""

    async def test_caller_enrichment(self, session, session_factory):
        """Enriched context includes _caller with module:function:line."""
        opsalert.configure(session_factory=session_factory)

        await _fire(
            AlertSeverity.WARN, "cat", "msg", None,
            opsalert._dispatch.enrich_context({"user_key": "user_val"})
        )

        result = await session.execute(select(Alert))
        alert = result.scalar_one()
        import json
        ctx = json.loads(alert.context_json)
        assert "user_key" in ctx
        assert "_caller" in ctx
        # _caller should be from THIS test module, not from dispatch internals
        assert "test_dispatch" in ctx["_caller"]

    async def test_exception_enrichment(self, session, session_factory):
        """When called during exception handling, captures exc info."""
        opsalert.configure(session_factory=session_factory)

        try:
            raise ValueError("test boom")
        except ValueError:
            ctx = opsalert._dispatch.enrich_context(None)

        await _fire(AlertSeverity.ERROR, "cat", "msg", None, ctx)

        result = await session.execute(select(Alert))
        alert = result.scalar_one()
        import json
        ctx = json.loads(alert.context_json)
        assert ctx["_exc_type"] == "ValueError"
        assert "test boom" in ctx["_exc_message"]
        assert "_traceback" in ctx

    def test_traceback_keeps_the_application_frames(self):
        """A truncated traceback must keep the frames that name OUR code.

        Regression: the traceback was capped with ``[-2000:]``, keeping the
        TAIL. A failure deep inside a library (SQLAlchemy, greenlet, the MySQL
        dialect) produces thousands of characters of library frames below the
        application frames, so the cap discarded every frame naming our code and
        stored only third-party internals.

        Real cost (vingapi staging 2026-08-03, alert ids 31115-31120): a
        MissingGreenlet on a lazy relationship load stored six tracebacks that
        were 100% site-packages, naming no endpoint, operation or presenter.
        """
        try:
            _application_frame_marker(_deep_library_stack)
        except ValueError:
            ctx = opsalert._dispatch.enrich_context(None)

        tb = ctx["_traceback"]
        assert len(tb) > 2000 or "_frame_00" in tb, "fixture stack was too short to truncate"
        assert "_application_frame_marker" in tb, (
            "the outermost (application) frames were truncated away"
        )
        assert "_frame_59" in tb, "the innermost (raising) frames were truncated away"

    def test_traceback_stays_within_its_budget(self):
        """Keeping both ends must not mean storing an unbounded traceback."""
        try:
            _application_frame_marker(_deep_library_stack)
        except ValueError:
            ctx = opsalert._dispatch.enrich_context(None)

        assert len(ctx["_traceback"]) <= 2200

    def test_truncated_traceback_says_it_was_truncated(self):
        """A reader must not mistake an elided middle for the whole stack."""
        try:
            _application_frame_marker(_deep_library_stack)
        except ValueError:
            ctx = opsalert._dispatch.enrich_context(None)

        assert "frames elided" in ctx["_traceback"]

    async def test_enrichment_preserves_caller_data(self, session, session_factory):
        """Caller-provided context keys are preserved alongside enrichment."""
        opsalert.configure(session_factory=session_factory)

        ctx = opsalert._dispatch.enrich_context({"my_key": "my_val", "status_code": 500})

        await _fire(AlertSeverity.WARN, "cat", "msg", None, ctx)

        result = await session.execute(select(Alert))
        alert = result.scalar_one()
        import json
        stored = json.loads(alert.context_json)
        assert stored["my_key"] == "my_val"
        assert stored["status_code"] == 500
        assert "_caller" in stored

    async def test_enrichment_with_none_context(self, session, session_factory):
        """Enrichment works when caller passes None context."""
        opsalert.configure(session_factory=session_factory)

        ctx = opsalert._dispatch.enrich_context(None)

        assert "_caller" in ctx
        assert isinstance(ctx, dict)

    async def test_trace_provider_adds_trace_fields(self, session, session_factory):
        """When trace_provider returns (id, origin), both appear in enriched context."""
        opsalert.configure(
            session_factory=session_factory,
            trace_provider=lambda: ("req-abc-123", "POST /api/alerts"),
        )

        ctx = opsalert._dispatch.enrich_context({"user_key": "val"})

        assert ctx["_trace_id"] == "req-abc-123"
        assert ctx["_trace_origin"] == "POST /api/alerts"
        # Caller-provided data preserved
        assert ctx["user_key"] == "val"

    async def test_trace_provider_none_values_omitted(self, session, session_factory):
        """When trace_provider returns (None, None), no trace keys appear."""
        opsalert.configure(
            session_factory=session_factory,
            trace_provider=lambda: (None, None),
        )

        ctx = opsalert._dispatch.enrich_context(None)

        assert "_trace_id" not in ctx
        assert "_trace_origin" not in ctx
        # Standard enrichment still works
        assert "_caller" in ctx

    async def test_trace_provider_not_configured(self, session, session_factory):
        """When trace_provider is None (default), enrichment works without trace keys."""
        opsalert.configure(session_factory=session_factory)

        ctx = opsalert._dispatch.enrich_context(None)

        assert "_trace_id" not in ctx
        assert "_trace_origin" not in ctx
        assert "_caller" in ctx

    async def test_trace_provider_exception_graceful(self, session, session_factory):
        """When trace_provider raises, enrichment continues without trace keys."""
        def bad_provider():
            raise RuntimeError("context lost")

        opsalert.configure(
            session_factory=session_factory,
            trace_provider=bad_provider,
        )

        ctx = opsalert._dispatch.enrich_context({"important": "data"})

        assert "_trace_id" not in ctx
        assert "_trace_origin" not in ctx
        # Enrichment still completed for everything else
        assert "_caller" in ctx
        assert ctx["important"] == "data"

    async def test_trace_provider_partial_values(self, session, session_factory):
        """When trace_provider returns only trace_id (origin is None), only _trace_id appears."""
        opsalert.configure(
            session_factory=session_factory,
            trace_provider=lambda: ("req-xyz-789", None),
        )

        ctx = opsalert._dispatch.enrich_context(None)

        assert ctx["_trace_id"] == "req-xyz-789"
        assert "_trace_origin" not in ctx


class TestFireFailureHandling:
    """Test that fire never raises, even on errors."""

    async def test_fire_logs_on_session_error(self, session_factory):
        """If the session factory fails, _fire logs but doesn't raise."""
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def bad_factory():
            raise ConnectionError("DB is down")
            yield  # pragma: no cover

        opsalert.configure(session_factory=bad_factory)

        # Should not raise
        await _fire(AlertSeverity.ERROR, "cat", "msg", None, None)


def _application_frame_marker(fn):
    """A uniquely-named frame standing in for an application-level frame."""
    return fn()


# A stack of DISTINCT frames. Recursion will not do: traceback.format_tb
# collapses repeated identical frames into "[Previous line repeated N times]",
# so a recursive fixture produces a short traceback and never exercises the cap.
_ns: dict = {}
exec(
    "\n".join(
        [f"def _frame_{i:02d}():\n    _frame_{i + 1:02d}()" for i in range(60)]
        + ["def _frame_60():", "    raise ValueError('boom at the bottom')"]
    ),
    _ns,
)
_deep_library_stack = _ns["_frame_00"]
