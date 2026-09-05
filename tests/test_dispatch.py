"""Tests for the fire API -- warn/error/critical dispatch.

Updated for the ingest path: fires go through the bounded queue + writer
thread.  Tests that verify DB rows call ``opsalert.flush()`` before reading.
"""
import pytest
from sqlalchemy import create_engine, text

import opsalert
from opsalert.ingest import flush
from opsalert.model import OpsAlertBase


def _make_db(tmp_path, name="dispatch.db"):
    """Create a sqlite file DB with opsalert tables; return (url, engine)."""
    db_path = tmp_path / name
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    OpsAlertBase.metadata.create_all(engine)
    return url, engine


class TestFireAPI:
    """Test the public warn/error/critical functions."""

    def test_warn_creates_alert(self, tmp_path):
        """opsalert.warn() creates an alert row with WARN severity."""
        url, engine = _make_db(tmp_path)
        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )

        opsalert.warn(
            "test_category",
            message="test message",
            source="test_source",
            context={"key": "value"},
        )
        flush(timeout=5.0)

        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT severity, category, message, source, context_json FROM opsalert")
            ).fetchone()

        assert row is not None
        assert row[0] == "warn"
        assert row[1] == "test_category"
        assert row[2] == "test message"
        assert row[3] == "test_source"
        assert '"key"' in row[4]
        engine.dispose()

    def test_error_creates_alert(self, tmp_path):
        """opsalert.error() creates an alert with ERROR severity."""
        url, engine = _make_db(tmp_path)
        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )

        opsalert.error("test_category", message="error msg")
        flush(timeout=5.0)

        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT severity FROM opsalert")
            ).fetchone()

        assert row is not None
        assert row[0] == "error"
        engine.dispose()

    def test_critical_creates_alert(self, tmp_path):
        """opsalert.critical() creates an alert with CRITICAL severity."""
        url, engine = _make_db(tmp_path)
        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )

        opsalert.critical("infra", message="DB pool exhausted", context={"pool_size": 10})
        flush(timeout=5.0)

        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT severity, category FROM opsalert")
            ).fetchone()

        assert row is not None
        assert row[0] == "critical"
        assert row[1] == "infra"
        engine.dispose()

    def test_multiple_fires_create_separate_rows(self, tmp_path):
        """Each fire call creates a separate row -- no dedup at write time."""
        url, engine = _make_db(tmp_path)
        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )

        for i in range(3):
            opsalert.warn("cat", message=f"msg {i}")
        flush(timeout=5.0)

        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM opsalert")).scalar()

        assert count == 3
        engine.dispose()


class TestTestingMode:
    """Test that testing=True suppresses all alert fires."""

    def test_testing_mode_noop(self):
        """When testing=True, _fire_sync is a no-op."""
        opsalert.configure(testing=True)

        # This should not raise or create anything
        opsalert.warn("cat", message="should be suppressed")

    def test_testing_mode_no_rows(self, tmp_path):
        """Testing mode doesn't create any rows."""
        url, engine = _make_db(tmp_path)
        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
            testing=True,
        )

        opsalert.warn("cat", message="suppressed")
        flush(timeout=2.0)

        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM opsalert")).scalar()
        assert count == 0
        engine.dispose()


class TestConfigRequired:
    """Test that configure() must be called before use."""

    def test_fire_without_configure_is_noop(self):
        """Calling warn/error/critical without configure() silently no-ops."""
        # Should not raise -- dispatch functions never disrupt caller
        opsalert.warn("cat", message="test")
        opsalert.error("cat", message="test")
        opsalert.critical("cat", message="test")

    async def test_get_config_without_configure_raises(self):
        """get_config() raises RuntimeError if not configured."""
        with pytest.raises(RuntimeError, match="opsalert.configure"):
            opsalert.get_config()


class TestConfigTraceProvider:
    """Test that configure() accepts and stores trace_provider."""

    def test_configure_stores_trace_provider(self):
        def provider():
            return ("id", "origin")

        opsalert.configure(trace_provider=provider)

        cfg = opsalert.get_config()
        assert cfg.trace_provider is provider

    def test_configure_trace_provider_defaults_to_none(self):
        opsalert.configure()

        cfg = opsalert.get_config()
        assert cfg.trace_provider is None


class TestEnrichment:
    """Test that auto-enrichment adds debugging info."""

    def test_caller_enrichment(self, tmp_path):
        """Enriched context includes _caller with module:function:line."""
        url, engine = _make_db(tmp_path)
        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )

        opsalert.warn("cat", message="msg", context={"user_key": "user_val"})
        flush(timeout=5.0)

        import json

        with engine.connect() as conn:
            ctx_json = conn.execute(
                text("SELECT context_json FROM opsalert")
            ).scalar()

        ctx = json.loads(ctx_json)
        assert "user_key" in ctx
        assert "_caller" in ctx
        # _caller should be from THIS test module, not from dispatch internals
        assert "test_dispatch" in ctx["_caller"]
        engine.dispose()

    def test_exception_enrichment(self, tmp_path):
        """When called during exception handling, captures exc info."""
        url, engine = _make_db(tmp_path)
        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )

        try:
            raise ValueError("test boom")
        except ValueError:
            opsalert.error("cat", message="msg")
        flush(timeout=5.0)

        import json

        with engine.connect() as conn:
            ctx_json = conn.execute(
                text("SELECT context_json FROM opsalert")
            ).scalar()

        ctx = json.loads(ctx_json)
        assert ctx["_exc_type"] == "ValueError"
        assert "test boom" in ctx["_exc_message"]
        assert "_traceback" in ctx
        engine.dispose()

    def test_traceback_keeps_the_application_frames(self):
        """A truncated traceback must keep the frames that name OUR code."""
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

    def test_enrichment_preserves_caller_data(self, tmp_path):
        """Caller-provided context keys are preserved alongside enrichment."""
        url, engine = _make_db(tmp_path)
        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )

        opsalert.warn("cat", message="msg", context={"my_key": "my_val", "status_code": 500})
        flush(timeout=5.0)

        import json

        with engine.connect() as conn:
            ctx_json = conn.execute(
                text("SELECT context_json FROM opsalert")
            ).scalar()

        stored = json.loads(ctx_json)
        assert stored["my_key"] == "my_val"
        assert stored["status_code"] == 500
        assert "_caller" in stored
        engine.dispose()

    def test_enrichment_with_none_context(self):
        """Enrichment works when caller passes None context."""
        opsalert.configure()
        ctx = opsalert._dispatch.enrich_context(None)

        assert "_caller" in ctx
        assert isinstance(ctx, dict)

    def test_trace_provider_adds_trace_fields(self):
        """When trace_provider returns (id, origin), both appear in enriched context."""
        opsalert.configure(
            trace_provider=lambda: ("req-abc-123", "POST /api/alerts"),
        )

        ctx = opsalert._dispatch.enrich_context({"user_key": "val"})

        assert ctx["_trace_id"] == "req-abc-123"
        assert ctx["_trace_origin"] == "POST /api/alerts"
        assert ctx["user_key"] == "val"

    def test_trace_provider_none_values_omitted(self):
        """When trace_provider returns (None, None), no trace keys appear."""
        opsalert.configure(
            trace_provider=lambda: (None, None),
        )

        ctx = opsalert._dispatch.enrich_context(None)

        assert "_trace_id" not in ctx
        assert "_trace_origin" not in ctx
        assert "_caller" in ctx

    def test_trace_provider_not_configured(self):
        """When trace_provider is None (default), enrichment works without trace keys."""
        opsalert.configure()

        ctx = opsalert._dispatch.enrich_context(None)

        assert "_trace_id" not in ctx
        assert "_trace_origin" not in ctx
        assert "_caller" in ctx

    def test_trace_provider_exception_graceful(self):
        """When trace_provider raises, enrichment continues without trace keys."""

        def bad_provider():
            raise RuntimeError("context lost")

        opsalert.configure(
            trace_provider=bad_provider,
        )

        ctx = opsalert._dispatch.enrich_context({"important": "data"})

        assert "_trace_id" not in ctx
        assert "_trace_origin" not in ctx
        assert "_caller" in ctx
        assert ctx["important"] == "data"

    def test_trace_provider_partial_values(self):
        """When trace_provider returns only trace_id, only _trace_id appears."""
        opsalert.configure(
            trace_provider=lambda: ("req-xyz-789", None),
        )

        ctx = opsalert._dispatch.enrich_context(None)

        assert ctx["_trace_id"] == "req-xyz-789"
        assert "_trace_origin" not in ctx


class TestIdentityProvider:
    """Attribution enrichment: which account did this happen to?"""

    def test_identity_provider_adds_user_and_org(self):
        opsalert.configure(
            identity_provider=lambda: (42, 7),
        )

        ctx = opsalert._dispatch.enrich_context({"user_key": "val"})

        assert ctx["_user_id"] == 42
        assert ctx["_org_id"] == 7
        assert ctx["user_key"] == "val"

    def test_identity_provider_none_values_omitted(self):
        """An anonymous request carries no attribution keys at all."""
        opsalert.configure(
            identity_provider=lambda: (None, None),
        )

        ctx = opsalert._dispatch.enrich_context(None)

        assert "_user_id" not in ctx
        assert "_org_id" not in ctx
        assert "_caller" in ctx

    def test_identity_provider_partial_values(self):
        opsalert.configure(
            identity_provider=lambda: (42, None),
        )

        ctx = opsalert._dispatch.enrich_context(None)

        assert ctx["_user_id"] == 42
        assert "_org_id" not in ctx

    def test_identity_provider_not_configured(self):
        opsalert.configure()

        ctx = opsalert._dispatch.enrich_context(None)

        assert "_user_id" not in ctx
        assert "_org_id" not in ctx

    def test_identity_provider_exception_is_graceful(self):
        """A broken provider costs attribution, never the alert."""

        def bad_provider():
            raise RuntimeError("no request context here")

        opsalert.configure(
            identity_provider=bad_provider,
        )

        ctx = opsalert._dispatch.enrich_context({"important": "data"})

        assert "_user_id" not in ctx
        assert ctx["important"] == "data"
        assert "_caller" in ctx

    def test_a_broken_provider_still_stores_the_alert(self, tmp_path):
        def bad_provider():
            raise ValueError("boom")

        url, engine = _make_db(tmp_path)
        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
            identity_provider=bad_provider,
        )

        opsalert.error("cat", message="the alert that matters")
        flush(timeout=5.0)

        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT message FROM opsalert WHERE category='cat'")
            ).fetchone()
        assert row is not None
        assert row[0] == "the alert that matters"
        engine.dispose()


class TestFireFailureHandling:
    """Test that fire never raises, even on errors."""

    def test_fire_logs_on_bad_ingest_url(self):
        """If ingest_url is unreachable, fire still doesn't raise."""
        opsalert.configure(
            ingest_url="sqlite:////nonexistent/impossible/path.db",
            ingest_max_retry_s=0.1,
        )

        # Should not raise
        opsalert.error("cat", message="msg")
        flush(timeout=2.0)


class TestFireNeverTouchesHostSession:
    """Dispatch contract: session_factory is never called."""

    def test_fire_never_touches_host_session_factory(self, tmp_path):
        """Configure with a raising factory + real ingest_url -> row exists."""
        url, engine = _make_db(tmp_path)

        calls = []

        def raising_factory():
            calls.append(1)
            raise RuntimeError("should not be called")

        opsalert.configure(
            session_factory=raising_factory,
            ingest_url=url,
        )

        opsalert.warn("cat", message="should not touch factory")
        flush(timeout=5.0)

        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM opsalert")).scalar()
        assert count >= 1
        assert len(calls) == 0, "session_factory was called!"
        engine.dispose()


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
