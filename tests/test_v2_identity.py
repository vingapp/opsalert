"""Red-first tests for O2 identity v2.

These tests exercise the v2 identity system — new columns on Alert and
AlertCondition, the v2 fingerprint, event_json, kind validation,
subject-per-event recording, and the lint helper.

Written BEFORE the implementation; they must fail on unmodified code.
"""
import json
import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, select, text

import opsalert
from opsalert.ingest import Event, flush, write_batch
from opsalert.model import AlertCondition, OpsAlertBase
from opsalert.store import fire_alert
from opsalert.types import AlertSeverity

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_db(tmp_path, name="identity.db"):
    db_path = tmp_path / name
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    OpsAlertBase.metadata.create_all(engine)
    return url, engine


def _count(engine, table):
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()


# ---------------------------------------------------------------------------
# Identity table tests — Alert and AlertCondition carry v2 columns
# ---------------------------------------------------------------------------


class TestIdentityTableColumns:
    """The new identity columns exist on Alert and AlertCondition.

    These tests fail on unmodified code because the columns do not exist yet.
    """

    def test_alert_has_kind_column(self, tmp_path):
        """Alert.kind is a mapped column."""
        url, engine = _make_db(tmp_path)
        with engine.connect() as conn:
            cols = [row[1] for row in conn.execute(text("PRAGMA table_info(opsalert)"))]
        assert "kind" in cols, "Alert table must have a 'kind' column"
        engine.dispose()

    def test_alert_has_fingerprint_version_column(self, tmp_path):
        url, engine = _make_db(tmp_path)
        with engine.connect() as conn:
            cols = [row[1] for row in conn.execute(text("PRAGMA table_info(opsalert)"))]
        assert "fingerprint_version" in cols
        engine.dispose()

    def test_alert_has_event_json_column(self, tmp_path):
        url, engine = _make_db(tmp_path)
        with engine.connect() as conn:
            cols = [row[1] for row in conn.execute(text("PRAGMA table_info(opsalert)"))]
        assert "event_json" in cols
        engine.dispose()

    def test_alert_has_emit_site_column(self, tmp_path):
        url, engine = _make_db(tmp_path)
        with engine.connect() as conn:
            cols = [row[1] for row in conn.execute(text("PRAGMA table_info(opsalert)"))]
        assert "emit_site" in cols
        engine.dispose()

    def test_alert_condition_has_kind_column(self, tmp_path):
        url, engine = _make_db(tmp_path)
        with engine.connect() as conn:
            cols = [row[1] for row in conn.execute(text("PRAGMA table_info(alert_condition)"))]
        assert "kind" in cols
        engine.dispose()

    def test_alert_condition_has_fingerprint_version_column(self, tmp_path):
        url, engine = _make_db(tmp_path)
        with engine.connect() as conn:
            cols = [row[1] for row in conn.execute(text("PRAGMA table_info(alert_condition)"))]
        assert "fingerprint_version" in cols
        engine.dispose()

    def test_alert_condition_has_resolution_url_column(self, tmp_path):
        url, engine = _make_db(tmp_path)
        with engine.connect() as conn:
            cols = [row[1] for row in conn.execute(text("PRAGMA table_info(alert_condition)"))]
        assert "resolution_url" in cols
        engine.dispose()

    def test_alert_condition_has_message_example_column(self, tmp_path):
        url, engine = _make_db(tmp_path)
        with engine.connect() as conn:
            cols = [row[1] for row in conn.execute(text("PRAGMA table_info(alert_condition)"))]
        assert "message_example" in cols
        engine.dispose()


# ---------------------------------------------------------------------------
# Subject-per-event rule: EVERY event records its subject, even sampled-out
# ---------------------------------------------------------------------------


class TestSubjectPerEvent:
    def test_write_batch_records_subject_for_sampled_out_event(self, tmp_path):
        """Sampling never hides a new user: even a sampled-out event records
        its subject via subject_upsert_statement.

        This test fails on unmodified code because write_batch does not
        currently record subjects at all.
        """
        url, engine = _make_db(tmp_path)

        sig_key = "a" * 64
        now = datetime.now(UTC)

        # One sampled-IN event with user_id
        ev_in = Event(
            event_id=uuid.uuid4().hex,
            ts=now,
            severity="warn",
            category="cat",
            message="msg",
            source=None,
            context={"_user_id": 42, "environment": "production"},
            params=None,
            template="msg",
            environment="production",
            signature_key=sig_key,
            sampled_in=True,
            kind="cat.test",
            fingerprint_version=2,
            fingerprint_json='["2","cat.test","production"]',
            emit_site="test:func",
            exception_class=None,
            trace_id=None,
            span_id=None,
            user_id=42,
            org_id=None,
            release=None,
            subjects=[("user", "42")],
        )

        # One sampled-OUT event with a different user_id
        ev_out = Event(
            event_id=uuid.uuid4().hex,
            ts=now,
            severity="warn",
            category="cat",
            message="msg",
            source=None,
            context={"_user_id": 99, "environment": "production"},
            params=None,
            template="msg",
            environment="production",
            signature_key=sig_key,
            sampled_in=False,
            kind="cat.test",
            fingerprint_version=2,
            fingerprint_json='["2","cat.test","production"]',
            emit_site="test:func",
            exception_class=None,
            trace_id=None,
            span_id=None,
            user_id=99,
            org_id=None,
            release=None,
            subjects=[("user", "99")],
        )

        with engine.connect() as conn:
            write_batch(conn, [ev_in, ev_out], {}, now)
            conn.commit()

        # The sampled-out event's subject must still be recorded
        with engine.connect() as conn:
            subject_count = conn.execute(
                text("SELECT COUNT(*) FROM alert_condition_subject")
            ).scalar()

        assert subject_count >= 2, (
            f"expected subjects for BOTH events (including sampled-out), got {subject_count}"
        )
        engine.dispose()


# ---------------------------------------------------------------------------
# event_json shape
# ---------------------------------------------------------------------------


class TestEventJsonShape:
    def test_event_json_shape(self, tmp_path):
        """event_json on Alert follows the Sentry event shape."""
        url, engine = _make_db(tmp_path)

        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )
        opsalert.error(
            "test_cat",
            message="test message",
            kind="test_cat.failure",
            context={"custom_key": "custom_val"},
        )
        flush(timeout=5.0)

        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT event_json FROM opsalert LIMIT 1")
            ).fetchone()

        assert row is not None, "expected an alert row"
        assert row[0] is not None, "event_json must not be NULL"
        event = json.loads(row[0])

        # Sentry-shaped keys
        assert "event_id" in event
        assert "timestamp" in event
        assert event["level"] == "error"
        assert event["logger"] == "test_cat"
        assert event["platform"] == "python"
        assert "fingerprint" in event
        assert "tags" in event
        assert event["tags"]["kind"] == "test_cat.failure"
        assert event["message"] == "test message"

        engine.dispose()


# ---------------------------------------------------------------------------
# kind validation
# ---------------------------------------------------------------------------


class TestKindValidation:
    def test_kind_invalid_raises_only_in_testing(self):
        """An invalid kind raises ValueError ONLY under cfg.testing."""
        opsalert.configure(testing=True)
        with pytest.raises(ValueError, match="kind"):
            opsalert.warn("cat", message="msg", kind="INVALID")

    def test_kind_valid_dotted_lowercase(self, tmp_path):
        """A valid dotted kind does not raise."""
        url, engine = _make_db(tmp_path)
        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )
        # Should not raise
        opsalert.warn("cat", message="msg", kind="cat.some_event")
        flush(timeout=5.0)
        engine.dispose()


# ---------------------------------------------------------------------------
# Legacy kind fallback
# ---------------------------------------------------------------------------


class TestLegacyKindFallback:
    def test_legacy_kind_fallback_never_coarser_than_v1(self, tmp_path):
        """Two messages that v1 split remain split under legacy v2 fallback.

        kind=None means legacy fallback: kind = f"{category}.legacy" and the
        message template joins the fingerprint. This must never merge messages
        that v1 would have kept separate.
        """
        from opsalert.signature import condition_signature, event_signature, normalize_message

        msg_a = "Task failure: adp_import"
        msg_b = "Task failure: sendgrid_bounce"

        # v1 keeps them separate
        tmpl_a = normalize_message(msg_a)
        tmpl_b = normalize_message(msg_b)
        v1_a = condition_signature("task_failure", None, "production", tmpl_a)
        v1_b = condition_signature("task_failure", None, "production", tmpl_b)
        assert v1_a != v1_b, "v1 must keep these separate"

        # v2 legacy fallback must also keep them separate
        v2_a = event_signature(
            kind="task_failure.legacy",
            environment="production",
            exception_chain=[],
            origin_frame="",
            template=tmpl_a,
        )
        v2_b = event_signature(
            kind="task_failure.legacy",
            environment="production",
            exception_chain=[],
            origin_frame="",
            template=tmpl_b,
        )
        assert v2_a != v2_b, "legacy v2 fallback must not merge messages that v1 split"


# ---------------------------------------------------------------------------
# Admin fire_alert uses v2 identity
# ---------------------------------------------------------------------------


class TestAdminFireAlertV2:
    async def test_admin_fire_alert_uses_v2_identity(self, session, session_factory):
        """fire_alert(session, ...) with kind= produces v2 identity."""
        opsalert.configure(session_factory=session_factory)

        alert = await fire_alert(
            session,
            severity=AlertSeverity.ERROR,
            category="admin_cat",
            message="admin fire",
            kind="admin_cat.test_fire",
        )
        await session.commit()

        # The condition should carry v2 identity
        condition = (
            await session.execute(
                select(AlertCondition).where(AlertCondition.id == alert.condition_id)
            )
        ).scalar_one()

        assert condition.kind == "admin_cat.test_fire"
        assert condition.fingerprint_version == 2


# ---------------------------------------------------------------------------
# Condition search matches kind
# ---------------------------------------------------------------------------


class TestConditionSearchMatchesKind:
    async def test_condition_search_matches_kind(self, session, session_factory):
        """Condition list/attention dicts expose kind; search matches it.

        For v2 conditions, message_template = kind, so a search for the kind
        string matches via the existing message_template search.
        """
        from opsalert.query import query_conditions

        opsalert.configure(session_factory=session_factory)

        await fire_alert(
            session,
            severity=AlertSeverity.ERROR,
            category="searchable",
            message="something happened",
            kind="searchable.db_timeout",
        )
        await session.commit()

        from opsalert.lifecycle import sync_condition_stats
        await sync_condition_stats(session)
        await session.commit()

        items, total, _ = await query_conditions(
            session, search="db_timeout", environment=None,
        )
        assert total >= 1
        match = [c for c in items if c.get("kind") == "searchable.db_timeout"]
        assert len(match) >= 1


# ---------------------------------------------------------------------------
# Fingerprint v2 tests
# ---------------------------------------------------------------------------


class TestFingerprintV2:
    def test_versioned_key_differs_from_v1(self):
        """v2 fingerprint for the same inputs must differ from v1."""
        from opsalert.signature import condition_signature, event_signature

        v1 = condition_signature("cat", None, "production", "boom")
        v2 = event_signature(
            kind="cat.boom",
            environment="production",
            exception_chain=[],
            origin_frame="",
        )
        assert v1 != v2

    def test_ignores_message_wording(self):
        """Two different messages with the same kind produce the same key."""
        from opsalert.signature import event_signature

        key_a = event_signature(
            kind="cat.failure",
            environment="production",
            exception_chain=[],
            origin_frame="",
        )
        key_b = event_signature(
            kind="cat.failure",
            environment="production",
            exception_chain=[],
            origin_frame="",
        )
        assert key_a == key_b

    def test_splits_on_exception_class(self):
        """Different exception classes produce different keys."""
        from opsalert.signature import event_signature

        key_a = event_signature(
            kind="cat.failure",
            environment="production",
            exception_chain=["ValueError"],
            origin_frame="",
        )
        key_b = event_signature(
            kind="cat.failure",
            environment="production",
            exception_chain=["TypeError"],
            origin_frame="",
        )
        assert key_a != key_b

    def test_splits_on_origin_frame(self):
        """Different origin frames produce different keys."""
        from opsalert.signature import event_signature

        key_a = event_signature(
            kind="cat.failure",
            environment="production",
            exception_chain=[],
            origin_frame="src.api:handle_request",
        )
        key_b = event_signature(
            kind="cat.failure",
            environment="production",
            exception_chain=[],
            origin_frame="src.tasks:process_queue",
        )
        assert key_a != key_b

    def test_ignores_line_numbers(self):
        """Origin frame is module:function, no lineno — a helper refactor
        that moves the raise inside the same function does not fork."""
        from opsalert.signature import event_signature

        # Origin frame has no line number by construction
        key = event_signature(
            kind="cat.failure",
            environment="production",
            exception_chain=[],
            origin_frame="src.api:handle_request",
        )
        # Same module:function, different (implicit) line → same key
        assert key == event_signature(
            kind="cat.failure",
            environment="production",
            exception_chain=[],
            origin_frame="src.api:handle_request",
        )

    def test_errno_splits_dbapi_errors(self):
        """DBAPI errors with different errno split."""
        from opsalert.signature import event_signature

        key_1213 = event_signature(
            kind="cat.db",
            environment="production",
            exception_chain=["OperationalError:1213"],
            origin_frame="",
        )
        key_1205 = event_signature(
            kind="cat.db",
            environment="production",
            exception_chain=["OperationalError:1205"],
            origin_frame="",
        )
        assert key_1213 != key_1205

    def test_never_raises_on_broken_exc(self):
        """A broken exc object with a raising __str__ still yields a key."""
        from opsalert.signature import build_exception_chain, extract_origin_frame

        class _BadError(Exception):
            def __str__(self):
                raise RuntimeError("broken __str__")

        try:
            raise _BadError("boom")
        except _BadError as e:
            chain = build_exception_chain(e)
            origin = extract_origin_frame(e, in_app_prefixes=())
            # Must not raise
            assert isinstance(chain, list)
            assert isinstance(origin, str)

    def test_exc_beats_sys_exc_info(self):
        """An explicit exc= beats sys.exc_info()."""
        from opsalert.signature import build_exception_chain

        inner = ValueError("inner")
        outer = TypeError("outer")

        try:
            raise outer
        except TypeError:
            # sys.exc_info() is TypeError, but exc= is ValueError
            chain = build_exception_chain(inner)
            assert chain[0] == "ValueError"

    def test_stacklevel_skips_wrappers(self):
        """stacklevel=2 skips one wrapper frame for emit_site."""
        from opsalert._enrichment import compute_emit_site

        def wrapper():
            return compute_emit_site(stacklevel=2)

        site = wrapper()
        # Should point to THIS function, not wrapper
        assert "test_stacklevel_skips_wrappers" in site

    def test_origin_frame_uses_real_module_name(self, tmp_path):
        """origin_frame must use __name__ from the frame, not guess from path.

        A temp module under a directory named src/ whose __name__ is
        'tmpmod.thing' must yield 'tmpmod.thing:do_raise', not a path-derived
        module name.
        """
        import importlib
        import importlib.util
        import sys

        from opsalert.signature import extract_origin_frame

        # Create a temp module file under src/ — the path looks like
        # src/thing.py but __name__ will be 'tmpmod.thing'.
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        mod_file = src_dir / "thing.py"
        mod_file.write_text(
            "def do_raise():\n"
            "    raise ValueError('boom from tmpmod.thing')\n"
        )

        spec = importlib.util.spec_from_file_location("tmpmod.thing", str(mod_file))
        mod = importlib.util.module_from_spec(spec)
        sys.modules["tmpmod.thing"] = mod
        try:
            spec.loader.exec_module(mod)
            try:
                mod.do_raise()
            except ValueError as e:
                origin = extract_origin_frame(e, in_app_prefixes=("tmpmod.",))
                assert origin == "tmpmod.thing:do_raise", (
                    f"expected 'tmpmod.thing:do_raise', got {origin!r}"
                )
        finally:
            sys.modules.pop("tmpmod.thing", None)


# ---------------------------------------------------------------------------
# fire_alert(session, kind=None) produces v2 identity
# ---------------------------------------------------------------------------


class TestAdminFireAlertV2LegacyFallback:
    async def test_fire_alert_kind_none_produces_v2_identity(self, session, session_factory):
        """fire_alert(session, kind=None) must create a v2 condition with
        kind='<category>.legacy' and fingerprint_version=2, the same legacy
        fallback as _dispatch."""
        opsalert.configure(session_factory=session_factory)

        alert = await fire_alert(
            session,
            severity=AlertSeverity.ERROR,
            category="admin_cat",
            message="admin fire with no kind",
        )
        await session.commit()

        assert alert.condition_id is not None
        condition = (
            await session.execute(
                select(AlertCondition).where(AlertCondition.id == alert.condition_id)
            )
        ).scalar_one()

        assert condition.fingerprint_version == 2
        assert condition.kind == "admin_cat.legacy"


# ---------------------------------------------------------------------------
# Lint helper
# ---------------------------------------------------------------------------


class TestLintHelper:
    def test_scan_fire_sites_detects_missing_kind(self, tmp_path):
        """scan_fire_sites finds calls without a static kind= string."""
        from opsalert.lint import scan_fire_sites

        bad_file = tmp_path / "bad.py"
        bad_file.write_text(
            "import opsalert\n"
            "opsalert.warn('cat', message='msg')\n"
        )

        findings = scan_fire_sites([str(bad_file)], in_app_prefix="src.")
        assert len(findings) >= 1
        assert any("kind" in f.message.lower() for f in findings)

    def test_scan_fire_sites_accepts_valid_kind(self, tmp_path):
        """A call with a valid static kind= string produces no finding."""
        from opsalert.lint import scan_fire_sites

        good_file = tmp_path / "good.py"
        good_file.write_text(
            "import opsalert\n"
            "opsalert.warn('cat', message='msg', kind='cat.thing')\n"
        )

        findings = scan_fire_sites([str(good_file)], in_app_prefix="src.")
        assert len(findings) == 0

    def test_scan_fire_sites_detects_invalid_kind(self, tmp_path):
        """A call with an invalid kind string produces a finding."""
        from opsalert.lint import scan_fire_sites

        bad_file = tmp_path / "invalid_kind.py"
        bad_file.write_text(
            "import opsalert\n"
            "opsalert.error('cat', message='msg', kind='INVALID')\n"
        )

        findings = scan_fire_sites([str(bad_file)], in_app_prefix="src.")
        assert len(findings) >= 1


# ---------------------------------------------------------------------------
# Trace provider 3-tuple support
# ---------------------------------------------------------------------------


class TestTraceProvider3Tuple:
    """The trace_provider contract accepts 2-tuple or 3-tuple."""

    def test_2tuple_unchanged(self):
        """A 2-tuple (trace_id, trace_origin) behaves exactly as before."""
        opsalert.configure(
            trace_provider=lambda: ("req-abc-123", "POST /api/alerts"),
        )

        from opsalert._enrichment import enrich_context
        ctx = enrich_context({"user_key": "val"})

        assert ctx["_trace_id"] == "req-abc-123"
        assert ctx["_trace_origin"] == "POST /api/alerts"
        assert "_span_id" not in ctx

    def test_3tuple_stamps_span_id(self):
        """A 3-tuple (trace_id, trace_origin, span_id) stamps _span_id."""
        opsalert.configure(
            trace_provider=lambda: ("req-abc-123", "POST /api/alerts", "abcd1234"),
        )

        from opsalert._enrichment import enrich_context
        ctx = enrich_context(None)

        assert ctx["_trace_id"] == "req-abc-123"
        assert ctx["_trace_origin"] == "POST /api/alerts"
        assert ctx["_span_id"] == "abcd1234"
