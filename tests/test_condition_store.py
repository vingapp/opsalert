"""Fire-path condition resolution (A2).

The contract under test is the uncomfortable one: conditionization is
bookkeeping, and bookkeeping must never cost an alert. Every failure mode
here ends with the occurrence stored and the caller unharmed.
"""
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

import opsalert
from opsalert import store
from opsalert.lifecycle import _naive, sync_condition_stats
from opsalert.model import Alert, AlertCondition
from opsalert.signature import condition_signature, normalize_message
from opsalert.store import fire_alert, resolve_condition_id


def _drain_fires():
    """Wait for enqueued events to be written by the ingest writer thread."""
    from opsalert.ingest import flush

    flush(timeout=5.0)


async def _conditions(session):
    return (await session.execute(select(AlertCondition))).scalars().all()


class TestConditionResolution:
    async def test_fire_links_the_occurrence_to_a_condition(self, session):
        alert = await fire_alert(
            session, severity="error", category="import", message="Row 42 failed"
        )
        await session.commit()

        conditions = await _conditions(session)
        assert len(conditions) == 1
        assert alert.condition_id == conditions[0].id
        assert conditions[0].message_template == "Row <n> failed"
        assert conditions[0].status == "new"
        assert conditions[0].severity == "error"

    async def test_a_new_condition_is_born_with_its_first_seen(self, session):
        """No NULL window between creating the condition and the stats sweep."""
        alert = await fire_alert(
            session, severity="error", category="import", message="Row 42 failed"
        )
        alert.created = datetime.now(UTC) - timedelta(days=2)  # older than the row
        await session.commit()

        condition = (await _conditions(session))[0]
        stamped = condition.first_seen
        assert stamped is not None

        # The sweep still owns it afterwards, and can only move it EARLIER.
        await sync_condition_stats(session)
        await session.commit()
        await session.refresh(condition)
        assert _naive(condition.first_seen) == _naive(alert.created)
        assert _naive(condition.first_seen) < _naive(stamped)

    async def test_same_signature_fires_share_one_condition(self, session):
        for row in range(5):
            await fire_alert(
                session, severity="error", category="import", message=f"Row {row} failed"
            )
        await session.commit()

        conditions = await _conditions(session)
        assert len(conditions) == 1
        occurrences = (await session.execute(select(Alert))).scalars().all()
        assert len(occurrences) == 5
        assert {o.condition_id for o in occurrences} == {conditions[0].id}

    async def test_different_problems_get_different_conditions(self, session):
        await fire_alert(session, severity="error", category="import", message="Row 1 failed")
        await fire_alert(session, severity="error", category="import", message="Pool exhausted")
        await session.commit()

        assert len(await _conditions(session)) == 2

    async def test_environment_splits_conditions(self, session, session_factory):
        """A staging failure and a production failure are separate rows (P9)."""
        opsalert.configure(session_factory=session_factory, environment="staging")
        await fire_alert(session, severity="error", category="import", message="boom")
        await session.commit()

        opsalert.configure(session_factory=session_factory, environment="production")
        await fire_alert(session, severity="error", category="import", message="boom")
        await session.commit()

        conditions = await _conditions(session)
        assert {c.environment for c in conditions} == {"staging", "production"}

    async def test_upsert_is_idempotent_on_sqlite(self, session):
        """Two resolutions of one signature return the same id, never two rows.

        SQLite's ``ON CONFLICT DO UPDATE ... RETURNING id`` is what makes the
        duplicate path return the EXISTING id; ``DO NOTHING`` would return no
        row at all. (The MySQL ``LAST_INSERT_ID(id)`` branch is exercised
        against a real MySQL in vingapi — it cannot be tested here.)
        """
        values = {
            "signature_key": condition_signature("cat", None, None, "boom"),
            "category": "cat",
            "source": None,
            "environment": None,
            "message_template": "boom",
            "status": "new",
            "severity": "error",
        }
        first = await store._upsert_condition(session, dict(values))
        second = await store._upsert_condition(session, dict(values))
        await session.commit()

        assert first == second
        assert len(await _conditions(session)) == 1


class TestUpsertStatements:
    """W7 — the two dialect constructs, compiled and inspected.

    Neither near-miss is detectable at runtime here: MySQL without
    ``LAST_INSERT_ID(id)`` silently reports the WRONG id on the duplicate
    path, and SQLite ``DO NOTHING`` returns no row at all. Compiling the SQL
    is how the branch that no local test can execute still gets checked.
    """

    def _values(self):
        return {
            "signature_key": "k" * 64,
            "category": "cat",
            "source": None,
            "environment": "production",
            "message_template": "boom",
            "status": "new",
            "severity": "error",
        }

    def test_mysql_returns_the_existing_id_on_a_duplicate(self):
        from sqlalchemy.dialects import mysql

        sql = str(
            store.upsert_statement("mysql", self._values()).compile(dialect=mysql.dialect())
        )
        assert "ON DUPLICATE KEY UPDATE" in sql
        assert "LAST_INSERT_ID(id)" in sql

    def test_sqlite_updates_and_returns_rather_than_doing_nothing(self):
        from sqlalchemy.dialects import sqlite

        sql = str(
            store.upsert_statement("sqlite", self._values()).compile(dialect=sqlite.dialect())
        )
        assert "ON CONFLICT" in sql
        assert "DO UPDATE" in sql
        assert "DO NOTHING" not in sql
        assert "RETURNING" in sql


class TestIsolatedResolution:
    """P1's primary path: resolution in a session of its own.

    On a real deployment the condition is resolved in a short-lived session
    from ``cfg.session_factory``, so the row lock the upsert takes is held for
    two statements and never across the caller's work. These tests reach that
    branch by reporting a non-SQLite dialect — the in-memory SQLite engine is
    a single shared connection, which is exactly why the production branch is
    skipped for it (a "separate" session there would commit the caller's open
    transaction along with its own).
    """

    async def test_sqlite_takes_the_savepoint_fallback(self, session, session_factory):
        """The escape hatch is refused when it would not actually isolate."""
        opsalert.configure(session_factory=session_factory)
        assert store._dialect_name(session) == "sqlite"
        alert = await fire_alert(session, severity="error", category="c", message="m")
        await session.commit()
        assert alert.condition_id is not None

    async def test_condition_is_committed_independently_of_the_caller(
        self, session, session_factory, monkeypatch
    ):
        """The caller rolls back; the condition it resolved is still there.

        That is the isolation property in one assertion: the resolution's
        transaction is not the caller's transaction.
        """
        monkeypatch.setattr(store, "_dialect_name", lambda _s: "postgresql")
        opsalert.configure(session_factory=session_factory)

        await fire_alert(session, severity="error", category="c", message="boom")
        await session.rollback()

        conditions = await _conditions(session)
        assert len(conditions) == 1
        # The occurrence rode the caller's transaction and went with it.
        assert (await session.execute(select(Alert))).scalars().all() == []

    async def test_isolated_failure_degrades_to_a_null_condition(
        self, session, monkeypatch
    ):
        """A broken factory costs the grouping, not the alert (F1)."""

        def _broken_factory():
            raise RuntimeError("pool exhausted")

        monkeypatch.setattr(store, "_dialect_name", lambda _s: "postgresql")
        opsalert.configure(session_factory=_broken_factory)

        alert = await fire_alert(session, severity="error", category="c", message="boom")
        await session.commit()

        assert alert.id is not None
        assert alert.condition_id is None

    async def test_isolated_path_reuses_an_existing_condition(
        self, session, session_factory, monkeypatch
    ):
        monkeypatch.setattr(store, "_dialect_name", lambda _s: "postgresql")
        opsalert.configure(session_factory=session_factory)

        first = await fire_alert(session, severity="error", category="c", message="boom")
        second = await fire_alert(session, severity="error", category="c", message="boom")
        await session.commit()

        assert first.condition_id == second.condition_id
        assert len(await _conditions(session)) == 1


class TestStructuredParams:
    async def test_params_render_the_message_and_pin_the_identity(self, session):
        """Different values, one condition; the stored text keeps the values."""
        for stub in ("ChFICzP9VHlILNzd", "VHppTliH5Pr97ZJ9"):
            await fire_alert(
                session,
                severity="error",
                category="request_anomaly",
                message="PUT /api/view/shares/{stub}/ exceeded its budget",
                params={"stub": stub},
            )
        await session.commit()

        conditions = await _conditions(session)
        assert len(conditions) == 1
        assert conditions[0].message_template == (
            "PUT /api/view/shares/{stub}/ exceeded its budget"
        )
        messages = {
            a.message for a in (await session.execute(select(Alert))).scalars().all()
        }
        assert messages == {
            "PUT /api/view/shares/ChFICzP9VHlILNzd/ exceeded its budget",
            "PUT /api/view/shares/VHppTliH5Pr97ZJ9/ exceeded its budget",
        }

    async def test_a_missing_param_key_never_raises(self, session):
        """A bad emit site costs a placeholder, not the alert."""
        alert = await fire_alert(
            session,
            severity="error",
            category="request_anomaly",
            message="PUT {route} took {ms}ms",
            params={"route": "/api/x/"},
        )
        await session.commit()

        assert alert.message == "PUT /api/x/ took {ms}ms"
        assert alert.condition_id is not None

    async def test_params_identity_ignores_the_rendered_text(self, session):
        """The template is the identity — the normalizer never sees the render."""
        await fire_alert(
            session,
            severity="error",
            category="c",
            message="job {n} failed",
            params={"n": 7},
        )
        await session.commit()
        condition = (await _conditions(session))[0]
        assert condition.message_template == "job {n} failed"
        assert condition.message_template != normalize_message("job 7 failed")


class TestResolutionFailureIsSurvivable:
    async def test_occurrence_is_stored_with_a_null_condition(self, session, monkeypatch):
        """P2: resolution failing costs grouping, never the occurrence."""

        async def _boom(*args, **kwargs):
            raise RuntimeError("condition table on fire")

        monkeypatch.setattr(store, "_lookup_or_create", _boom)

        alert = await fire_alert(
            session, severity="critical", category="infra", message="DB pool exhausted"
        )
        await session.commit()

        assert alert.id is not None
        assert alert.condition_id is None
        assert alert.message == "DB pool exhausted"
        assert await _conditions(session) == []

    async def test_a_database_failure_leaves_the_callers_transaction_healthy(
        self, session, monkeypatch
    ):
        """P1: the SAVEPOINT absorbs the error; the caller commits normally.

        The induced failure is a failed FLUSH — a duplicate key — because that
        is the one that leaves a session in pending-rollback and kills every
        statement the caller makes afterwards. Remove the ``begin_nested()``
        and this test dies with a PendingRollbackError.
        """
        clashing = AlertCondition(
            signature_key="taken", category="c", message_template="t", severity="error"
        )
        session.add(clashing)
        await session.commit()

        earlier = Alert(severity="warn", category="other", message="work in progress")
        session.add(earlier)

        async def _duplicate_insert(session_, **kwargs):
            # A failed FLUSH, not a failed SELECT: this is the failure that
            # leaves a session in "pending rollback" and kills every later
            # statement the caller makes. Only the SAVEPOINT contains it.
            session_.add(
                AlertCondition(
                    signature_key="taken",
                    category="c",
                    message_template="t",
                    severity="error",
                )
            )
            await session_.flush()

        monkeypatch.setattr(store, "_lookup_or_create", _duplicate_insert)

        alert = await fire_alert(
            session, severity="error", category="infra", message="still alive?"
        )
        # The caller's transaction is intact: it commits, and BOTH its own
        # earlier work and the alert survive.
        await session.commit()

        assert alert.condition_id is None
        messages = {
            a.message for a in (await session.execute(select(Alert))).scalars().all()
        }
        assert messages == {"work in progress", "still alive?"}

    async def test_resolution_failure_never_reaches_the_caller(self, session, monkeypatch):
        async def _boom(*args, **kwargs):
            raise RuntimeError("nope")

        monkeypatch.setattr(store, "_lookup_or_create", _boom)
        # No pytest.raises: an exception escaping here would be the regression.
        assert (
            await resolve_condition_id(
                session,
                category="c",
                source=None,
                environment=None,
                template="t",
                severity="error",
            )
            is None
        )

    def test_dispatch_api_still_never_raises(self, tmp_path, monkeypatch):
        """The hard contract: opsalert.error() never raises, even if the
        ingest writer's condition resolution fails entirely."""
        from sqlalchemy import create_engine, text

        from opsalert import ingest
        from opsalert.model import OpsAlertBase

        db_path = tmp_path / "dispatch_never_raises.db"
        url = f"sqlite:///{db_path}"
        engine = create_engine(url)
        OpsAlertBase.metadata.create_all(engine)

        def _boom_sync(*args, **kwargs):
            raise RuntimeError("nope")

        monkeypatch.setattr(ingest, "_resolve_condition_sync", _boom_sync)
        opsalert.configure(
            session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no")),
            ingest_url=url,
        )

        opsalert.error("cat", message="boom")  # must not raise
        _drain_fires()

        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT message, condition_id FROM opsalert WHERE category='cat'")
            ).fetchone()
        assert row is not None
        assert row[0] == "boom"
        assert row[1] is None  # condition resolution failed
        engine.dispose()

    async def test_fire_alert_works_without_configure(self, session):
        """Storing must never depend on configuration state."""
        alert = await fire_alert(session, severity="warn", category="c", message="m")
        await session.commit()

        stored = (await session.execute(select(Alert))).scalar_one()
        assert stored.id == alert.id
        assert stored.severity == "warn"
        assert stored.category == "c"
        assert stored.message == "m"


@pytest.mark.parametrize("severity", ["warn", "error", "critical"])
async def test_condition_records_the_firing_severity(session, severity):
    await fire_alert(session, severity=severity, category="c", message="m")
    await session.commit()
    condition = (await session.execute(select(AlertCondition))).scalar_one()
    assert condition.severity == severity
    assert condition.latest_severity == severity
