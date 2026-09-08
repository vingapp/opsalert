"""Tests for ingest race-condition fallback paths.

The upsert in _resolve_condition_sync catches IntegrityError (insert race)
and falls back to SELECT. Any other exception must propagate.
"""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError, OperationalError

from opsalert.ingest import Event, _resolve_condition_sync
from opsalert.model import AlertCondition, OpsAlertBase
from opsalert.signature import condition_signature


@pytest.fixture()
def sync_engine():
    """Synchronous in-memory SQLite engine with tables."""
    engine = create_engine("sqlite://", echo=False)
    OpsAlertBase.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture()
def sync_conn(sync_engine):
    """Synchronous connection with a transaction."""
    with sync_engine.connect() as conn:
        yield conn


def _make_event(
    category: str = "test.cat",
    message: str = "boom",
    source: str = "test",
) -> Event:
    """Build a minimal Event with a computed signature_key."""
    template = message
    sig = condition_signature(
        category=category,
        source=source,
        environment=None,
        template=template,
    )
    return Event(
        event_id="evt-test-1",
        ts=datetime.now(UTC),
        severity="error",
        category=category,
        message=message,
        source=source,
        context=None,
        params=None,
        template=template,
        environment=None,
        signature_key=sig,
    )


class TestResolveConditionSyncIntegrityFallback:
    """_resolve_condition_sync catches IntegrityError and falls back to SELECT."""

    def test_integrity_error_returns_existing_id(self, sync_conn) -> None:
        """When the upsert raises IntegrityError (race), the fallback SELECT
        returns the winner's id."""
        event = _make_event()

        # First call: creates the condition row normally.
        cid_1 = _resolve_condition_sync(sync_conn, event)
        sync_conn.commit()
        assert cid_1 is not None

        # Verify it exists.
        row = sync_conn.execute(
            select(AlertCondition.id).where(AlertCondition.signature_key == event.signature_key)
        ).scalar_one()
        assert row == cid_1

        # Patch the SELECT-first to miss (simulates race window) and the store
        # upsert_statement to raise IntegrityError; the fallback SELECTs again.
        original_execute = sync_conn.execute
        call_count = 0

        def selective_execute(stmt, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call is the lookup SELECT — return empty to force upsert
                return original_execute(select(AlertCondition.id).where(AlertCondition.id < 0))
            return original_execute(stmt, *args, **kwargs)

        def exploding_upsert(dialect, values):
            raise IntegrityError("mock", params={}, orig=Exception("dup"))

        with (
            patch("opsalert.store.upsert_statement", side_effect=exploding_upsert),
            patch.object(sync_conn, "execute", side_effect=selective_execute),
        ):
            cid_2 = _resolve_condition_sync(sync_conn, event)

        assert cid_2 == cid_1

    def test_non_integrity_error_propagates(self, sync_conn) -> None:
        """A non-IntegrityError from the upsert propagates to the caller."""
        event = _make_event()

        original_execute = sync_conn.execute
        call_count = 0

        def selective_execute(stmt, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Force the lookup SELECT to miss
                return original_execute(select(AlertCondition.id).where(AlertCondition.id < 0))
            return original_execute(stmt, *args, **kwargs)

        def exploding_upsert(dialect, values):
            raise OperationalError("mock", params={}, orig=Exception("disk full"))

        with (
            patch("opsalert.store.upsert_statement", side_effect=exploding_upsert),
            patch.object(sync_conn, "execute", side_effect=selective_execute),
        ):
            with pytest.raises(OperationalError):
                _resolve_condition_sync(sync_conn, event)
