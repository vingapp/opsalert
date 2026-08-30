"""Condition queries — the list, the attention line (A8) and next-fix (A15)."""
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

import opsalert
from opsalert.lifecycle import set_disposition, set_status, sync_condition_stats
from opsalert.model import Alert, AlertCondition
from opsalert.query import (
    query_attention,
    query_conditions,
    query_next_fix,
    query_occurrences,
)
from opsalert.store import fire_alert


async def _fire_old(session, *, severity="error", category="cat", message="boom"):
    alert = await fire_alert(session, severity=severity, category=category, message=message)
    alert.created = datetime.now(UTC) - timedelta(minutes=10)
    await session.flush()
    return alert


async def _condition_for(session, message) -> AlertCondition:
    return (
        await session.execute(
            select(AlertCondition).where(AlertCondition.message_template == message)
        )
    ).scalar_one()


class TestQueryConditions:
    async def test_lists_conditions_with_facet_counts(self, session, session_factory):
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="pool exhausted")
        await _fire_old(session, severity="warn", category="other", message="odd param")
        await session.commit()
        await sync_condition_stats(session)
        await set_status(session, await _condition_for(session, "odd param"), "acknowledged")
        await session.commit()

        items, total, aggregates = await query_conditions(session)

        assert total == 2
        assert aggregates["byStatus"] == {"new": 1, "acknowledged": 1}
        assert aggregates["bySeverity"] == {"error": 1, "warn": 1}
        assert {i["template"] for i in items} == {"pool exhausted", "odd param"}
        assert items[0]["effective_disposition"] in {"immediate", "digest"}

    async def test_filters_by_status_severity_category_and_search(
        self, session, session_factory
    ):
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="pool exhausted")
        await _fire_old(session, severity="warn", category="other", message="odd param")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        assert (await query_conditions(session, severity="warn"))[1] == 1
        assert (await query_conditions(session, category="other"))[1] == 1
        assert (await query_conditions(session, search="pool"))[1] == 1
        assert (await query_conditions(session, status="acknowledged"))[1] == 0

    async def test_facet_counts_ignore_the_facet_filters(self, session, session_factory):
        """Clicking "acknowledged" must not zero out the other facet's count."""
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="a")
        await _fire_old(session, message="b")
        await session.commit()
        await sync_condition_stats(session)
        await set_status(session, await _condition_for(session, "a"), "acknowledged")
        await session.commit()

        items, total, aggregates = await query_conditions(session, status="acknowledged")
        assert total == 1
        assert aggregates["byStatus"] == {"new": 1, "acknowledged": 1}

    async def test_pagination_and_sort(self, session, session_factory):
        opsalert.configure(session_factory=session_factory)
        for message in ("a", "b", "c"):
            await _fire_old(session, message=message)
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        page, total, _ = await query_conditions(session, limit=2, sort="category")
        assert total == 3
        assert len(page) == 2

    async def test_conditions_are_scoped_to_the_environment(self, session, session_factory):
        """P9 — a production list must never show staging's conditions."""
        opsalert.configure(session_factory=session_factory, environment="staging")
        await _fire_old(session, message="staging only")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        opsalert.configure(session_factory=session_factory, environment="production")
        await _fire_old(session, message="production only")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        items, total, _ = await query_conditions(session)  # configured: production
        assert total == 1
        assert items[0]["template"] == "production only"
        assert (await query_conditions(session, environment="staging"))[1] == 1


class TestOccurrenceDrilldown:
    async def test_occurrences_can_be_filtered_by_condition(self, session):
        first = await _fire_old(session, message="pool exhausted")
        await _fire_old(session, message="disk full")
        await session.commit()

        items, total = await query_occurrences(session, condition_id=first.condition_id)
        assert total == 1
        assert items[0]["message"] == "pool exhausted"
        assert items[0]["condition_id"] == first.condition_id


class TestAttention:
    """A8/W2 — what the watchdog is allowed to wake somebody for."""

    async def test_only_immediate_new_conditions_are_returned(
        self, session, session_factory
    ):
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="loud")  # error → immediate
        await _fire_old(session, severity="warn", message="quiet")  # warn → digest
        await _fire_old(session, message="parked")
        await _fire_old(session, message="handled")
        await session.commit()
        await sync_condition_stats(session)
        await set_disposition(session, await _condition_for(session, "parked"), "collect")
        await set_status(session, await _condition_for(session, "handled"), "acknowledged")
        await session.commit()

        result = await query_attention(session)

        assert [c["template"] for c in result["conditions"]] == ["loud"]

    async def test_an_owner_override_can_promote_a_warning(self, session, session_factory):
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, severity="warn", message="quiet but important")
        await session.commit()
        await sync_condition_stats(session)
        await set_disposition(
            session, await _condition_for(session, "quiet but important"), "immediate"
        )
        await session.commit()

        result = await query_attention(session)
        assert len(result["conditions"]) == 1

    async def test_bootstrap_returns_the_current_set_and_a_fresh_cursor(
        self, session, session_factory
    ):
        """No cursor is not "give me everything that ever happened"."""
        opsalert.configure(session_factory=session_factory)
        alert = await _fire_old(session, message="loud")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        result = await query_attention(session)
        assert len(result["conditions"]) == 1
        assert result["cursor"] == alert.id
        assert result["conditions"][0]["count_since_cursor"] == 1
        assert result["conditions"][0]["reopened"] is False

    async def test_nothing_new_since_the_cursor_is_an_empty_list(
        self, session, session_factory
    ):
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="loud")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        first = await query_attention(session)
        again = await query_attention(session, cursor=first["cursor"])

        assert again["conditions"] == []
        assert again["cursor"] == first["cursor"]

    async def test_a_recurrence_since_the_cursor_comes_back_with_its_count(
        self, session, session_factory
    ):
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="loud")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()
        cursor = (await query_attention(session))["cursor"]

        for _ in range(3):
            await _fire_old(session, message="loud")
        await session.commit()

        result = await query_attention(session, cursor=cursor)
        assert len(result["conditions"]) == 1
        assert result["conditions"][0]["count_since_cursor"] == 3
        assert result["cursor"] > cursor

    async def test_attention_is_scoped_to_the_environment(self, session, session_factory):
        opsalert.configure(session_factory=session_factory, environment="staging")
        await _fire_old(session, message="staging only")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        opsalert.configure(session_factory=session_factory, environment="production")
        result = await query_attention(session)
        assert result["conditions"] == []

    async def test_a_reopened_condition_says_so(self, session, session_factory):
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="loud")
        await session.commit()
        await sync_condition_stats(session)
        condition = await _condition_for(session, "loud")
        await set_status(session, condition, "resolved", actor="chris")
        await session.commit()

        from opsalert.lifecycle import reopen_condition

        reopen_condition(condition)
        await session.commit()

        result = await query_attention(session)
        assert result["conditions"][0]["reopened"] is True


class TestNextFixExcludesHandledConditions:
    """A15/P11 — do not hand back work somebody already picked up."""

    async def test_acknowledged_conditions_are_never_returned(
        self, session, session_factory
    ):
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, severity="critical", message="acknowledged problem")
        await session.commit()
        await sync_condition_stats(session)
        await set_status(
            session, await _condition_for(session, "acknowledged problem"), "acknowledged"
        )
        await session.commit()

        assert await query_next_fix(session) is None

    async def test_the_next_open_problem_is_returned_instead(self, session, session_factory):
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, severity="critical", message="acknowledged problem")
        await _fire_old(session, severity="error", message="still open")
        await session.commit()
        await sync_condition_stats(session)
        await set_status(
            session, await _condition_for(session, "acknowledged problem"), "acknowledged"
        )
        await session.commit()

        result = await query_next_fix(session)
        assert result["message"] == "still open"

    async def test_resolved_and_closed_are_excluded_too(self, session, session_factory):
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="fixed already")
        await session.commit()
        await sync_condition_stats(session)
        await set_status(session, await _condition_for(session, "fixed already"), "resolved")
        await session.commit()

        assert await query_next_fix(session) is None

    async def test_orphans_behave_exactly_as_before(self, session, session_factory):
        """A condition-less occurrence is still triage material."""
        opsalert.configure(session_factory=session_factory)
        session.add(Alert(severity="error", category="cat", message="orphan"))
        await session.commit()

        result = await query_next_fix(session)
        assert result["message"] == "orphan"
