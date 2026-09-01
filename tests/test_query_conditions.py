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

    async def test_the_cursor_never_passes_a_condition_that_was_not_returned(
        self, session, session_factory
    ):
        """opsalert#4 — a condition born after the candidate snapshot must survive.

        Orphan occurrences exist before the first call but their condition is
        only created later (the adoption sweep). If the first call's cursor had
        advanced past those occurrence ids, the condition could never be
        reported: it has already fired, and nothing above the cursor is left.
        """
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="loud")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        # Two occurrences whose condition resolution degraded (F1): stored,
        # counted by nobody yet, owned by no condition.
        for _ in range(2):
            session.add(
                Alert(
                    severity="error",
                    category="cat",
                    message="orphaned boom",
                    condition_id=None,
                    created=datetime.now(UTC) - timedelta(minutes=10),
                )
            )
        await session.commit()

        first = await query_attention(session)
        assert [c["template"] for c in first["conditions"]] == ["loud"]

        # The sweeper adopts them — the condition is born now, after the
        # snapshot the first call took.
        await sync_condition_stats(session)
        await session.commit()

        second = await query_attention(session, cursor=first["cursor"])

        assert [c["template"] for c in second["conditions"]] == ["orphaned boom"]
        assert second["conditions"][0]["count_since_cursor"] == 2
        assert second["cursor"] > first["cursor"]

    async def test_limit_truncation_is_cursor_safe(self, session, session_factory):
        """A truncated condition is drained by the next call, never skipped."""
        opsalert.configure(session_factory=session_factory)
        for message in ("first", "second", "third"):
            await _fire_old(session, message=message)
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        page1 = await query_attention(session, limit=2)
        assert [c["template"] for c in page1["conditions"]] == ["first", "second"]

        page2 = await query_attention(session, cursor=page1["cursor"], limit=2)
        assert [c["template"] for c in page2["conditions"]] == ["third"]

        page3 = await query_attention(session, cursor=page2["cursor"], limit=2)
        assert page3["conditions"] == []
        assert page3["cursor"] == page2["cursor"]

    async def test_a_reported_condition_repeats_only_when_it_fires_again(
        self, session, session_factory
    ):
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="loud")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        first = await query_attention(session)
        quiet = await query_attention(session, cursor=first["cursor"])
        assert quiet["conditions"] == []
        assert quiet["cursor"] == first["cursor"]

        await _fire_old(session, message="loud")
        await session.commit()

        again = await query_attention(session, cursor=quiet["cursor"])
        assert [c["template"] for c in again["conditions"]] == ["loud"]
        assert again["conditions"][0]["count_since_cursor"] == 1

    async def test_an_empty_attention_set_hands_the_cursor_straight_back(
        self, session, session_factory
    ):
        """Nothing to report must not move the cursor — in either direction."""
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, severity="warn", message="quiet")  # digest, never here
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        bootstrap = await query_attention(session)
        assert bootstrap["conditions"] == []
        assert bootstrap["cursor"] == 0

        held = await query_attention(session, cursor=17)
        assert held["conditions"] == []
        assert held["cursor"] == 17

    async def test_a_cursor_beyond_every_occurrence_comes_back_unchanged(
        self, session, session_factory
    ):
        """The cursor never moves backwards, even when it is ahead of the table."""
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="loud")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        result = await query_attention(session, cursor=10_000)

        assert result["conditions"] == []
        assert result["cursor"] == 10_000

    async def test_a_quiet_new_condition_neither_reports_nor_holds_the_cursor_back(
        self, session, session_factory
    ):
        """Still ``new``, but everything it did is below the cursor."""
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="old news")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()
        cursor = (await query_attention(session))["cursor"]

        await _fire_old(session, message="fresh")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        result = await query_attention(session, cursor=cursor)

        assert [c["template"] for c in result["conditions"]] == ["fresh"]
        fresh = (
            await session.execute(
                select(Alert).where(Alert.message == "fresh")
            )
        ).scalar_one()
        assert result["cursor"] == fresh.id

    async def test_bootstrap_lists_a_condition_with_no_occurrences_without_a_cursor_jump(
        self, session, session_factory
    ):
        """A zero-occurrence condition is visible but contributes no cursor."""
        opsalert.configure(session_factory=session_factory)
        session.add(
            AlertCondition(
                signature_key="empty-one",
                category="cat",
                message_template="never fired",
                status="new",
                severity="error",
                latest_severity="error",
            )
        )
        await session.commit()

        result = await query_attention(session)

        assert [c["template"] for c in result["conditions"]] == ["never fired"]
        assert result["cursor"] == 0


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
