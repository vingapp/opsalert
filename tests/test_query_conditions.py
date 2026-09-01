"""Condition queries — the list, the attention line (A8) and next-fix (A15)."""
import base64
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete, select

import opsalert
from opsalert.lifecycle import set_disposition, set_status, sync_condition_stats
from opsalert.model import Alert, AlertCondition
from opsalert.query import (
    _decode_attention_cursor,
    _encode_attention_cursor,
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
    """A8 — the watchdog line, and the per-condition cursor behind it (opsalert#4)."""

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
        await _fire_old(session, message="loud")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        result = await query_attention(session)
        assert len(result["conditions"]) == 1
        assert isinstance(result["cursor"], str)
        assert result["cursor"].startswith("2.")
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
        assert result["cursor"] != cursor

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

    async def test_a_condition_adopted_below_a_chattier_siblings_high_water_mark(
        self, session, session_factory
    ):
        """opsalert#4 headline — the reviewer's shape.

        Orphan occurrences are committed BEFORE a chattier condition's newest
        occurrence, and adopted into a condition of their own AFTER that
        sibling was reported. Their ids therefore sit *below* the highest
        occurrence id the caller has been told about. A max-included-occurrence
        cursor buries them; a per-condition mark set has no entry for the new
        condition at all, so it is reported once.
        """
        opsalert.configure(session_factory=session_factory)

        # Two occurrences whose condition resolution degraded (F1): stored,
        # owned by no condition, and — crucially — written first.
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

        # The chatty sibling fires above them and is reported.
        await _fire_old(session, message="loud")
        await session.commit()

        first = await query_attention(session)
        assert [c["template"] for c in first["conditions"]] == ["loud"]

        # Now the sweeper adopts the orphans — a condition born after the
        # snapshot, whose occurrence ids are all below "loud"'s newest — and
        # the sibling fires again.
        await sync_condition_stats(session)
        await session.commit()
        await _fire_old(session, message="loud")
        await session.commit()

        second = await query_attention(session, cursor=first["cursor"])

        assert {c["template"] for c in second["conditions"]} == {"loud", "orphaned boom"}
        by_template = {c["template"]: c for c in second["conditions"]}
        assert by_template["orphaned boom"]["count_since_cursor"] == 2
        assert by_template["loud"]["count_since_cursor"] == 1

        third = await query_attention(session, cursor=second["cursor"])
        assert third["conditions"] == []
        assert third["cursor"] == second["cursor"]

    async def test_a_late_committed_low_id_occurrence_counts_as_a_refire(
        self, session, session_factory
    ):
        """Auto-increment order is not commit order.

        A slow request reserves its occurrence id early and commits late, so
        the row lands below ids the watchdog has already been told about. The
        refire mark is a COUNT, not a high-water id, so it still fires.
        """
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="slow request")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()
        condition = await _condition_for(session, "slow request")

        # Push the sequence up, then report — the caller's high-water mark is
        # now well above the id the late row will use.
        session.add(
            Alert(
                id=500,
                severity="error",
                category="cat",
                message="slow request",
                condition_id=condition.id,
                created=datetime.now(UTC) - timedelta(minutes=10),
            )
        )
        await session.commit()
        first = await query_attention(session)
        assert first["conditions"][0]["count_since_cursor"] == 2

        # The late commit: a LOWER id, visible only now.
        session.add(
            Alert(
                id=200,
                severity="error",
                category="cat",
                message="slow request",
                condition_id=condition.id,
                created=datetime.now(UTC) - timedelta(minutes=10),
            )
        )
        await session.commit()

        second = await query_attention(session, cursor=first["cursor"])
        assert [c["template"] for c in second["conditions"]] == ["slow request"]
        assert second["conditions"][0]["count_since_cursor"] == 1

    async def test_a_never_reported_condition_with_no_occurrences_is_reported_once(
        self, session, session_factory
    ):
        """Identity, not occurrence ids: a zero-occurrence row still wakes once."""
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

        first = await query_attention(session)
        assert [c["template"] for c in first["conditions"]] == ["never fired"]
        assert first["conditions"][0]["count_since_cursor"] == 0

        second = await query_attention(session, cursor=first["cursor"])
        assert second["conditions"] == []

        condition = await _condition_for(session, "never fired")
        session.add(
            Alert(
                severity="error",
                category="cat",
                message="never fired",
                condition_id=condition.id,
                created=datetime.now(UTC) - timedelta(minutes=10),
            )
        )
        await session.commit()

        third = await query_attention(session, cursor=second["cursor"])
        assert [c["template"] for c in third["conditions"]] == ["never fired"]
        assert third["conditions"][0]["count_since_cursor"] == 1

    async def test_limit_truncation_reports_every_condition_exactly_once(
        self, session, session_factory
    ):
        """A truncated condition keeps its old mark, so the next call drains it."""
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

    async def test_limit_one_loses_neither_a_fresh_row_nor_a_refire(
        self, session, session_factory
    ):
        """limit=1 with a never-reported x0 row AND a refired row: both surface."""
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="chatty")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()
        cursor = (await query_attention(session, limit=1))["cursor"]

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
        await _fire_old(session, message="chatty")
        await session.commit()

        seen = []
        for _ in range(2):
            page = await query_attention(session, cursor=cursor, limit=1)
            seen.extend(c["template"] for c in page["conditions"])
            cursor = page["cursor"]

        assert sorted(seen) == ["chatty", "never fired"]

        drained = await query_attention(session, cursor=cursor, limit=1)
        assert drained["conditions"] == []

    async def test_reopening_a_condition_wakes_the_watchdog_again(
        self, session, session_factory
    ):
        """Leaving ``new`` drops the mark, so coming back is a fresh wake."""
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="loud")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        first = await query_attention(session)
        assert [c["template"] for c in first["conditions"]] == ["loud"]

        condition = await _condition_for(session, "loud")
        await set_status(session, condition, "acknowledged", actor="chris")
        await session.commit()

        quiet = await query_attention(session, cursor=first["cursor"])
        assert quiet["conditions"] == []

        await set_status(session, condition, "new", actor="chris")
        await session.commit()

        again = await query_attention(session, cursor=quiet["cursor"])
        assert [c["template"] for c in again["conditions"]] == ["loud"]
        assert again["conditions"][0]["count_since_cursor"] == 1

    async def test_a_legacy_integer_cursor_is_honoured_once_then_upgraded(
        self, session, session_factory
    ):
        """The cursor prod holds today is a bare int — one tick to upgrade."""
        opsalert.configure(session_factory=session_factory)
        first_alert = await _fire_old(session, message="already told")
        await _fire_old(session, message="brand new")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        result = await query_attention(session, cursor=str(first_alert.id))

        assert [c["template"] for c in result["conditions"]] == ["brand new"]
        assert result["cursor"].startswith("2.")

        settled = await query_attention(session, cursor=result["cursor"])
        assert settled["conditions"] == []

    async def test_a_legacy_zero_cursor_reports_everything_once(
        self, session, session_factory
    ):
        """``"0"`` is a v1 watermark, not a bootstrap — but they agree here."""
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="one")
        await _fire_old(session, message="two")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        result = await query_attention(session, cursor="0")
        assert sorted(c["template"] for c in result["conditions"]) == ["one", "two"]

        settled = await query_attention(session, cursor=result["cursor"])
        assert settled["conditions"] == []

    async def test_a_garbage_cursor_is_rejected_rather_than_flooding(
        self, session, session_factory
    ):
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="loud")
        await session.commit()

        with pytest.raises(ValueError):
            await query_attention(session, cursor="nope")

    async def test_a_v2_cursor_without_a_mark_map_is_rejected(
        self, session, session_factory
    ):
        """Valid base64, valid JSON, wrong shape — still not a bootstrap."""
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="loud")
        await session.commit()

        payload = base64.urlsafe_b64encode(b'{"nope": 1}').decode().rstrip("=")

        with pytest.raises(ValueError):
            await query_attention(session, cursor="2." + payload)

    async def test_marks_for_conditions_that_are_gone_are_pruned(
        self, session, session_factory
    ):
        """A cursor carrying ids that are no longer candidates does not grow."""
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, message="loud")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        stale = _encode_attention_cursor({999: 4, 1000: 1})
        result = await query_attention(session, cursor=stale)

        assert [c["template"] for c in result["conditions"]] == ["loud"]
        assert _decode_attention_cursor(result["cursor"])[0] == {
            (await _condition_for(session, "loud")).id: 1
        }

    async def test_an_empty_attention_set_hands_the_cursor_straight_back(
        self, session, session_factory
    ):
        """Nothing to report must not disturb what the caller already knows."""
        opsalert.configure(session_factory=session_factory)
        await _fire_old(session, severity="warn", message="quiet")  # digest, never here
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        bootstrap = await query_attention(session)
        assert bootstrap["conditions"] == []
        assert _decode_attention_cursor(bootstrap["cursor"])[0] == {}

        held = await query_attention(session, cursor=bootstrap["cursor"])
        assert held["conditions"] == []
        assert held["cursor"] == bootstrap["cursor"]

    async def test_marks_are_scoped_to_the_environment_they_were_issued_for(
        self, session, session_factory
    ):
        """One cursor per environment: a cross-environment call prunes to scope."""
        opsalert.configure(session_factory=session_factory, environment="staging")
        await _fire_old(session, message="staging only")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()
        staging_cursor = (await query_attention(session))["cursor"]
        assert _decode_attention_cursor(staging_cursor)[0] != {}

        opsalert.configure(session_factory=session_factory, environment="production")
        crossed = await query_attention(session, cursor=staging_cursor)
        assert crossed["conditions"] == []
        assert _decode_attention_cursor(crossed["cursor"])[0] == {}

        # The staging cursor itself was not mutated; used in its own scope it
        # still says "already told about that one".
        opsalert.configure(session_factory=session_factory, environment="staging")
        back = await query_attention(session, cursor=staging_cursor)
        assert back["conditions"] == []

    async def test_deleting_occurrences_lowers_the_mark_and_masks_one_refire(
        self, session, session_factory
    ):
        """The documented residual — pinned so it stays a known, bounded cost."""
        opsalert.configure(session_factory=session_factory)
        for _ in range(3):
            await _fire_old(session, message="loud")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()

        first = await query_attention(session)
        assert first["conditions"][0]["count_since_cursor"] == 3

        victim = (
            (
                await session.execute(
                    select(Alert).where(Alert.message == "loud").order_by(Alert.id)
                )
            )
            .scalars()
            .first()
        )
        await session.execute(delete(Alert).where(Alert.id == victim.id))
        await session.commit()

        # Live count (2) is below the mark (3): not a refire, and the mark
        # follows the count down.
        lowered = await query_attention(session, cursor=first["cursor"])
        assert lowered["conditions"] == []
        condition_id = (await _condition_for(session, "loud")).id
        assert _decode_attention_cursor(lowered["cursor"])[0] == {condition_id: 2}

        await _fire_old(session, message="loud")
        await session.commit()
        refired = await query_attention(session, cursor=lowered["cursor"])
        assert [c["template"] for c in refired["conditions"]] == ["loud"]
        assert refired["conditions"][0]["count_since_cursor"] == 1

    async def test_a_refire_between_a_deletion_and_the_next_call_is_masked_once(
        self, session, session_factory
    ):
        """The residual, stated exactly: the mark is only corrected on a call.

        An admin deletes an occurrence and the condition fires again before
        the watchdog polls. The live count is back to the old mark, not above
        it, so that one refire is not reported; the next one is.
        """
        opsalert.configure(session_factory=session_factory)
        for _ in range(3):
            await _fire_old(session, message="loud")
        await session.commit()
        await sync_condition_stats(session)
        await session.commit()
        cursor = (await query_attention(session))["cursor"]

        victim = (
            (
                await session.execute(
                    select(Alert).where(Alert.message == "loud").order_by(Alert.id)
                )
            )
            .scalars()
            .first()
        )
        await session.execute(delete(Alert).where(Alert.id == victim.id))
        await _fire_old(session, message="loud")
        await session.commit()

        masked = await query_attention(session, cursor=cursor)
        assert masked["conditions"] == []

        await _fire_old(session, message="loud")
        await session.commit()
        heard = await query_attention(session, cursor=masked["cursor"])
        assert [c["template"] for c in heard["conditions"]] == ["loud"]
        assert heard["conditions"][0]["count_since_cursor"] == 1


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
