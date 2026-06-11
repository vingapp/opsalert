"""Tests for query API — dashboard selectors, next-fix, aggregates, delete."""
from datetime import datetime, timezone

from sqlalchemy import select

from opsalert.model import Alert
from opsalert.store import fire_alert
from opsalert.query import (
    query_categories,
    query_messages,
    query_occurrences,
    query_by_trace_id,
    query_aggregates,
    query_next_fix,
    delete_by_category,
    delete_by_id,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _seed_alerts(session, alerts: list[dict]) -> list[Alert]:
    """Seed alert rows from a list of dicts."""
    rows = []
    for a in alerts:
        row = await fire_alert(session, **a)
        rows.append(row)
    await session.commit()
    return rows


# ---------------------------------------------------------------------------
# query_categories (Level 1)
# ---------------------------------------------------------------------------


class TestQueryCategories:
    """Test Level 1 grouped query."""

    async def test_groups_by_category(self, session):
        """Returns one row per category with count."""
        await _seed_alerts(session, [
            {"severity": "error", "category": "sendgrid", "message": "500"},
            {"severity": "error", "category": "sendgrid", "message": "429"},
            {"severity": "warn", "category": "validation", "message": "bad param"},
        ])

        cats = await query_categories(session)
        assert len(cats) == 2

        by_cat = {c["category"]: c for c in cats}
        assert by_cat["sendgrid"]["count"] == 2
        assert by_cat["validation"]["count"] == 1

    async def test_worst_severity_per_category(self, session):
        """Returns the worst severity within each category."""
        await _seed_alerts(session, [
            {"severity": "warn", "category": "infra", "message": "a"},
            {"severity": "critical", "category": "infra", "message": "b"},
            {"severity": "error", "category": "infra", "message": "c"},
        ])

        cats = await query_categories(session)
        assert cats[0]["severity"] == "critical"

    async def test_latest_message(self, session):
        """Returns the message from the most recent alert in each category."""
        a1 = Alert(
            severity="warn", category="cat", message="old",
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        a2 = Alert(
            severity="warn", category="cat", message="new",
            created=datetime(2024, 6, 1, tzinfo=timezone.utc),
        )
        session.add_all([a1, a2])
        await session.commit()

        cats = await query_categories(session)
        assert cats[0]["latest_message"] == "new"

    async def test_filter_by_severity(self, session):
        """severity= filter narrows to one severity."""
        await _seed_alerts(session, [
            {"severity": "warn", "category": "a", "message": "m"},
            {"severity": "error", "category": "b", "message": "m"},
        ])

        cats = await query_categories(session, severity="error")
        assert len(cats) == 1
        assert cats[0]["category"] == "b"

    async def test_filter_by_source(self, session):
        """source= filter narrows results."""
        await _seed_alerts(session, [
            {"severity": "warn", "category": "a", "message": "m", "source": "email"},
            {"severity": "warn", "category": "b", "message": "m", "source": "sms"},
        ])

        cats = await query_categories(session, source="email")
        assert len(cats) == 1
        assert cats[0]["category"] == "a"

    async def test_filter_by_search(self, session):
        """search= filter matches message text."""
        await _seed_alerts(session, [
            {"severity": "warn", "category": "a", "message": "SendGrid 500 error"},
            {"severity": "warn", "category": "b", "message": "Twilio failure"},
        ])

        cats = await query_categories(session, search="SendGrid")
        assert len(cats) == 1
        assert cats[0]["category"] == "a"

    async def test_empty_result(self, session):
        """Returns empty list when no alerts exist."""
        cats = await query_categories(session)
        assert cats == []


# ---------------------------------------------------------------------------
# query_messages (Level 2)
# ---------------------------------------------------------------------------


class TestQueryMessages:
    """Test Level 2 message grouping."""

    async def test_groups_by_message(self, session):
        """Returns one row per unique message within a category."""
        await _seed_alerts(session, [
            {"severity": "error", "category": "sendgrid", "message": "500"},
            {"severity": "error", "category": "sendgrid", "message": "500"},
            {"severity": "error", "category": "sendgrid", "message": "429"},
        ])

        msgs, total = await query_messages(session, category="sendgrid")
        assert len(msgs) == 2
        assert total == 2

        by_msg = {m["message"]: m for m in msgs}
        assert by_msg["500"]["count"] == 2
        assert by_msg["429"]["count"] == 1

    async def test_filter_by_severity(self, session):
        """severity= filter works at message level."""
        await _seed_alerts(session, [
            {"severity": "warn", "category": "cat", "message": "a"},
            {"severity": "error", "category": "cat", "message": "b"},
        ])

        msgs, total = await query_messages(session, category="cat", severity="error")
        assert len(msgs) == 1
        assert total == 1
        assert msgs[0]["message"] == "b"

    async def test_other_categories_excluded(self, session):
        """Only returns messages from the specified category."""
        await _seed_alerts(session, [
            {"severity": "warn", "category": "cat_a", "message": "a"},
            {"severity": "warn", "category": "cat_b", "message": "b"},
        ])

        msgs, total = await query_messages(session, category="cat_a")
        assert len(msgs) == 1
        assert total == 1
        assert msgs[0]["message"] == "a"

    async def test_pagination_bounds_groups(self, session):
        """limit/offset paginate the message groups; total is the full count.

        Regression for #orphan-flood: a category whose messages embed unique
        ids produced one group per occurrence, and an unbounded GROUP BY
        returned tens of thousands of rows. Pagination must bound the page
        while still reporting the true distinct-message total.
        """
        await _seed_alerts(session, [
            {"severity": "warn", "category": "flood", "message": f"task-{i}"}
            for i in range(25)
        ])

        page, total = await query_messages(session, category="flood", limit=10, offset=0)
        assert len(page) == 10
        assert total == 25

        page2, total2 = await query_messages(session, category="flood", limit=10, offset=20)
        assert len(page2) == 5
        assert total2 == 25


# ---------------------------------------------------------------------------
# query_occurrences (Level 3)
# ---------------------------------------------------------------------------


class TestQueryOccurrences:
    """Test Level 3 individual occurrences."""

    async def test_returns_items_and_total(self, session):
        """Returns (items, total) tuple."""
        await _seed_alerts(session, [
            {"severity": "error", "category": "cat", "message": "m"},
            {"severity": "error", "category": "cat", "message": "m"},
            {"severity": "error", "category": "cat", "message": "m"},
        ])

        items, total = await query_occurrences(session, category="cat")
        assert total == 3
        assert len(items) == 3

    async def test_pagination(self, session):
        """limit and offset paginate results."""
        await _seed_alerts(session, [
            {"severity": "warn", "category": "cat", "message": f"m{i}"}
            for i in range(10)
        ])

        items, total = await query_occurrences(session, category="cat", limit=3, offset=0)
        assert total == 10
        assert len(items) == 3

        items2, _ = await query_occurrences(session, category="cat", limit=3, offset=3)
        assert len(items2) == 3
        # No overlap
        ids1 = {i["id"] for i in items}
        ids2 = {i["id"] for i in items2}
        assert ids1.isdisjoint(ids2)

    async def test_sort_ascending(self, session):
        """sort=created returns oldest first."""
        a1 = Alert(
            severity="warn", category="cat", message="old",
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        a2 = Alert(
            severity="warn", category="cat", message="new",
            created=datetime(2024, 6, 1, tzinfo=timezone.utc),
        )
        session.add_all([a1, a2])
        await session.commit()

        items, _ = await query_occurrences(session, category="cat", sort="created")
        assert items[0]["message"] == "old"

    async def test_sort_descending(self, session):
        """sort=-created returns newest first (default)."""
        a1 = Alert(
            severity="warn", category="cat", message="old",
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        a2 = Alert(
            severity="warn", category="cat", message="new",
            created=datetime(2024, 6, 1, tzinfo=timezone.utc),
        )
        session.add_all([a1, a2])
        await session.commit()

        items, _ = await query_occurrences(session, category="cat", sort="-created")
        assert items[0]["message"] == "new"

    async def test_returns_dicts_not_orm(self, session):
        """Items are plain dicts with all expected keys."""
        await _seed_alerts(session, [
            {"severity": "error", "category": "cat", "message": "m",
             "source": "email", "context": {"key": "val"}},
        ])

        items, _ = await query_occurrences(session)
        item = items[0]
        assert isinstance(item, dict)
        assert set(item.keys()) == {
            "id", "severity", "category", "source", "message",
            "context_json", "notified", "created",
        }

    async def test_filter_by_message(self, session):
        """message= filter narrows to exact message match."""
        await _seed_alerts(session, [
            {"severity": "warn", "category": "cat", "message": "a"},
            {"severity": "warn", "category": "cat", "message": "b"},
        ])

        items, total = await query_occurrences(session, category="cat", message="a")
        assert total == 1
        assert items[0]["message"] == "a"


# ---------------------------------------------------------------------------
# query_aggregates
# ---------------------------------------------------------------------------


class TestQueryAggregates:
    """Test aggregate statistics."""

    async def test_empty_db(self, session):
        """Returns zeros when no alerts exist."""
        agg = await query_aggregates(session)
        assert agg["total"] == 0
        assert agg["by_severity"] == {}

    async def test_counts_by_severity(self, session):
        """Returns correct breakdown by severity."""
        await _seed_alerts(session, [
            {"severity": "warn", "category": "a", "message": "m"},
            {"severity": "warn", "category": "b", "message": "m"},
            {"severity": "error", "category": "c", "message": "m"},
            {"severity": "critical", "category": "d", "message": "m"},
        ])

        agg = await query_aggregates(session)
        assert agg["total"] == 4
        assert agg["by_severity"]["warn"] == 2
        assert agg["by_severity"]["error"] == 1
        assert agg["by_severity"]["critical"] == 1


# ---------------------------------------------------------------------------
# query_next_fix
# ---------------------------------------------------------------------------


class TestQueryNextFix:
    """Test next-fix priority selector."""

    async def test_empty_db(self, session):
        """Returns None when no alerts exist."""
        result = await query_next_fix(session)
        assert result is None

    async def test_prioritizes_critical_over_error(self, session):
        """Critical alerts are selected over errors regardless of count."""
        await _seed_alerts(session, [
            {"severity": "error", "category": "errors", "message": "many"},
            {"severity": "error", "category": "errors", "message": "many"},
            {"severity": "error", "category": "errors", "message": "many"},
            {"severity": "critical", "category": "infra", "message": "one"},
        ])

        result = await query_next_fix(session)
        assert result["severity"] == "critical"
        assert result["category"] == "infra"

    async def test_oldest_breaks_severity_tie(self, session):
        """When severities tie, oldest group (by first occurrence) wins."""
        await _seed_alerts(session, [
            {"severity": "error", "category": "a", "message": "m"},
            {"severity": "error", "category": "b", "message": "m"},
            {"severity": "error", "category": "b", "message": "m"},
            {"severity": "error", "category": "b", "message": "m"},
        ])

        result = await query_next_fix(session)
        # "a" was inserted first (oldest), so it wins despite fewer occurrences
        assert result["category"] == "a"
        assert result["count"] == 1

    async def test_aggregates_callers(self, session):
        """Collects unique _caller values from context."""
        await _seed_alerts(session, [
            {"severity": "error", "category": "cat", "message": "m",
             "context": {"_caller": "module_a:func:10"}},
            {"severity": "error", "category": "cat", "message": "m",
             "context": {"_caller": "module_b:func:20"}},
            {"severity": "error", "category": "cat", "message": "m",
             "context": {"_caller": "module_a:func:10"}},  # duplicate
        ])

        result = await query_next_fix(session)
        assert result["callers"] == ["module_a:func:10", "module_b:func:20"]

    async def test_aggregates_exception_signatures(self, session):
        """Collects unique exception signatures."""
        await _seed_alerts(session, [
            {"severity": "error", "category": "cat", "message": "m",
             "context": {"_exc_type": "ValueError", "_exc_message": "bad"}},
            {"severity": "error", "category": "cat", "message": "m",
             "context": {"_exc_type": "TypeError", "_exc_message": "wrong"}},
        ])

        result = await query_next_fix(session)
        assert len(result["exception_signatures"]) == 2

    async def test_limits_tracebacks_to_3(self, session):
        """Collects at most 3 unique tracebacks."""
        for i in range(5):
            await fire_alert(session, severity="error", category="cat", message="m",
                             context={"_exc_type": f"Err{i}", "_exc_message": f"m{i}",
                                      "_traceback": f"tb {i}"})
        await session.commit()

        result = await query_next_fix(session)
        assert len(result["tracebacks"]) == 3

    async def test_limits_samples(self, session):
        """Collects at most max_samples user contexts."""
        for i in range(10):
            await fire_alert(session, severity="error", category="cat", message="m",
                             context={"user_key": f"val_{i}"})
        await session.commit()

        result = await query_next_fix(session, max_samples=3)
        assert len(result["sample_contexts"]) == 3

    async def test_strips_enrichment_from_samples(self, session):
        """Sample contexts exclude _-prefixed enrichment keys."""
        await _seed_alerts(session, [
            {"severity": "error", "category": "cat", "message": "m",
             "context": {"_caller": "mod:f:1", "_exc_type": "E",
                         "status_code": 500, "endpoint": "/api/test"}},
        ])

        result = await query_next_fix(session)
        sample = result["sample_contexts"][0]
        assert "status_code" in sample
        assert "endpoint" in sample
        assert "_caller" not in sample
        assert "_exc_type" not in sample

    async def test_max_occurrences_limit(self, session):
        """Respects max_occurrences to avoid unbounded memory."""
        for i in range(20):
            await fire_alert(session, severity="error", category="cat", message="m",
                             context={"_caller": f"mod:f:{i}"})
        await session.commit()

        result = await query_next_fix(session, max_occurrences=5)
        # Should still return results, just from fewer occurrences
        assert result is not None
        assert result["count"] == 20  # count is from GROUP BY, not limited
        # Callers limited by occurrence fetch
        assert len(result["callers"]) <= 5

    async def test_returns_fix_hint_from_config(self, session, session_factory):
        """Returns fix_hint from configured fix_hints map."""
        import opsalert
        opsalert.configure(
            session_factory=session_factory,
            fix_hints={"sendgrid": "Check SendGrid API key and rate limits."},
            default_fix_hint="Generic fix hint.",
        )

        await _seed_alerts(session, [
            {"severity": "error", "category": "sendgrid", "message": "500"},
        ])

        result = await query_next_fix(session)
        assert result["fix_hint"] == "Check SendGrid API key and rate limits."

    async def test_returns_default_fix_hint_when_no_match(self, session, session_factory):
        """Returns default_fix_hint when category not in fix_hints map."""
        import opsalert
        opsalert.configure(
            session_factory=session_factory,
            fix_hints={"other_cat": "Irrelevant hint."},
            default_fix_hint="Default hint for unknown categories.",
        )

        await _seed_alerts(session, [
            {"severity": "error", "category": "unknown_cat", "message": "boom"},
        ])

        result = await query_next_fix(session)
        assert result["fix_hint"] == "Default hint for unknown categories."

    async def test_fix_hint_fallback_when_unconfigured(self, session):
        """Returns hardcoded fallback when opsalert is not configured."""
        # reset_opsalert_config fixture ensures unconfigured state
        await _seed_alerts(session, [
            {"severity": "error", "category": "cat", "message": "m"},
        ])

        result = await query_next_fix(session)
        assert result["fix_hint"] == "Examine the tracebacks and code locations above."


# ---------------------------------------------------------------------------
# delete_by_category
# ---------------------------------------------------------------------------


class TestDeleteByCategory:
    """Test bulk delete by category."""

    async def test_deletes_all_in_category(self, session):
        """Deletes all alerts in the specified category."""
        await _seed_alerts(session, [
            {"severity": "warn", "category": "target", "message": "a"},
            {"severity": "warn", "category": "target", "message": "b"},
            {"severity": "warn", "category": "keep", "message": "c"},
        ])

        count = await delete_by_category(session, category="target")
        await session.commit()
        assert count == 2

        remaining = (await session.execute(select(Alert))).scalars().all()
        assert len(remaining) == 1
        assert remaining[0].category == "keep"

    async def test_deletes_by_category_and_message(self, session):
        """Narrows deletion by message within category."""
        await _seed_alerts(session, [
            {"severity": "warn", "category": "cat", "message": "delete_me"},
            {"severity": "warn", "category": "cat", "message": "delete_me"},
            {"severity": "warn", "category": "cat", "message": "keep_me"},
        ])

        count = await delete_by_category(session, category="cat", message="delete_me")
        await session.commit()
        assert count == 2

        remaining = (await session.execute(select(Alert))).scalars().all()
        assert len(remaining) == 1
        assert remaining[0].message == "keep_me"

    async def test_returns_zero_for_nonexistent(self, session):
        """Returns 0 when category doesn't exist."""
        count = await delete_by_category(session, category="nonexistent")
        assert count == 0


# ---------------------------------------------------------------------------
# delete_by_id
# ---------------------------------------------------------------------------


class TestDeleteById:
    """Test single alert deletion."""

    async def test_deletes_existing_alert(self, session):
        """Returns True and deletes the alert."""
        rows = await _seed_alerts(session, [
            {"severity": "warn", "category": "cat", "message": "m"},
        ])

        ok = await delete_by_id(session, alert_id=rows[0].id)
        await session.commit()
        assert ok is True

        remaining = (await session.execute(select(Alert))).scalars().all()
        assert len(remaining) == 0

    async def test_returns_false_for_nonexistent(self, session):
        """Returns False when alert ID doesn't exist."""
        ok = await delete_by_id(session, alert_id=99999)
        assert ok is False


# ---------------------------------------------------------------------------
# query_by_trace_id
# ---------------------------------------------------------------------------


class TestQueryByTraceId:
    """Test trace_id lookup via JSON_EXTRACT on context_json."""

    async def test_finds_alerts_with_matching_trace_id(self, session):
        tid = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"
        await _seed_alerts(session, [
            {"severity": "error", "category": "cat", "message": "traced",
             "context": {"_trace_id": tid, "user_key": "val"}},
            {"severity": "warn", "category": "cat", "message": "other",
             "context": {"_trace_id": "ffffffffffffffffffffffffffffffff"}},
        ])

        results = await query_by_trace_id(session, tid)
        assert len(results) == 1
        assert results[0]["message"] == "traced"

    async def test_returns_empty_for_unknown_trace_id(self, session):
        await _seed_alerts(session, [
            {"severity": "error", "category": "cat", "message": "m",
             "context": {"_trace_id": "a" * 32}},
        ])

        results = await query_by_trace_id(session, "b" * 32)
        assert results == []

    async def test_skips_alerts_without_trace_id(self, session):
        tid = "c" * 32
        await _seed_alerts(session, [
            {"severity": "error", "category": "cat", "message": "with_trace",
             "context": {"_trace_id": tid}},
            {"severity": "error", "category": "cat", "message": "no_context"},
            {"severity": "error", "category": "cat", "message": "no_trace_key",
             "context": {"other_key": "val"}},
        ])

        results = await query_by_trace_id(session, tid)
        assert len(results) == 1
        assert results[0]["message"] == "with_trace"

    async def test_returns_multiple_alerts_for_same_trace(self, session):
        tid = "d" * 32
        await _seed_alerts(session, [
            {"severity": "error", "category": "cat", "message": "first",
             "context": {"_trace_id": tid}},
            {"severity": "warn", "category": "cat", "message": "second",
             "context": {"_trace_id": tid}},
            {"severity": "critical", "category": "cat", "message": "third",
             "context": {"_trace_id": tid}},
        ])

        results = await query_by_trace_id(session, tid)
        assert len(results) == 3

    async def test_respects_limit(self, session):
        tid = "e" * 32
        for i in range(5):
            await fire_alert(session, severity="error", category="cat",
                             message=f"msg {i}", context={"_trace_id": tid})
        await session.commit()

        results = await query_by_trace_id(session, tid, limit=2)
        assert len(results) == 2
