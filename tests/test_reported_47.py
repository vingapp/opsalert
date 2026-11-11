"""opsalert#47: deliver_alerts stopped on prod.

``_load_condition_batches`` carried ``latest_message`` as a correlated scalar
subquery inside the per-condition aggregate. MySQL ran it as a DEPENDENT
SUBQUERY once per joined unsent row, each run reading and sorting every row
of the condition: condition 738 with 2,155 unsent of 43,370 rows was ~93M
reads, every sweep passed the 300 s soft limit and no alert mail went out.

The reported shape here: one condition with many occurrences, most already
notified and a block still unsent, beside a quieter second condition.
SQLite cannot show the MySQL plan, so the guard is on the SQL issued: the
aggregate carries no nested SELECT, and the statement count does not grow
with rows or conditions.
"""

from sqlalchemy import event, update

from opsalert.delivery import _load_condition_batches
from opsalert.model import Alert
from opsalert.store import fire_alert


async def _seed(session, category: str, total: int, unsent: int) -> str:
    """Fire ``total`` occurrences of one condition; leave the last ``unsent``
    unnotified. Returns the newest unsent rendered message."""
    ids = []
    for n in range(total):
        alert = await fire_alert(
            session,
            severity="error",
            category=category,
            message="probe run {run} stuck",
            params={"run": n},
        )
        ids.append(alert.id)
    await session.commit()
    await session.execute(
        update(Alert).where(Alert.id.in_(ids[: total - unsent])).values(notified=True)
    )
    await session.commit()
    return f"probe run {total - 1} stuck"


def _capture(engine) -> list[str]:
    statements: list[str] = []

    def _record(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", _record)
    return statements


async def test_reported_47_latest_message_is_not_a_per_row_subquery(engine, session):
    newest_738 = await _seed(session, "adp_probe_run", total=400, unsent=60)
    newest_other = await _seed(session, "sendgrid", total=5, unsent=2)

    statements = _capture(engine)
    batches, skipped = await _load_condition_batches(session)

    assert skipped == 0
    by_category = {b.category: b for b in batches}
    # The reader still gets the instance: the newest unsent occurrence's text.
    assert by_category["adp_probe_run"].latest_message == newest_738
    assert by_category["adp_probe_run"].count == 60
    assert by_category["sendgrid"].latest_message == newest_other
    assert by_category["sendgrid"].count == 2

    # Aggregate + one keyed message lookup, and no nested SELECT anywhere:
    # a correlated subquery in the aggregate is what MySQL ran per row.
    assert len(statements) == 2, statements
    for sql in statements:
        assert sql.upper().count("SELECT") == 1, sql


async def test_reported_47_statement_count_independent_of_condition_count(engine, session):
    for n in range(12):
        await _seed(session, f"cat_{n}", total=3, unsent=1)

    statements = _capture(engine)
    batches, _ = await _load_condition_batches(session)

    assert len(batches) == 12
    assert len(statements) == 2, statements
    assert {b.latest_message for b in batches} == {"probe run 2 stuck"}


async def test_reported_47_no_unsent_rows_issues_only_the_aggregate(engine, session):
    await _seed(session, "quiet", total=3, unsent=0)

    statements = _capture(engine)
    batches, _ = await _load_condition_batches(session)

    assert batches == []
    assert len(statements) == 1, statements
