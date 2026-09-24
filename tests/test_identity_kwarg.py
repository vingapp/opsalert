"""opsalert#29 — the ``identity`` kwarg.

A caller splits the conditions of one ``kind`` per route / scope / entity by
passing ``identity=`` instead of templating the value into the message.
Identity is exact (canonical JSON, key order irrelevant), cannot collide with
any other fingerprint part, and survives every path an occurrence can take —
including the drop record path, which used to recompute the signature without
the exception chain and origin frame.
"""
import json
import logging
import random
import string
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select, text

import opsalert
from opsalert import _dispatch, ingest
from opsalert.model import Alert, AlertCondition, OpsAlertBase
from opsalert.signature import (
    IDENTITY_SENTINEL,
    event_fingerprint_parts,
    event_signature,
    identity_parts,
    signature_from_parts,
)
from opsalert.store import fire_alert

# Pinned on base commit 199b11a (before this slice) by calling the base
# ``event_signature`` with PIN_KWARGS, and by firing
# ``opsalert.error("cat", message="boom", kind="x.y")`` with
# ``environment="test"`` and reading the queued Event's ``signature_key``.
# A reorder of the parts list, or identity leaking into a no-identity call,
# changes these values and fails loudly.
PIN_KWARGS = {
    "kind": "x.y",
    "environment": "test",
    "exception_chain": ["ValueError"],
    "origin_frame": "mod:fn",
}
PINNED_SIGNATURE = "19c880230c960dfa6404ac9f94193a4603d793881dd95acfd019d915f65b104a"
PINNED_PARTS = ["2", "x.y", "test", "ValueError", "mod:fn"]
PINNED_DISPATCH_SIGNATURE = (
    "0ea3bebeb735d6bf3099a54024538aa9e3142e567f766210b9796462f484cbf8"
)
PINNED_DISPATCH_FINGERPRINT = ["2", "x.y", "test", ""]

A = {"route": "/x", "scope": "org", "entity": "42"}
B = {"route": "/y", "scope": "org", "entity": "42"}


def _canonical(identity: dict[str, str]) -> str:
    return json.dumps(identity, sort_keys=True, separators=(",", ":"))


def _make_db(tmp_path, name="identity.db"):
    url = f"sqlite:///{tmp_path / name}"
    engine = create_engine(url)
    OpsAlertBase.metadata.create_all(engine)
    return url, engine


def _no_session_factory():
    raise RuntimeError("dispatch path must not use the session factory")


def _configure(url, **kwargs):
    opsalert.configure(session_factory=_no_session_factory, ingest_url=url, **kwargs)


def _hold_queue(monkeypatch):
    """Keep the writer thread from starting, so events stay in the queue."""
    monkeypatch.setattr(ingest, "_start_thread", lambda: None)


def _raised(exc: BaseException) -> BaseException:
    try:
        raise exc
    except BaseException as caught:
        return caught


# ---------------------------------------------------------------------------
# Rule 1 — identity splits conditions of one kind
# ---------------------------------------------------------------------------


def test_identity_kwarg_splits_conditions_same_kind(tmp_path):
    url, engine = _make_db(tmp_path)
    _configure(url)
    for identity in (A, A, B, B):
        opsalert.error("cat", message="same message", kind="x.y", identity=identity)
    result = opsalert.flush(timeout=5.0)
    assert result.written == 4

    with engine.connect() as conn:
        conditions = conn.execute(
            select(AlertCondition.id, AlertCondition.fingerprint_json)
        ).all()
        assert len(conditions) == 2
        for condition_id, fingerprint_json in conditions:
            occurrences = conn.execute(
                text("SELECT COUNT(*) FROM opsalert WHERE condition_id = :cid"),
                {"cid": condition_id},
            ).scalar()
            assert occurrences == 2
            parts = json.loads(fingerprint_json)
            assert parts[-2] == IDENTITY_SENTINEL
            assert parts[-1] in (_canonical(A), _canonical(B))
        tails = {json.loads(fp)[-1] for _, fp in conditions}
        assert tails == {_canonical(A), _canonical(B)}
    engine.dispose()


# ---------------------------------------------------------------------------
# Rule 2 — no identity keeps today's condition, byte for byte
# ---------------------------------------------------------------------------


def test_no_identity_signature_unchanged(tmp_path, monkeypatch):
    assert event_signature(**PIN_KWARGS, identity=None) == event_signature(**PIN_KWARGS)
    assert event_signature(**PIN_KWARGS) == PINNED_SIGNATURE
    assert event_fingerprint_parts(**PIN_KWARGS, identity=None) == event_fingerprint_parts(
        **PIN_KWARGS
    )
    assert event_fingerprint_parts(**PIN_KWARGS) == PINNED_PARTS

    _configure(f"sqlite:///{tmp_path / 'unused.db'}", environment="test")
    _hold_queue(monkeypatch)
    opsalert.error("cat", message="boom", kind="x.y")
    (event,) = list(ingest._queue)
    assert event.signature_key == PINNED_DISPATCH_SIGNATURE
    assert json.loads(event.fingerprint_json) == PINNED_DISPATCH_FINGERPRINT


def test_empty_identity_signature_unchanged(tmp_path):
    assert event_signature(**PIN_KWARGS, identity={}) == event_signature(
        **PIN_KWARGS, identity=None
    )
    assert event_fingerprint_parts(**PIN_KWARGS, identity={}) == event_fingerprint_parts(
        **PIN_KWARGS, identity=None
    )

    url, engine = _make_db(tmp_path)
    _configure(url, environment="test")
    opsalert.error("cat", message="boom", kind="x.y", identity={})
    opsalert.error("cat", message="boom", kind="x.y", identity=None)
    opsalert.error("cat", message="boom", kind="x.y")
    assert opsalert.flush(timeout=5.0).written == 3

    with engine.connect() as conn:
        rows = conn.execute(select(Alert.condition_id, Alert.fingerprint_json)).all()
        (condition,) = conn.execute(
            select(AlertCondition.signature_key, AlertCondition.fingerprint_json)
        ).all()
    assert len(rows) == 3
    assert len({r.condition_id for r in rows}) == 1
    assert {r.fingerprint_json for r in rows} == {json.dumps(PINNED_DISPATCH_FINGERPRINT)}
    assert condition.signature_key == PINNED_DISPATCH_SIGNATURE
    assert condition.fingerprint_json == json.dumps(PINNED_DISPATCH_FINGERPRINT)
    engine.dispose()


# ---------------------------------------------------------------------------
# Rule 3 — exact, order-insensitive, collision-free
# ---------------------------------------------------------------------------


def test_identity_order_insensitive():
    ab = {"a": "1", "b": "2"}
    ba = {"b": "2", "a": "1"}
    assert event_signature(**PIN_KWARGS, identity=ab) == event_signature(
        **PIN_KWARGS, identity=ba
    )
    assert json.dumps(event_fingerprint_parts(**PIN_KWARGS, identity=ab)) == json.dumps(
        event_fingerprint_parts(**PIN_KWARGS, identity=ba)
    )


def test_identity_encoding_has_no_collisions():
    base = {"kind": "x.y", "environment": "test"}

    # (a) the separator inside a key or value cannot shift the boundary
    assert event_signature(
        **base, exception_chain=[], origin_frame="", identity={"a": "b=c"}
    ) != event_signature(**base, exception_chain=[], origin_frame="", identity={"a=b": "c"})

    # (b) identity parts cannot be forged by other fingerprint parts
    with_identity = event_fingerprint_parts(
        **base, exception_chain=[], origin_frame="", identity={"a": "b"}
    )
    forgeries = [
        event_fingerprint_parts(
            **base, exception_chain=["identity", '{"a":"b"}'], origin_frame=""
        ),
        event_fingerprint_parts(
            **base, exception_chain=["identity"], origin_frame='{"a":"b"}'
        ),
        event_fingerprint_parts(**base, exception_chain=[], origin_frame="identity"),
        event_fingerprint_parts(
            **base, exception_chain=[], origin_frame="identity", identity={"a": "b"}
        ),
    ]
    for forged in forgeries:
        assert forged != with_identity
        assert signature_from_parts(forged) != signature_from_parts(with_identity)
    # The one case that could collide on parts alone — the chain spelling out
    # the identity tail — is blocked by the origin frame sitting in between.
    assert with_identity[-3:] == ["", IDENTITY_SENTINEL, '{"a":"b"}']
    assert forgeries[0][-3:] == [IDENTITY_SENTINEL, '{"a":"b"}', ""]

    # (c) a sweep of random mappings: every parts list distinct, every
    # identity part is JSON text starting with "{"
    rng = random.Random(29)
    alphabet = string.ascii_letters + string.digits + "=:,{}\"\\ \x1f"
    mappings: dict[str, dict[str, str]] = {}
    while len(mappings) < 20:
        mapping = {
            "".join(rng.choices(alphabet, k=rng.randint(1, 6))): "".join(
                rng.choices(alphabet, k=rng.randint(0, 6))
            )
            for _ in range(rng.randint(1, 4))
        }
        mappings[_canonical(mapping)] = mapping
    all_parts = []
    for mapping in mappings.values():
        extra = identity_parts(mapping)
        assert extra[0] == IDENTITY_SENTINEL
        assert extra[1].startswith("{")
        assert json.loads(extra[1]) == mapping
        all_parts.append(
            tuple(event_fingerprint_parts(**PIN_KWARGS, identity=mapping))
        )
    assert len(set(all_parts)) == 20
    assert tuple(PINNED_PARTS) not in set(all_parts)


# ---------------------------------------------------------------------------
# Rule 4 — identity survives the drop record path
# ---------------------------------------------------------------------------


def test_identity_survives_drop_record_path(tmp_path, monkeypatch):
    """A dropped event lands on the SAME condition as its written sibling.

    Uses the eviction pattern of ``test_eviction_targets_biggest_fingerprint``:
    the writer is held, the queue holds one event, the second fire evicts the
    first into ``_record_drop``.
    """
    url, engine = _make_db(tmp_path)
    _configure(url, environment="test", ingest_queue_max=1)
    _hold_queue(monkeypatch)

    exc = _raised(ValueError("boom"))
    for _ in range(2):
        opsalert.error("cat", message="boom", kind="x.y", exc=exc, identity=A)

    (sibling,) = list(ingest._queue)
    assert json.loads(sibling.fingerprint_json)[3] == "ValueError"  # chain non-empty
    drop = ingest._dropped[sibling.signature_key]
    assert drop.count == 1

    with engine.begin() as conn:
        ingest.write_batch(conn, [sibling], {}, datetime.now(UTC))
        sibling_condition = conn.execute(select(Alert.condition_id)).scalar_one()
        dropped_condition = ingest._resolve_condition_sync_from_drop(conn, drop)
        conditions = conn.execute(select(AlertCondition.signature_key)).scalars().all()

    assert sibling_condition is not None
    assert dropped_condition == sibling_condition
    assert conditions == [sibling.signature_key]
    engine.dispose()


def test_drop_record_with_unreadable_fingerprint_falls_back_to_recompute(
    tmp_path, caplog
):
    """A drop whose stored parts cannot be read still resolves (v1-style recompute)."""
    _, engine = _make_db(tmp_path)
    drop = ingest.DropRecord(
        count=1,
        category="cat",
        source=None,
        environment="test",
        template="boom",
        severity="error",
        kind="x.y",
        fingerprint_json="{not json",
    )
    with caplog.at_level(logging.WARNING, logger="opsalert.internal"):
        with engine.begin() as conn:
            condition_id = ingest._resolve_condition_sync_from_drop(conn, drop)
            key = conn.execute(select(AlertCondition.signature_key)).scalar_one()
    assert condition_id is not None
    assert key == event_signature(
        kind="x.y", environment="test", exception_chain=[], origin_frame=""
    )
    assert any("fingerprint_json" in r.getMessage() for r in caplog.records)
    engine.dispose()


# ---------------------------------------------------------------------------
# Direct store path (the issue calls it create_alert; it is store.fire_alert)
# ---------------------------------------------------------------------------


async def test_identity_kwarg_on_create_alert(session):
    async def fire(**kwargs):
        alert = await fire_alert(
            session, severity="error", category="cat", message="boom", kind="x.y", **kwargs
        )
        return alert.condition_id

    a1 = await fire(identity=A)
    a2 = await fire(identity=dict(reversed(list(A.items()))))
    b1 = await fire(identity=B)
    assert a1 is not None and b1 is not None
    assert a1 == a2
    assert a1 != b1

    plain = await fire()
    assert await fire(identity=None) == plain
    assert await fire(identity={}) == plain
    assert plain not in (a1, b1)

    conditions = (await session.execute(select(AlertCondition))).scalars().all()
    assert len(conditions) == 3


# ---------------------------------------------------------------------------
# Validation mirrors kind: raise in testing, coerce + warn once in production
# ---------------------------------------------------------------------------


def test_identity_non_str_raises_in_testing(tmp_path, monkeypatch, caplog):
    opsalert.configure(session_factory=_no_session_factory, testing=True)
    with pytest.raises(TypeError):
        opsalert.error("cat", message="boom", kind="x.y", identity={"a": 1})
    with pytest.raises(TypeError):
        opsalert.warn("cat", message="boom", kind="x.y", identity={1: "a"})
    # str-only identity is fine in testing mode (no-op, no raise)
    opsalert.critical("cat", message="boom", kind="x.y", identity={"a": "1"})

    opsalert.reset_config()
    _configure(f"sqlite:///{tmp_path / 'unused.db'}")
    _hold_queue(monkeypatch)
    monkeypatch.setattr(_dispatch, "_invalid_identity_warned", set())
    with caplog.at_level(logging.WARNING, logger="opsalert._dispatch"):
        for _ in range(2):
            opsalert.error("cat", message="boom", kind="x.y", identity={"a": 1})
    events = list(ingest._queue)
    assert len(events) == 2
    for event in events:
        assert json.loads(event.fingerprint_json)[-2:] == [IDENTITY_SENTINEL, '{"a":"1"}']
        assert event.signature_key == event_signature(
            kind="x.y", environment=None, exception_chain=[], origin_frame="",
            identity={"a": "1"},
        )
    warnings = [r for r in caplog.records if "identity" in r.getMessage()]
    assert len(warnings) == 1


def test_identity_non_mapping_never_raises_in_production(tmp_path, monkeypatch):
    """The no-raise contract: an unusable identity is dropped, the alert is kept."""
    _configure(f"sqlite:///{tmp_path / 'unused.db'}")
    _hold_queue(monkeypatch)
    monkeypatch.setattr(_dispatch, "_invalid_identity_warned", set())
    opsalert.error("cat", message="boom", kind="x.y", identity=["not", "a", "mapping"])
    (event,) = list(ingest._queue)
    assert IDENTITY_SENTINEL not in json.loads(event.fingerprint_json)


def test_identity_not_stored_in_occurrence_context(tmp_path, monkeypatch):
    _configure(f"sqlite:///{tmp_path / 'unused.db'}")
    _hold_queue(monkeypatch)
    opsalert.error("cat", message="boom", kind="x.y", identity=A)
    (event,) = list(ingest._queue)
    assert "identity" not in (event.context or {})
    assert "/x" not in json.dumps(event.context or {}, default=str)


# ---------------------------------------------------------------------------
# Guard: one hashing helper
# ---------------------------------------------------------------------------


def test_signature_hashing_lives_only_in_signature_module():
    """Every signature is computed in ``opsalert/signature.py``.

    A second inline sha256 reconstruction (what lifecycle adoption used to
    have) is how two paths drift onto different conditions. Route it through
    ``signature_from_parts`` instead of adding it to an allow-list.
    """
    package = Path(opsalert.__file__).parent
    offenders = [
        path.name
        for path in package.glob("*.py")
        if path.name != "signature.py"
        and ("sha256" in path.read_text() or '"\\x1f".join' in path.read_text())
    ]
    assert offenders == []

