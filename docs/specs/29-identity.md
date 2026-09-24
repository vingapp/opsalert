# opsalert#29 — `identity` kwarg: split conditions of one kind without templating data

vingapi#736 slice 0 (owner-approved). Base: `lead/29-identity-kwarg` (cut from integration
`c6f468e`). Worker branch: `worker/29-identity`. One slice. Every change below is in scope;
nothing else is. Do not touch the normalizer, lifecycle rules, or delivery.

## Product rules

1. A caller can say "this occurrence belongs to the condition for THIS route/scope/entity"
   without smuggling the data into the message template. `identity={"route": "/x", "scope":
   "org", "entity": "42"}` on the same `kind` yields a different condition than
   `identity={"route": "/y", ...}`; two calls with equal identity share a condition.
2. Callers that pass nothing keep their condition. `identity=None` and `identity={}` produce a
   signature key and `fingerprint_json` byte-identical to today's. No existing condition moves.
3. Identity is exact, not guessed: key order is irrelevant, and no two distinct mappings can
   collide with each other or with any other fingerprint part.
4. Identity survives every path an occurrence can take: the queue, the sampled-out path, the
   drop record path, and lifecycle adoption. The drop path today RECOMPUTES the signature with
   an empty exception chain and empty origin frame (`ingest.py` `_resolve_condition_sync_from_drop`),
   so a dropped event already lands on a different condition than its siblings. That is the
   class this slice fixes: every place that needs a signature from a stored occurrence hashes
   the stored `fingerprint_json` parts, through one shared helper.

## Contract

- Note: the issue names `store.create_alert`; the function in this repo is `store.fire_alert`
  (`store.py`, `async def fire_alert`). The test name from the issue is kept as is.

### `opsalert/signature.py`

- `IDENTITY_SENTINEL = "identity"`.
- `def identity_parts(identity: Mapping[str, str] | None) -> list[str]`: returns `[]` for
  `None` or an empty mapping; otherwise `[IDENTITY_SENTINEL, json.dumps(dict(identity),
  sort_keys=True, separators=(",", ":"))]`. The JSON text of a non-empty dict always starts
  with `{`, so it can never equal the sentinel or an exception-class name; document that in the
  docstring — it is the collision argument.
- `event_signature(...)` and `event_fingerprint_parts(...)` gain `identity: Mapping[str, str] |
  None = None`. Parts order becomes
  `["2", kind, environment, *exception_chain, origin_frame, (template if not None), *identity_parts(identity)]`.
  With `identity` None/empty the parts list is unchanged from today.
- `def signature_from_parts(parts: Sequence[object]) -> str`: the hash `event_signature`
  computes, applied to an already-built parts list (`"\x1f".join(str(p).replace("\x1f"," "))`,
  sha256 hex). `event_signature` becomes `signature_from_parts(event_fingerprint_parts(...))`
  so the two cannot drift.

### `opsalert/_dispatch.py`

- `warn` / `error` / `critical` / `_fire_sync` gain `identity: Mapping[str, str] | None = None`
  as the LAST keyword. Passed through to `event_signature` / `event_fingerprint_parts`.
- Validation mirrors `kind`: every key and value must be `str`. In testing mode
  (`cfg.testing`) a non-str key or value raises `TypeError` (the lint/test-suite signal). In
  production it is coerced with `str()` and a `logger.warning` fires once per
  `(emit_site, repr(sorted(keys)))` (same `_invalid_kind_warned`-style set, separate set).
  The no-raise contract holds: nothing in this path may raise in production.
- Identity is NOT stored in the occurrence context; it lives in `fingerprint_json` only.

### `opsalert/store.py` — `fire_alert`

- Gains `identity: Mapping[str, str] | None = None`, passed to both signature helpers. Same
  None/{} invariant.

### `opsalert/ingest.py` — `_resolve_condition_sync_from_drop`

- If `dr.fingerprint_json` is set: `fp = signature_from_parts(json.loads(dr.fingerprint_json))`.
  On a `ValueError`/`TypeError` from `json.loads`, fall through to the existing recompute (log
  at warning). If it is not set, keep today's branches unchanged (v1 records).
- Delete the `exception_chain=[]` / `origin_frame=""` recompute for the `dr.kind` branch only
  when `fingerprint_json` was usable; the recompute stays as the fallback.

### `opsalert/lifecycle.py` — adoption (`~294-310`)

- Replace the inline sha256 reconstruction with `signature_from_parts(parts)`. Behaviour is
  identical; this is the "one helper" part of rule 4. No other lifecycle change.

### `README.md`

- Add `identity=None` to the three signatures in the API table and one row:
  `identity` | `Mapping[str, str] \| None` | Extra exact identity for the condition: same `kind`,
  different `identity` → different condition. Key order is irrelevant. `None` and `{}` are the
  same as omitting it. Use it for per-route / per-entity conditions instead of putting the value
  in the message template.

## Tests — `tests/test_identity_kwarg.py`

All run under the in-memory aiosqlite fixtures from `conftest.py`; the dispatch-path tests use
the same `configure(...)` + `flush()` pattern as `tests/test_ingest.py`. Names are the report.

- `test_identity_kwarg_splits_conditions_same_kind` — two `opsalert.error(kind="x.y", identity=A)`
  and two with `identity=B`, same message: after flush, exactly two `alert_condition` rows, each
  with two occurrences, and `fingerprint_json` of each contains `"identity"` followed by the
  canonical JSON.
- `test_no_identity_signature_unchanged` — `event_signature(...)` and
  `event_fingerprint_parts(...)` with `identity=None` equal the call without the kwarg AND equal a
  literal 64-hex value pinned in the test computed on the base commit (so a future reorder cannot
  pass silently). Same pin for the dispatch path: fire without identity, assert
  `signature_key` equals the pinned value.
- `test_empty_identity_signature_unchanged` — `identity={}` equals `identity=None` for
  `event_signature`, `event_fingerprint_parts`, and the row written by `opsalert.error`.
- `test_identity_order_insensitive` — `{"a":"1","b":"2"}` and `{"b":"2","a":"1"}` produce the
  same signature and the same `fingerprint_json`.
- `test_identity_encoding_has_no_collisions` — (a) `{"a":"b=c"}` vs `{"a=b":"c"}` differ; (b)
  identity `{"a":"b"}` with `exception_chain=[]` differs from `exception_chain=["identity",
  '{"a":"b"}']` with no identity and from `origin_frame="identity"` variants; (c) a parametrised
  sweep over 20 random mappings asserts all parts lists are distinct and every identity part
  starts with `{`.
- `test_identity_survives_drop_record_path` — fire an event with `kind`, an `exc` (so the
  chain is non-empty), and `identity`; force it through `_record_drop` (use the eviction
  pattern from `test_eviction_targets_biggest_fingerprint`) and resolve via
  `_resolve_condition_sync_from_drop`; the condition id equals the one a normally-written
  sibling event landed on. Seen RED on the base: today the drop path recomputes without the
  chain and creates a second condition.
- `test_identity_kwarg_on_create_alert` — `fire_alert(session, ..., kind="x.y", identity=A)`
  twice and once with `identity=B`: two conditions; `fire_alert(..., identity=None)` and
  `identity={}` share a condition with a call that omits the kwarg.
- `test_identity_non_str_raises_in_testing` — `identity={"a": 1}` under `testing=True` raises
  `TypeError`; under `testing=False` it enqueues with `"1"` and logs one warning.
- `test_lifecycle_adoption_relies_on_signature_from_parts` (defensive, in
  `tests/test_lifecycle.py`) — a v2 occurrence row with identity parts in `fingerprint_json` and
  a NULL `condition_id` is adopted onto the condition whose `signature_key` equals
  `signature_from_parts(parts)`. Docstring: lifecycle adoption breaks if the helper's hashing
  changes; fix the consumer, never the helper.

Run the whole suite green (`.venv/bin/pytest`), `ruff check`, and `mypy opsalert/` before opening
the PR. Report: the test list above with pass/fail, the red-on-base output for
`test_identity_survives_drop_record_path`, and the pinned signature value's provenance.
