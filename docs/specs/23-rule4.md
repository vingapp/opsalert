# opsalert#23 — delete lifecycle rule 4; regression = "fix shipped and it still fires"

Owner decision (2026-09-10, option b): the acknowledged state stops watching the release
stamp. Regression detection lives on the resolved→reopened path, and its note names the
linked issue and the release the fix shipped in.

Base: `lead/23-ack-lease` (cut from integration `38e66ab`). Worker branch: `worker/23-rule4`.
One slice. Every change below is in scope; nothing else is.

## Product rules

1. An acknowledged condition re-surfaces ONLY on: severity escalation, burst, subject
   spread, or lease expiry (opsalert#7). A deploy is not a reason. An ack with no `--for`
   and no issue is "owned until something changes", never "owned until the next release".
2. A resolved (or closed) condition that fires again is the regression signal. The reopen
   note names the linked issue and the release the fix shipped in, so the reader knows
   *which* fix failed without opening the row.
3. The word "regression" appears in a note or a query field only for rule 2. A condition
   that merely persisted across a deploy is never called a regression.

## Changes

### `opsalert/lifecycle.py`

- **Delete rule 4** in `_escalate_acknowledged` (the `last_seen_release != acknowledged_release`
  block and its comment). Renumber the lease-expiry comment to "Rule 4". Update the function
  docstring and the module docstring (item 2) so neither mentions release/regression as an
  escalation reason.
- **`acknowledged_release` keeps being stamped** at ack (audit: "release at last ack"). It is
  no longer read by any rule or query. Update the comment in `reopen_condition` that says it is
  "a valid baseline for is_regression" — it is not; say it is audit only.
- **New column stamp `resolved_release`** (see model). `set_status(..., release: str | None = None)`
  new keyword. On `status == resolved`: `condition.resolved_release = release if release is not
  None else get_config().release` (from `opsalert._config`). Truncate to 40 chars like
  `_dispatch.py` does. On `closed` (from any status): leave `resolved_release` as is (closed
  follows resolved; the fix release is still the fix release). On `acknowledged` and on a
  human `new`: clear it (a new episode is owned; the old fix is history). `reopen_condition`
  KEEPS it (that is what makes the reopen a regression in the attention feed).
- **Reopen note.** `reopen_condition(condition, *, now=None, fired_release: str | None = None)`
  appends a note line via `_append_note`:
  - if `condition.resolved_release` is set:
    `reopened: regression — fired again under {fired_release or "unknown release"} after fix shipped in {resolved_release} ({issue})`
  - else: `reopened: fired again after being {resolved|closed} ({issue})`
  where `{issue}` is `condition.issue_url or condition.resolution_url or "no linked issue"`.
  The "resolved|closed" word is the status the condition is leaving.
  Callers:
  - `delivery._reopen_recurring`: pass `fired_release` = the `release` column of the newest
    (max id) unnotified occurrence on that condition created after the resolve/close stamp
    (same predicate as the id query; one extra query per reopened condition is fine — reopens
    are rare). `last_seen_release` is NOT correct here: delivery runs before the stats fold.
  - `lifecycle._reopen_recurrences` (the belt): pass `condition.last_seen_release` (stats have
    folded by then).
- Keep the existing `logger.warning` in `reopen_condition`.

### `opsalert/model.py`

- Add `resolved_release: Mapped[str | None] = mapped_column(String(40), nullable=True)` next to
  `acknowledged_release`, with a comment: "Release current when the condition was resolved —
  the release the fix shipped in. Kept through reopen (a reopen after this is a regression),
  cleared on ack / manual new."
- Fix the comment on `first_seen_release`/`last_seen_release` and `acknowledged_release`: they
  are no longer "used by the regression reopen rule".

### `opsalert/query.py`

- `_condition_to_dict` (the dict at ~line 470): add `"acknowledged_release"` and
  `"resolved_release"`.
- `query_attention`: `is_regression` becomes
  `condition.resolved_release is not None and condition.status == "new"`
  (a resolved/closed row is not in the attention feed anyway; an acked row had the stamp
  cleared). Drop the `acknowledged_release` comparison entirely.

### `README.md`

- DDL block: add `resolved_release VARCHAR(40)` (and the other v2 columns are already
  missing from that block — add ONLY `resolved_release`; do not widen).
- Lifecycle section: state rule 1 and rule 2 above in one or two sentences each; mention that
  `set_status(resolved)` stamps `resolved_release` from the `release` kwarg or the configured
  release.

### Host migration note

The column is DDL owned by vingapi's alembic (migration 264 family). Add one line to the
module docstring of `lifecycle.py` next to the existing "host MUST apply that migration"
paragraph naming `resolved_release`. The lead sends the DDL to the vingapi lane.

## Tests (`tests/test_lifecycle.py`, `tests/test_delivery_conditions.py`, `tests/test_query_conditions.py`)

Delete: `TestRegressionReopen` (both tests), `TestRegressionFullFlow`,
`test_attention_is_regression_computed` (replaced below). Keep every release-FOLD test
(`test_release_folded_from_occurrence_context`, watermark tests) — folding is unchanged.

Add, and see each red on the base before the change (report base sha + red output):

- `test_reported_23_acked_across_deploy_stays_acked` (test_lifecycle.py) — the reported case.
  Fixture is the staging shape: a `warn`, `immediate`-dispositioned condition fires under
  `_release="803942d"`, stats sync, acked with an issue and no lease (note "telemetry, not a
  fault"); one occurrence fires under `_release="9a1b2c3"` an hour later at the same severity;
  stats sync; `apply_lifecycle_rules`. Assert `escalated == 0`, status still `acknowledged`,
  `reopened_count == 0`, notes do not contain "regression", `last_seen_release == "9a1b2c3"`,
  `acknowledged_release == "803942d"`. Red on base: escalated == 1.
- `test_acked_no_issue_snooze_survives_deploy` — same shape but acked as a snooze
  (`acknowledged_until` a day out, no issue). Stays acknowledged after one occurrence under a new
  release. Red on base.
- `test_resolved_fires_under_fix_release_reopens_with_regression_note` — fire under
  `_release="v1"`, sync, `set_status(resolved, issue_url="https://github.com/vingapp/vingapi/issues/603", release="v2")`;
  assert `resolved_release == "v2"`. Fire under `_release="v2"` after resolve; `deliver_alerts`
  (delivery path). Assert status `new`, `reopened_count == 1`, `resolved_release == "v2"` kept,
  notes contain the exact line
  `reopened: regression — fired again under v2 after fix shipped in v2 (https://github.com/vingapp/vingapi/issues/603)`.
  Red on base (`release` kwarg does not exist).
- `test_resolved_release_defaults_to_configured_release` — `opsalert.configure(release="cfg-sha")`,
  `set_status(resolved)` with no `release` kwarg → `resolved_release == "cfg-sha"`. Reset config
  in a `finally` the way other config-touching tests do.
- `test_reopen_belt_note_uses_last_seen_release` — resolved condition, occurrence already
  `notified=True` (so delivery skips it), sync stats, `apply_lifecycle_rules`; note names
  `last_seen_release` as the fired release.
- `test_reopen_without_resolved_release_note_has_no_regression_word` — pre-v2 row: resolved with
  `resolved_release` NULL (set it to None after resolve), fires again; note is
  `reopened: fired again after being resolved (<issue or "no linked issue">)` and does not contain
  "regression".
- `test_ack_clears_resolved_release` — resolved (release="v2") → reopen → ack: `resolved_release`
  is None; `is_regression` in `query_attention` is False for it afterwards (it is acked, so it
  is not in attention at all — assert on the column and on `query_conditions` dict instead).
- `test_attention_is_regression_means_reopened_after_fix` (test_query_conditions.py) — a `new`
  condition with `resolved_release="v2"` → `is_regression is True`; a `new` condition with
  `acknowledged_release="v1"`, `last_seen_release="v2"`, `resolved_release=None` →
  `is_regression is False` (the old definition; red on base for the second assert).
- `test_condition_dict_carries_release_stamps` — `query_conditions` items carry
  `acknowledged_release` and `resolved_release`.

Defensive tests (tests-play-defense rule): the reopen note relies on `Alert.release` being
populated from `_release` context by `_dispatch` — add
`test_reopen_note_relies_on_dispatch_stamping_alert_release` in test_delivery_conditions.py
exercising the real `fire_alert` with `context={"_release": ...}` and asserting the note names it.

## Acceptance

Worker report = the list above, each test name with red-on-base evidence and green-on-branch,
plus full `pytest` green, `ruff check`, and `mypy opsalert` clean. No other files touched.
Commit message: `fix(lifecycle): drop ack release-change reopen; resolved→reopened is the regression signal (#23)`.
