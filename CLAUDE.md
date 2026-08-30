# opsalert — repo context

Standalone operational alerting. `import opsalert; opsalert.error(...)` from anywhere (async or sync) — fire-and-forget, structured storage, pluggable delivery.

For users-of-uptake guidance see `~/CLAUDE.md`. The repo README has the contract and quick-start — read it for the API surface.

## Stack

- Python 3.11+
- SQLAlchemy 2.0 (only runtime dep — keep it that way)
- Tests use aiosqlite (in-memory SQLite, no MySQL required)

## Layout

- `opsalert/` is the importable package — **at the repo root, not under `src/`** (hatchling build). Imports are `from opsalert import ...`.
- Two DB tables — opsalert owns its own schema:
  - `opsalert` — one row per **occurrence** (one `opsalert.error(...)` call). Volatile:
    pruned on a retention clock.
  - `alert_condition` — one row per **condition**, the recurring problem those
    occurrences are instances of. Carries the state a human cares about (status,
    disposition, issue url, counters) and is never auto-deleted.
- `tests/` — pytest, `asyncio_mode = "auto"`

## Hard contract (do not break)

- **`opsalert.error()` and friends MUST NOT raise.** All internal failures are logged; the caller continues. This is the central guarantee — every downstream consumer relies on it. If you're editing the dispatch path, any exception that escapes is a regression.
- Works from both async (FastAPI) and sync (Celery) contexts without the caller having to know which it's in.
- **No occurrence is ever lost to conditionization.** `condition_id` is nullable;
  any resolution failure stores the occurrence with NULL and returns normally.
  Resolution runs in its own short-lived session (or a `SAVEPOINT` when no isolated
  session exists) so its row lock is never held across the caller's transaction.
- **Delivery reopens inline.** An unnotified occurrence on a resolved/closed
  condition reopens it BEFORE disposition gating — never rely on sweep ordering
  for that, or a `collect` disposition will swallow a recurrence.
- **A sent mark is unlosable**: every transport-accepted send commits its
  notified-marks before the next send.
- **Cleanup deletes only what is provably counted**: an occurrence goes when
  `created < cutoff AND id <= its condition's stats_synced_through` (orphans on
  age alone). The stats sweep only counts occurrences older than a 60s lag,
  because auto-increment order is not commit order.

## API surface (additions)

- `warn/error/critical(category, *, message, source, context, params=None)` —
  `params` makes the emission structured: `message` is a format template, the
  template is the condition's identity, the rendered text is what is stored.
  The call shape without `params` is unchanged.
- Sweeper entry points: `sync_condition_stats`, `apply_lifecycle_rules`
  (alongside `deliver_alerts` / `cleanup_alerts`).
- Lifecycle edits: `set_status`, `set_disposition` (transition-validated; they
  RAISE on an invalid transition — they are admin actions, not the fire path).
- Queries: `query_conditions`, `query_attention`, `query_occurrences(condition_id=)`.

## Consumers

- **vingapi** imports `opsalert` broadly (main, reporting, files, integrations, messaging, validation, oauth, admin) — it's the production alerting path. vingapi's pyproject declares `opsalert` as a regular dependency.
- **debork** has it as an optional `[alerts]` extra for failure reporting.
- **vingserver** does not use opsalert — legacy alerting is separate.

## Commands

```bash
.venv/bin/pytest                 # uptake plugin auto-runs
.venv/bin/uptake-lint
```

## Conventions

(Accreting.)
