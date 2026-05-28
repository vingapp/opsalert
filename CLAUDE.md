# opsalert — repo context

Standalone operational alerting. `import opsalert; opsalert.error(...)` from anywhere (async or sync) — fire-and-forget, structured storage, pluggable delivery.

For users-of-uptake guidance see `~/CLAUDE.md`. The repo README has the contract and quick-start — read it for the API surface.

## Stack

- Python 3.11+
- SQLAlchemy 2.0 (only runtime dep — keep it that way)
- Tests use aiosqlite (in-memory SQLite, no MySQL required)

## Layout

- `opsalert/` is the importable package — **at the repo root, not under `src/`** (hatchling build). Imports are `from opsalert import ...`.
- One DB table (`opsalert`) — opsalert owns its own schema.
- `tests/` — pytest, `asyncio_mode = "auto"`

## Hard contract (do not break)

- **`opsalert.error()` and friends MUST NOT raise.** All internal failures are logged; the caller continues. This is the central guarantee — every downstream consumer relies on it. If you're editing the dispatch path, any exception that escapes is a regression.
- Works from both async (FastAPI) and sync (Celery) contexts without the caller having to know which it's in.

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
