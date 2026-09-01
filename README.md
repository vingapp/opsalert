# opsalert

Standalone operational alerting for Python applications. Fire-and-forget alerts from any context (async or sync), query them through a dashboard API, and deliver notifications via pluggable transports.

## Why

Application code needs to report operational problems — failed API calls, unexpected states, infrastructure issues — without disrupting the caller. opsalert provides a single `import opsalert; opsalert.error(...)` call that:

- Never raises exceptions (all failures logged, caller unaffected)
- Works from both async (FastAPI) and sync (Celery) contexts
- Auto-enriches every alert with caller location, active exception info, and Celery task context
- Stores structured data for dashboard display and programmatic triage
- Groups occurrences into **conditions** — the recurring problem behind them — with
  a lifecycle somebody can act on (acknowledge, resolve, collect)
- Delivers notifications via pluggable transports (email, webhook, log)

## Installation

```bash
pip install -e /path/to/opsalert
```

Requires Python 3.11+ and SQLAlchemy 2.0+. No other runtime dependencies.

## Quick Start

### 1. Create the tables

opsalert owns two database tables — `opsalert` (one row per occurrence) and
`alert_condition` (one row per recurring problem). Create them via your migration
tool, or with `opsalert.ensure_tables(engine)`:

```sql
CREATE TABLE opsalert (
    id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    severity VARCHAR(10) NOT NULL,
    category VARCHAR(100) NOT NULL,
    source VARCHAR(100),
    message VARCHAR(500) NOT NULL,
    context_json TEXT,
    notified BOOLEAN NOT NULL DEFAULT 0,
    condition_id INTEGER NULL REFERENCES alert_condition (id),
    created DATETIME NOT NULL
);

CREATE TABLE alert_condition (
    id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    signature_key VARCHAR(64) NOT NULL UNIQUE,
    category VARCHAR(100) NOT NULL,
    source VARCHAR(100),
    environment VARCHAR(50),
    message_template VARCHAR(500) NOT NULL,
    status VARCHAR(12) NOT NULL DEFAULT 'new',
    disposition VARCHAR(12),
    severity VARCHAR(10) NOT NULL,
    latest_severity VARCHAR(10),
    issue_url VARCHAR(500),
    resolved_by VARCHAR(100),
    notes TEXT,
    acknowledged_at DATETIME, acknowledged_by VARCHAR(100), status_changed_at DATETIME,
    resolved_at DATETIME, closed_at DATETIME,
    first_seen DATETIME, last_seen DATETIME,
    occurrence_count INTEGER NOT NULL DEFAULT 0,
    reopened_count INTEGER NOT NULL DEFAULT 0,
    median_interval_seconds INTEGER,
    stats_synced_through INTEGER NOT NULL DEFAULT 0,
    created DATETIME NOT NULL,
    updated DATETIME NOT NULL
);

CREATE INDEX ix_opsalert_cat_created ON opsalert (category, created);
CREATE INDEX ix_opsalert_cat_msg ON opsalert (category, message);
CREATE INDEX ix_opsalert_notified_sev ON opsalert (notified, severity, category);
CREATE INDEX ix_opsalert_cat_notified_created ON opsalert (category, notified, created);
CREATE INDEX ix_opsalert_created ON opsalert (created);
CREATE INDEX ix_opsalert_condition ON opsalert (condition_id, id);
CREATE INDEX ix_alert_condition_env_status ON alert_condition (environment, status);
CREATE INDEX ix_alert_condition_env_category ON alert_condition (environment, category);
CREATE INDEX ix_alert_condition_status_last_seen ON alert_condition (status, last_seen);
```

For Alembic integration, add `OpsAlertBase.metadata` to your `target_metadata`:

```python
# alembic/env.py
from opsalert.model import OpsAlertBase

target_metadata = [Base.metadata, OpsAlertBase.metadata]
```

### 2. Configure at startup

```python
import opsalert

opsalert.configure(
    session_factory=my_async_session_factory,  # async ctx mgr -> AsyncSession
)
```

### 3. Fire alerts from anywhere

```python
import opsalert

opsalert.warn("sendgrid_delivery", message="SendGrid 429", source="email")
opsalert.error("import_pipeline", message="Row 42 failed", source="contacts", context={"row": 42})
opsalert.critical("startup_failure", message="DB pool exhausted")
```

That's it. Each call creates one row in the `opsalert` table and links it to the
condition it is an instance of. If `configure()` hasn't been called (e.g., in a test
suite), calls silently no-op.

Pass `params` when the message has variable parts, and the condition's identity
becomes exact instead of guessed from the text:

```python
opsalert.error(
    "request_anomaly",
    message="PUT {route} exceeded its budget",   # the template IS the identity
    params={"route": "/api/view/shares/abc123/"},  # stored message is rendered
)
```

## Configuration

Call `opsalert.configure()` once at application startup. All parameters except `session_factory` are optional.

```python
opsalert.configure(
    # Required: async context manager that yields an AsyncSession.
    session_factory=fresh_async_session,

    # No-op mode: all fires silently skip. Use in test suites to prevent
    # alerts from leaking outside test transactions.
    testing=False,

    # Deployment environment label. When set, every alert subject is prefixed
    # "[STAGING] [ERROR] category: message", every alert email body opens with
    # an "Environment: staging" line, and every stored occurrence's context
    # carries "environment": "staging". Leave unset (None) for no labelling.
    environment="staging",

    # Category -> debugging guidance. Shown in next-fix output to help
    # developers understand what each category means and how to fix it.
    fix_hints={
        "sendgrid_delivery": "Check SendGrid dashboard for rate limits.",
        "import_pipeline": "Check the import file format and row data.",
    },

    # Default hint when no category-specific hint exists.
    default_fix_hint="Examine the tracebacks and code locations above.",

    # Pluggable notification transport (see Transports section).
    transport=opsalert.CallableTransport(my_send_function),

    # Static delivery settings. These are defaults — override at runtime
    # via get_setting if you need dynamic configuration.
    delivery_enabled=True,
    delivery_to_email="ops@example.com",
    delivery_from_email="alerts@example.com",
    delivery_from_name="OpsAlert",
    delivery_throttle_minutes=60,       # Min interval between emails per category
    delivery_digest_interval_minutes=360,  # Digest email interval
    retention_max_age_days=90,          # Auto-delete alerts older than this

    # Optional: runtime settings resolver. Takes a setting key, returns
    # the current value or None to fall back to the static default.
    # Use this to make settings configurable without restarts.
    get_setting=my_settings_resolver,

    # Optional: () -> (trace_id, trace_origin) for the current execution context.
    trace_provider=my_trace_provider,

    # Optional: () -> (user_id, org_id) for whoever the current request belongs
    # to. Failures are swallowed — attribution never costs an alert.
    identity_provider=my_identity_provider,
)
```

### Runtime Settings Resolution

When `get_setting` is provided, opsalert calls it before reading static config values. This lets you change delivery settings without restarting the application:

```python
def resolve_setting(key: str):
    """Look up opsalert settings from your app's config system."""
    mapping = {
        "delivery_enabled": "alerts.delivery.enabled",
        "delivery_to_email": "alerts.delivery.to_email",
        "retention_max_age_days": "alerts.retention.max_age_days",
    }
    if key in mapping:
        return my_config_store.get(mapping[key])
    return None  # Fall back to static default

opsalert.configure(get_setting=resolve_setting, ...)
```

## Fire API

Three severity levels, identical signatures:

```python
opsalert.warn(category, *, message, source=None, context=None, params=None)
opsalert.error(category, *, message, source=None, context=None, params=None)
opsalert.critical(category, *, message, source=None, context=None, params=None)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `category` | `str` | Broad error type (e.g., `"sendgrid_delivery"`). Used for Level 1 grouping. Pass your own StrEnum — opsalert treats it as a plain string. |
| `message` | `str` | Specific sub-type (e.g., `"SendGrid 429"`, `"GET /api/users/"`). Used for Level 2 grouping. |
| `source` | `str \| None` | Where the alert originated (e.g., `"email"`, `"api"`, `"celery"`). |
| `context` | `dict \| None` | Arbitrary structured data. Serialized as JSON. |
| `params` | `dict \| None` | Values for a `str.format`-style `message` template. With `params`, the raw template is the condition's identity and the stored message is the rendered text. A missing key renders as its own placeholder — it never raises. |

### Severity Levels

| Level | When to Use | Delivery |
|-------|-------------|----------|
| `warn` | Unexpected but non-breaking (unknown request param, recoverable retry) | Batched into periodic digest emails |
| `error` | Something failed that shouldn't have (pipeline error, API failure) | Individual email per category, throttled |
| `critical` | Infrastructure-level problem (DB pool exhausted, sweeper crash) | Individual email per category, throttled |

### Auto-Enrichment

Every alert's `context` dict is automatically enriched with underscore-prefixed debugging keys (won't collide with your data):

| Key | Value |
|-----|-------|
| `_caller` | `module:function:line` of the code that fired the alert |
| `_exc_type` | Exception class name (if fired inside an `except` block) |
| `_exc_message` | Exception message (truncated to 500 chars) |
| `_traceback` | Formatted traceback (truncated to 2000 chars) |
| `_task_name` | Celery task name (if running inside a Celery task) |
| `_task_id` | Celery task ID |
| `_trace_id`, `_trace_origin` | From the configured `trace_provider`, if any |
| `_user_id`, `_org_id` | From the configured `identity_provider`, if any |

### Async/Sync Detection

`opsalert.warn/error/critical()` auto-detect the execution context:

- **Async context** (FastAPI request handler): Creates a background task on the running event loop. Non-blocking.
- **Sync context** (Celery worker, script): Calls `asyncio.run()` to execute. Blocks briefly but never raises.

## Query API

All query functions are async, take an `AsyncSession`, and return plain dicts (not ORM objects).

### Level 1: Categories

```python
categories = await opsalert.query_categories(
    session,
    severity="error",    # Optional: filter by severity
    source="api",        # Optional: filter by source
    search="sendgrid",   # Optional: search in message text
)
# Returns: [{"category", "severity", "source", "count", "latest_message", "latest_created"}, ...]
```

Groups alerts by `category`. Returns the worst severity, total count, and most recent message per category. Uses a window function CTE for efficient latest-message lookup (no correlated subqueries).

### Level 2: Messages within a Category

```python
messages = await opsalert.query_messages(
    session,
    category="sendgrid_delivery",
    severity="error",   # Optional
    search="429",       # Optional
)
# Returns: [{"message", "count", "latest_created"}, ...]
```

### Level 3: Individual Occurrences

```python
items, total = await opsalert.query_occurrences(
    session,
    category="sendgrid_delivery",
    message="SendGrid 429",     # Optional
    severity="error",            # Optional
    source="email",              # Optional
    search="rate limit",         # Optional
    sort="-created",             # Default; prefix with - for descending
    limit=50,
    offset=0,
)
# items: [{"id", "severity", "category", "source", "message", "context_json", "notified", "created"}, ...]
# total: int (for pagination)
```

### Aggregates

```python
stats = await opsalert.query_aggregates(session)
# Returns: {"total": 142, "by_severity": {"error": 80, "warn": 50, "critical": 12}}
```

### Next Fix (Triage)

```python
fix = await opsalert.query_next_fix(session, max_samples=5, max_occurrences=200)
# Returns highest-priority alert group with aggregated debugging data:
# {
#     "category": "sendgrid_delivery",
#     "message": "SendGrid 429",
#     "severity": "error",
#     "count": 37,
#     "source": "email",
#     "first_created": datetime,
#     "latest_created": datetime,
#     "callers": ["module:function:line", ...],       # Unique code locations
#     "exception_signatures": ["ExcType:message", ...],
#     "tracebacks": ["...", "...", "..."],             # Up to 3 unique
#     "sample_contexts": [{...}, {...}, ...],          # Up to max_samples
# }
# Returns None if no alerts exist.
```

Priority: CRITICAL > ERROR > WARN, then highest count, then most recent. Fetches only `context_json` column with a LIMIT to avoid unbounded memory usage.

### Delete

```python
# Delete all alerts in a category (optionally filtered by message)
count = await opsalert.delete_by_category(session, category="sendgrid_delivery")
count = await opsalert.delete_by_category(session, category="sendgrid_delivery", message="SendGrid 429")

# Delete a single alert by ID
ok = await opsalert.delete_by_id(session, alert_id=123)
```

## Delivery

opsalert provides two delivery functions that your scheduler calls periodically. They are plain async functions with no scheduler dependency — wrap them in whatever scheduling system you use.

```python
# Call from your scheduler (e.g., every 5 minutes)
stats = await opsalert.deliver_alerts(session)
# {"immediate_sent": 2, "immediate_throttled": 1,
#  "immediate_throttled_conditions": 4, "digest_sent": 1,
#  "digest_count": 15, "reopened": 1, "collected": 3, "skipped": 0}
# immediate_throttled counts category EMAILS suppressed because every one of
# that category's conditions was inside its window; the *_conditions figure
# counts the throttled conditions themselves.

# Maintenance: fold occurrences into their conditions, then run the rules.
await opsalert.sync_condition_stats(session)
# {"adopted": 0, "conditions_updated": 4, "occurrences_counted": 12}
await opsalert.apply_lifecycle_rules(session)
# {"reopened": 0, "auto_closed": 1, "auto_staled": 0}

stats = await opsalert.cleanup_alerts(session)
# Returns: {"deleted": 42}
```

### Delivery Behavior

Delivery is decided per CONDITION, and it decides using only what it can see at
the moment it runs — it never assumes the maintenance sweep ran first.

1. **Reopen first.** Any unnotified occurrence on a `resolved`/`closed` condition
   reopens it (status → `new`, `reopened_count` + 1) BEFORE any gating, and that
   condition then emails immediately whatever its disposition says. A recurrence
   of something you thought was fixed can never be swallowed by the state you
   left it in.
2. **Then route by effective disposition** (explicit override, else derived from
   severity — error/critical → `immediate`, warn → `digest`):
   - `collect` — occurrences are marked notified, no email. Recorded, not announced.
   - `digest` — batched into the periodic digest.
   - `immediate` — grouped into ONE email per category per sweep whose body
     enumerates that category's conditions (10 listed, then "and N more"),
     and the throttle gates that email: it goes out only if at least one
     member is unthrottled.
   - An `acknowledged` condition gets the digest at most: somebody is already on
     it, and its occurrences keep accruing regardless.
3. **Occurrences with no condition** (a fire-time resolution failure, or rows
   older than conditions) are delivered by the original category-grouped path,
   unchanged.

Throttle state is per condition, but it gates the category email as a unit. A
category is mailed only when at least one member is unthrottled — never emailed
inside the window, or just reopened — so a brand-new condition is never shadowed
by a noisy category-mate, while a category whose members have all been emailed
once stays silent for the rest of the window. When the email goes out it carries
every member with unnotified occurrences, throttled ones included, and they are
all marked notified with it; when every member is throttled nothing is sent and
nothing is marked. Throttle state is read from notified occurrence rows, and
every transport-accepted send commits its notified-marks before the next send —
a delivered email's mark cannot roll back.

The digest subject and `AlertMessage.severity` carry the worst severity present,
not a hardcoded `warn` — P5 routes acknowledged error/critical conditions into
the digest, so it is not a warnings-only email.

**Cleanup:**
- Deletes occurrences older than `retention_max_age_days` — but only once they
  are provably counted (`id <= its condition's stats_synced_through`). Counters
  outlive the rows they were computed from; a row the sweeper has not folded in
  yet simply waits.
- Occurrences with no condition are deleted on age alone.
- Conditions that ever had an occurrence are never auto-deleted. An untriaged
  condition silent for 30 days auto-closes, and a recurrence reopens it.
- A condition with ZERO occurrences — the leftover of a fire whose attachment
  degraded (opsalert#2) — is reaped once older than
  `condition_empty_reap_minutes` (default 60), provided it is untriaged and
  unannotated. It records nothing, so nothing is lost.

## Conditions and lifecycle

An occurrence is one `opsalert.error(...)` call. A **condition** is the recurring
problem those occurrences are instances of — identified by
`hash(category, source, environment, message_template)`, where the template is
either the caller's explicit `params` template or one derived from the message
by a conservative normalizer (numbers, uuids, long hex ids, quoted strings and
timestamps become placeholders; a multi-line message collapses to its first line).

Environment is part of the identity: the same failure in staging and in
production is two conditions, so resolving the staging one never silences
production.

`status` and `disposition` are orthogonal:

| `status` | Meaning |
|----------|---------|
| `new` | Untriaged. On the attention line if its disposition is `immediate`. |
| `acknowledged` | Somebody has it. Leaves the attention line and immediate email; digest at most. Occurrences keep accruing. |
| `resolved` | Believed fixed. Auto-closes after `max(6h, 10 × median interval)` of silence. |
| `closed` | Done. A recurrence reopens it. |

| `disposition` | Meaning |
|---------------|---------|
| `NULL` | Derive from severity: error/critical → immediate, warn → digest. |
| `immediate` | Email as soon as it fires (throttled). |
| `digest` | Periodic digest only. |
| `collect` | Record occurrences, never email. ("wontfix" = acknowledged + collect + a note.) |

```python
await opsalert.set_status(session, condition_id, "acknowledged", actor="chris")
await opsalert.set_status(
    session, condition_id, "resolved", actor="chris",
    issue_url="https://github.com/org/repo/pull/144",
)
await opsalert.set_disposition(session, condition_id, "collect", actor="chris")
```

Transitions are validated (`closed → resolved` raises `ValueError`) and stamped:
`acknowledged_at`/`acknowledged_by`, `resolved_at`/`resolved_by`, `closed_at`,
`status_changed_at`.

### Condition queries

```python
items, total, aggregates = await opsalert.query_conditions(
    session, status="new", severity=None, category=None, search=None,
    sort="-last_seen", limit=50, offset=0,
)
# aggregates: {"byStatus": {...}, "bySeverity": {...}}  — env-scoped

result = await opsalert.query_attention(session, cursor=last_cursor)
# {"conditions": [{"id", "severity", "category", "template", "occurrence_count",
#                  "count_since_cursor", "last_seen", "reopened"}], "cursor": 1234}

occurrences, total = await opsalert.query_occurrences(session, condition_id=7)
```

`query_attention` is the watchdog's view: only `new` conditions whose effective
disposition is `immediate`, and — with a cursor — only those that have fired
since it. Nothing new means an empty list and the caller's own cursor back.
Without a cursor it returns the current attention set plus a fresh cursor, never
a flood of history. **The returned cursor is the highest occurrence id the
response actually reported** — never a global high-water mark, so a condition
that was created (or adopted from orphans) after the query read its candidate
set still has occurrences above the cursor and surfaces on the next call.
Results are ordered by that per-condition high-water mark ascending, so a
`limit` truncation drops only conditions the cursor stays below.
`query_next_fix` likewise skips occurrences whose condition is acknowledged,
resolved or closed.

### Failure behaviour

Condition resolution runs in a short-lived session of its own (or, where no
isolated session exists, inside a `SAVEPOINT` on the caller's session), so its
row lock is never held across the caller's work. Any failure — deadlock, pool
exhaustion, a broken factory — stores the occurrence with `condition_id = NULL`
and returns normally. The maintenance sweep adopts orphans on an unbounded
`condition_id IS NULL` scan, and delivery still emails them via the legacy
category path. Conditionization can fail; an alert cannot be lost to it.

## Transports

opsalert never depends on any specific email library. Instead, you inject a transport at configuration time.

### CallableTransport

Wraps your application's existing send function:

```python
from opsalert import CallableTransport

def send_via_sendgrid(message, *, to, from_addr, from_name):
    sg = SendGridEmail(
        to_emails=to,
        subject=message.subject,
        html_content=message.html_body,
        from_email=from_addr,
        from_name=from_name,
    )
    sg.send()
    return bool(sg.msg_id)

opsalert.configure(transport=CallableTransport(send_via_sendgrid), ...)
```

### WebhookTransport

POST JSON to Slack, PagerDuty, etc. Uses only stdlib (no requests/httpx):

```python
from opsalert import WebhookTransport

opsalert.configure(
    transport=WebhookTransport(
        "https://hooks.slack.com/services/T.../B.../xxx",
        headers={"Authorization": "Bearer token"},
    ),
    ...
)
```

Payload:
```json
{"severity": "error", "category": "...", "subject": "...", "text": "...", "alert_count": 5}
```

### LogTransport

Logs alerts via `logging.warning()` instead of sending. For development:

```python
from opsalert import LogTransport

opsalert.configure(transport=LogTransport(), ...)
```

### Custom Transport

Implement the `Transport` ABC:

```python
from opsalert import Transport
from opsalert.types import AlertMessage

class PagerDutyTransport(Transport):
    def send(self, message: AlertMessage, *, to: str, from_addr: str, from_name: str) -> bool:
        # Your implementation here. Never raise — return False on failure.
        ...
        return True
```

## Database Model

Two tables, owned entirely by the package.

### `opsalert` — occurrences

| Column | Type | Description |
|--------|------|-------------|
| `id` | `int` | Primary key |
| `severity` | `varchar(10)` | `warn`, `error`, or `critical` |
| `category` | `varchar(100)` | Broad error type (host app's vocabulary) |
| `source` | `varchar(100)` | Where the alert originated (nullable) |
| `message` | `varchar(500)` | Specific sub-type for Level 2 grouping |
| `context_json` | `text` | JSON-serialized structured data (nullable) |
| `notified` | `bool` | Whether delivery has been sent for this alert |
| `condition_id` | `int \| None` | The condition this is an instance of. NULL = orphan (resolution failed, or the row predates conditions) — never an error, never a reason to drop the occurrence. |
| `created` | `datetime(tz)` | UTC timestamp, auto-set on creation |

Alerts are write-once. Only `notified` and `condition_id` are ever updated.

### `alert_condition` — the problems behind them

| Column | Type | Description |
|--------|------|-------------|
| `id` | `int` | Primary key — the handle operators use |
| `signature_key` | `varchar(64)` | `sha256(category, source, environment, template)`, unique |
| `category`, `source`, `environment` | | Identity fields, copied for querying |
| `message_template` | `varchar(500)` | The template that identifies the problem |
| `status` | `varchar(12)` | `new`, `acknowledged`, `resolved`, `closed` |
| `disposition` | `varchar(12)` | `immediate`, `digest`, `collect`, or NULL (derive from severity) |
| `severity` | `varchar(10)` | Worst severity ever seen |
| `latest_severity` | `varchar(10)` | Severity of the most recent occurrence |
| `issue_url`, `resolved_by`, `notes` | | The human resolution record |
| `acknowledged_at`, `acknowledged_by`, `status_changed_at`, `resolved_at`, `closed_at` | `datetime(tz)` | Audit stamps |
| `first_seen`, `last_seen` | `datetime(tz)` | Stamped when the condition is created, then maintained by the stats sweep; survive pruning |
| `occurrence_count`, `reopened_count` | `int` | Ditto |
| `median_interval_seconds` | `int \| None` | Median gap over the last ≤50 occurrences; drives auto-close |
| `stats_synced_through` | `int` | Highest occurrence id folded into the counters. Cleanup may only delete at or below it. |
| `created`, `updated` | `datetime(tz)` | |

### Indexes

| Index | Columns | Purpose |
|-------|---------|---------|
| `ix_opsalert_cat_created` | category, created | Dashboard L1 GROUP BY |
| `ix_opsalert_cat_msg` | category, message | Dashboard L2 drill-down |
| `ix_opsalert_notified_sev` | notified, severity, category | Delivery sweeper |
| `ix_opsalert_cat_notified_created` | category, notified, created | Batch throttle check |
| `ix_opsalert_created` | created | Cleanup sweeper |
| `ix_admin_alert_condition` | condition_id, id | Condition drill-down, watermark scan, orphan adoption |
| `ix_alert_condition_env_status` | environment, status | Conditions list / attention |
| `ix_alert_condition_env_category` | environment, category | Category facet |
| `ix_alert_condition_status_last_seen` | status, last_seen | Lifecycle sweeps |

### Alembic Integration

opsalert uses its own `DeclarativeBase` (`OpsAlertBase`), separate from your host app's `Base`. To include it in Alembic autogenerate:

```python
# alembic/env.py
from src.core.database import Base
from opsalert.model import OpsAlertBase

target_metadata = [Base.metadata, OpsAlertBase.metadata]
```

## Testing

opsalert ships with a test suite that runs against an in-memory SQLite database
(the signature tests are pinned to real production messages):

```bash
pip install opsalert[dev]
pytest tests/ -q
```

### Testing in Your Application

Set `testing=True` to make all fire calls no-op:

```python
opsalert.configure(session_factory=..., testing=True)

# These do nothing — no database writes, no side effects
opsalert.error("anything", message="won't be stored")
```

If `configure()` is never called (common in unit tests), fire calls also silently no-op.

To test that your code fires the right alerts, patch at the call site:

```python
from unittest.mock import patch

# If your module does `import opsalert; opsalert.error(...)`,
# patch the module-level reference:
@patch("src.my_module.opsalert")
def test_fires_alert(mock_alert):
    do_something_that_should_alert()
    mock_alert.error.assert_called_once()
```

## Package Structure

```
opsalert/
    __init__.py        Public API re-exports
    _config.py         OpsAlertConfig dataclass, configure(), get_config()
    signature.py       Condition identity: normalize_message(), condition_signature()
    lifecycle.py       sync_condition_stats(), apply_lifecycle_rules(), set_status()
    _dispatch.py       warn/error/critical — fire-and-forget entry points
    _enrichment.py     Auto-capture caller, exception, Celery task info
    model.py           Alert + AlertCondition models (own DeclarativeBase)
    store.py           fire_alert() + condition resolution (isolated / SAVEPOINT)
    query.py           Dashboard selectors, conditions, attention, next-fix, delete
    delivery.py        Condition-gated email delivery with per-condition throttling
    cleanup.py         Watermark-gated TTL deletion
    transport.py       Transport ABC + CallableTransport, WebhookTransport, LogTransport
    types.py           AlertSeverity enum, AlertMessage dataclass
```
