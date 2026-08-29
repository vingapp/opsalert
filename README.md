# opsalert

Standalone operational alerting for Python applications. Fire-and-forget alerts from any context (async or sync), query them through a dashboard API, and deliver notifications via pluggable transports.

## Why

Application code needs to report operational problems — failed API calls, unexpected states, infrastructure issues — without disrupting the caller. opsalert provides a single `import opsalert; opsalert.error(...)` call that:

- Never raises exceptions (all failures logged, caller unaffected)
- Works from both async (FastAPI) and sync (Celery) contexts
- Auto-enriches every alert with caller location, active exception info, and Celery task context
- Stores structured data for dashboard display and programmatic triage
- Delivers notifications via pluggable transports (email, webhook, log)

## Installation

```bash
pip install -e /path/to/opsalert
```

Requires Python 3.11+ and SQLAlchemy 2.0+. No other runtime dependencies.

## Quick Start

### 1. Create the table

opsalert owns a single database table. Create it via your migration tool or directly:

```sql
CREATE TABLE opsalert (
    id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    severity VARCHAR(10) NOT NULL,
    category VARCHAR(100) NOT NULL,
    source VARCHAR(100),
    message VARCHAR(500) NOT NULL,
    context_json TEXT,
    notified BOOLEAN NOT NULL DEFAULT 0,
    created DATETIME NOT NULL
);

CREATE INDEX ix_opsalert_cat_created ON opsalert (category, created);
CREATE INDEX ix_opsalert_cat_msg ON opsalert (category, message);
CREATE INDEX ix_opsalert_notified_sev ON opsalert (notified, severity, category);
CREATE INDEX ix_opsalert_cat_notified_created ON opsalert (category, notified, created);
CREATE INDEX ix_opsalert_created ON opsalert (created);
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

That's it. Each call creates one row in the `opsalert` table. If `configure()` hasn't been called (e.g., in a test suite), calls silently no-op.

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
opsalert.warn(category, *, message, source=None, context=None)
opsalert.error(category, *, message, source=None, context=None)
opsalert.critical(category, *, message, source=None, context=None)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `category` | `str` | Broad error type (e.g., `"sendgrid_delivery"`). Used for Level 1 grouping. Pass your own StrEnum — opsalert treats it as a plain string. |
| `message` | `str` | Specific sub-type (e.g., `"SendGrid 429"`, `"GET /api/users/"`). Used for Level 2 grouping. |
| `source` | `str \| None` | Where the alert originated (e.g., `"email"`, `"api"`, `"celery"`). |
| `context` | `dict \| None` | Arbitrary structured data. Serialized as JSON. |

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
# Returns: {"immediate_sent": 2, "immediate_throttled": 1, "digest_sent": 1, "digest_count": 15}

stats = await opsalert.cleanup_alerts(session)
# Returns: {"deleted": 42}
```

### Delivery Behavior

**Immediate** (ERROR + CRITICAL):
- One email per unnotified category
- Throttled: won't re-send for the same category within `delivery_throttle_minutes`
- Single query with LEFT JOIN for throttle check (no N+1)

**Digest** (WARN):
- All unnotified warnings batched into one email
- Sent on each scheduler invocation if any exist

**Cleanup:**
- Deletes alerts older than `retention_max_age_days`

Both functions mark processed alerts as `notified=True` so they aren't re-sent.

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

Single table `opsalert`, owned entirely by the package:

| Column | Type | Description |
|--------|------|-------------|
| `id` | `int` | Primary key |
| `severity` | `varchar(10)` | `warn`, `error`, or `critical` |
| `category` | `varchar(100)` | Broad error type (host app's vocabulary) |
| `source` | `varchar(100)` | Where the alert originated (nullable) |
| `message` | `varchar(500)` | Specific sub-type for Level 2 grouping |
| `context_json` | `text` | JSON-serialized structured data (nullable) |
| `notified` | `bool` | Whether delivery has been sent for this alert |
| `created` | `datetime(tz)` | UTC timestamp, auto-set on creation |

Alerts are write-once. No `modified` column — only the `notified` flag is ever updated.

### Indexes

| Index | Columns | Purpose |
|-------|---------|---------|
| `ix_opsalert_cat_created` | category, created | Dashboard L1 GROUP BY |
| `ix_opsalert_cat_msg` | category, message | Dashboard L2 drill-down |
| `ix_opsalert_notified_sev` | notified, severity, category | Delivery sweeper |
| `ix_opsalert_cat_notified_created` | category, notified, created | Batch throttle check |
| `ix_opsalert_created` | created | Cleanup sweeper |

### Alembic Integration

opsalert uses its own `DeclarativeBase` (`OpsAlertBase`), separate from your host app's `Base`. To include it in Alembic autogenerate:

```python
# alembic/env.py
from src.core.database import Base
from opsalert.model import OpsAlertBase

target_metadata = [Base.metadata, OpsAlertBase.metadata]
```

## Testing

opsalert ships with 77 tests that run against an in-memory SQLite database:

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
    _dispatch.py       warn/error/critical — fire-and-forget entry points
    _enrichment.py     Auto-capture caller, exception, Celery task info
    model.py           Alert SQLAlchemy model (own DeclarativeBase)
    store.py           fire_alert() — single INSERT per call
    query.py           Dashboard selectors, next-fix, aggregates, delete
    delivery.py        Batched email delivery with throttling
    cleanup.py         TTL-based deletion
    transport.py       Transport ABC + CallableTransport, WebhookTransport, LogTransport
    types.py           AlertSeverity enum, AlertMessage dataclass
```
