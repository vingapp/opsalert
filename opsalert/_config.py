"""Configuration — single configure() call wires everything at startup."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from opsalert.transport import Transport


@dataclass
class OpsAlertConfig:
    """Package configuration. Set once via configure(), read everywhere."""

    # Required: async context manager → AsyncSession
    session_factory: Callable[..., Any] | None = None

    # No-op mode: all fires silently skip (use in test suites)
    testing: bool = False

    # Deployment environment label ("staging", "production", ...). When set it
    # prefixes every alert subject, heads every alert email body, and is stamped
    # into every stored occurrence's context. None = no labelling at all, so
    # consumers that never configure it are unaffected.
    environment: str | None = None

    # Category → debugging guidance (host app provides its own)
    fix_hints: dict[str, str] = field(default_factory=dict)
    default_fix_hint: str = "Examine the tracebacks and code locations above."

    # Pluggable notification transport
    transport: Transport | None = None

    # Static delivery settings (overridden by get_setting if provided)
    delivery_enabled: bool = True
    delivery_to_email: str = ""
    delivery_from_email: str = ""
    delivery_from_name: str = "OpsAlert"
    delivery_throttle_minutes: int = 60
    delivery_digest_interval_minutes: int = 360
    retention_max_age_days: int = 90
    # A condition with zero occurrences (a degraded fire's leftover — see
    # opsalert#2) is reaped once older than this. Minutes, not days: the row
    # holds no history, the delay only shields in-flight fires.
    condition_empty_reap_minutes: int = 60

    # Optional runtime settings resolver: (key: str) → value | None
    get_setting: Callable[[str], Any] | None = None

    # Returns (trace_id, trace_origin) from the current execution context.
    # Injected by the host app so opsalert stays dependency-free.
    trace_provider: Callable[[], tuple[str | None, str | None]] | None = None

    # Returns (user_id, org_id) for whoever the current request belongs to.
    # Same shape and same contract as trace_provider: injected by the host
    # app, called on the fire path, and never allowed to break a fire. An
    # alert that names the account it happened to is an alert somebody can
    # actually reproduce.
    identity_provider: Callable[[], tuple[Any | None, Any | None]] | None = None

    # --- Ingest settings ---
    # Explicit sync database URL for the ingest writer thread. When set,
    # the writer connects with this URL. When None, the writer attempts to
    # derive a sync URL from session_factory (only for async_sessionmaker).
    # If neither works, events are counted as dropped.
    ingest_url: str | None = None
    ingest_queue_max: int = 2000
    ingest_sample_per_minute: int = 20
    ingest_batch_size: int = 100
    ingest_flush_interval_s: float = 0.25
    ingest_max_retry_s: float = 120.0


_config: OpsAlertConfig | None = None


def configure(**kwargs: Any) -> None:
    """Configure opsalert. Call once at application startup."""
    global _config
    _config = OpsAlertConfig(**kwargs)


def get_config() -> OpsAlertConfig:
    """Return current config. Raises if configure() hasn't been called."""
    if _config is None:
        raise RuntimeError(
            "opsalert.configure() must be called before using the alert API. "
            "Call it during application startup."
        )
    return _config


def _resolve_setting(key: str, default: Any = None) -> Any:
    """Resolve a setting: get_setting callback takes priority, then config attr, then default."""
    cfg = get_config()
    if cfg.get_setting is not None:
        value = cfg.get_setting(key)
        if value is not None:
            return value
    return getattr(cfg, key, default)


def reset_config() -> None:
    """Reset config to None. For testing only."""
    global _config
    _config = None


def derive_sync_url() -> str | None:
    """Derive a sync DB URL from the configured session_factory.

    Only works for ``async_sessionmaker`` — inspects its bind to get the async
    URL and swaps the driver to a sync equivalent. For plain callables
    (e.g. vingapi's ``fresh_async_session``), ``ingest_url`` must be set
    explicitly.

    asyncio is intentionally imported here (not in ingest.py).
    """

    cfg = get_config()
    factory = cfg.session_factory
    if factory is None:
        return None

    try:
        from sqlalchemy.ext.asyncio import async_sessionmaker

        if isinstance(factory, async_sessionmaker):
            bind = factory.kw.get("bind")
            if bind is not None:
                url_str = bind.url.render_as_string(hide_password=False)
                return _swap_driver(url_str)
    except (ImportError, AttributeError, Exception):
        pass

    return None


def _swap_driver(url: str) -> str:
    """Swap an async SQLAlchemy driver to its sync counterpart."""
    swaps = [
        ("mysql+asyncmy", "mysql+pymysql"),
        ("mysql+aiomysql", "mysql+pymysql"),
        ("sqlite+aiosqlite", "sqlite"),
        ("postgresql+asyncpg", "postgresql+psycopg"),
    ]
    for async_drv, sync_drv in swaps:
        if url.startswith(async_drv):
            return sync_drv + url[len(async_drv):]
    return url
