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

    # Optional runtime settings resolver: (key: str) → value | None
    get_setting: Callable[[str], Any] | None = None

    # Returns (trace_id, trace_origin) from the current execution context.
    # Injected by the host app so opsalert stays dependency-free.
    trace_provider: Callable[[], tuple[str | None, str | None]] | None = None


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
