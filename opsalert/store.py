"""Store — create one alert row per occurrence.

Every call creates a new Alert record. No deduplication at the data layer;
grouping is done at query time via ``category`` and ``message`` fields.
"""
import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from opsalert.model import Alert

logger = logging.getLogger(__name__)

# ``Alert.context_json`` is MySQL TEXT — 65535 *bytes*, not characters. An
# oversized context used to raise DataError 1406 mid-flush, which loses the
# whole alert: the record explaining what went wrong is dropped precisely when
# the failure was big enough to produce a huge context. Cap it instead, so a fat
# context costs detail and never the alert.
CONTEXT_MAX_BYTES = 60_000  # headroom under TEXT for the truncation markers
# Long values are cut to this before being replaced wholesale, so a truncated
# stack trace still shows where it started.
_VALUE_PREVIEW_BYTES = 2_000
# Fallback for a context that is mostly structure: how many key names to keep,
# and how long each may be. The key list has to fit the column as well.
_KEY_SAMPLE = 200
_KEY_PREVIEW_BYTES = 100


def _encoded_len(payload: str) -> int:
    return len(payload.encode("utf-8"))


def _truncate_str(value: str, limit: int) -> str:
    """Cut ``value`` to ``limit`` bytes without splitting a UTF-8 sequence."""
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value
    return encoded[:limit].decode("utf-8", errors="ignore")


def serialize_context(context: dict[str, Any] | None) -> str | None:
    """JSON-encode an alert context, capped to fit ``Alert.context_json``.

    Under the cap the context round-trips byte for byte. Over it, the biggest
    values are cut down (largest first) until the payload fits, and the result
    carries ``_truncated`` — the keys that lost data — plus ``_original_bytes``
    so a reader can tell how much was dropped. If shrinking values still isn't
    enough (a context that is mostly structure rather than a few long strings),
    fall back to a marker object listing the keys that were present.
    """
    if not context:
        return None

    serialized = json.dumps(context)
    original_bytes = _encoded_len(serialized)
    if original_bytes <= CONTEXT_MAX_BYTES:
        return serialized

    # Size every value once, then cut the oversized ones down in one pass —
    # largest first, stopping as soon as the running total fits. Re-dumping the
    # whole dict per candidate would be quadratic, and a context big enough to
    # land here is exactly the one that can carry thousands of keys.
    sizes = {key: _encoded_len(json.dumps(value, default=str)) for key, value in context.items()}
    capped: dict[str, Any] = dict(context)
    truncated_keys: list[str] = []
    running = original_bytes

    for key in sorted(sizes, key=lambda k: sizes[k], reverse=True):
        if running <= _budget(truncated_keys, original_bytes):
            break
        if sizes[key] <= _VALUE_PREVIEW_BYTES:
            break  # nothing bigger left to reclaim
        value = capped[key]
        if not isinstance(value, str):
            value = json.dumps(value, default=str)
        capped[key] = _truncate_str(value, _VALUE_PREVIEW_BYTES)
        truncated_keys.append(key)
        running -= sizes[key] - _encoded_len(json.dumps(capped[key]))

    if truncated_keys:
        candidate = json.dumps(
            {**capped, "_truncated": truncated_keys, "_original_bytes": original_bytes}
        )
        if _encoded_len(candidate) <= CONTEXT_MAX_BYTES:
            logger.warning(
                "opsalert: context exceeded %d bytes (%d); truncated keys %s",
                CONTEXT_MAX_BYTES,
                original_bytes,
                truncated_keys,
            )
            return candidate

    # Still too big with every long value cut down — the bulk is structure, not
    # a few fat strings. Keep the shape (a bounded sample of keys) and drop the
    # data; the key list itself has to fit the column too.
    keys = sorted(context)
    sample = keys[:_KEY_SAMPLE]
    logger.warning(
        "opsalert: context exceeded %d bytes (%d) and could not be shrunk by "
        "value; storing key sample only (%d keys)",
        CONTEXT_MAX_BYTES,
        original_bytes,
        len(keys),
    )
    return json.dumps(
        {
            "_truncated": [_truncate_str(k, _KEY_PREVIEW_BYTES) for k in sample],
            "_key_count": len(keys),
            "_original_bytes": original_bytes,
            "_dropped": True,
        }
    )


def _budget(truncated_keys: list[str], original_bytes: int) -> int:
    """Byte ceiling for the capped values, leaving room for the markers."""
    marker_bytes = _encoded_len(json.dumps(truncated_keys)) + len(str(original_bytes)) + 64
    return CONTEXT_MAX_BYTES - marker_bytes


async def fire_alert(
    session: "AsyncSession",
    *,
    severity: str,
    category: str,
    message: str,
    source: str | None = None,
    context: dict[str, Any] | None = None,
) -> Alert:
    """Create an alert record. Every call creates one row."""
    alert = Alert(
        severity=severity,
        category=category,
        message=message,
        source=source,
        context_json=serialize_context(context),
    )
    session.add(alert)
    await session.flush()
    return alert
