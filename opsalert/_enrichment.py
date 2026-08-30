"""Auto-enrichment — capture runtime debugging info into alert context.

Adds underscore-prefixed keys (won't collide with caller-provided data):
- _caller: module:function:line of the code that fired the alert
- _exc_type, _exc_message, _traceback: active exception info (if any)
- _task_name, _task_id: Celery task info (if running inside a task)
"""
import logging
import sys
import traceback as tb_module
from typing import Any

logger = logging.getLogger(__name__)

# A host-supplied provider that raises is a wiring defect, and one worth
# hearing about — but it fires on every alert, so it is reported ONCE per
# process and then left alone. Loud, not deafening.
_provider_failure_reported = False

# Module names to skip when walking the stack to find the caller.
# Both this module and _dispatch.py are internal to the package.
_SKIP_MODULES = frozenset({__name__, "opsalert._dispatch", "opsalert"})


_TRACEBACK_BUDGET = 2000


def _bounded_traceback(tb: Any, budget: int = _TRACEBACK_BUDGET) -> str:
    """Format a traceback within a size budget, keeping BOTH ends.

    Keeping only the tail (what a plain ``[-budget:]`` does) is the wrong half.
    A failure deep inside a library — SQLAlchemy, greenlet, a DB dialect —
    buries the application frames under thousands of characters of third-party
    frames, so a tail-only cap stores a stack that names none of our own code.
    That is precisely the case where the traceback is the only thing that says
    which endpoint or operation was running.

    The outermost frames say where the request entered; the innermost say where
    it blew up. Keep both, elide the middle, and label the elision so nobody
    reads a truncated stack as a complete one.
    """
    frames = tb_module.format_tb(tb)
    joined = "".join(frames)
    if len(joined) <= budget:
        return joined

    half = budget // 2
    head: list[str] = []
    size = 0
    for frame in frames:
        if size + len(frame) > half:
            break
        head.append(frame)
        size += len(frame)

    tail: list[str] = []
    size = 0
    for frame in reversed(frames[len(head) :]):
        if size + len(frame) > half:
            break
        tail.append(frame)
        size += len(frame)
    tail.reverse()

    elided = len(frames) - len(head) - len(tail)
    if elided <= 0:
        return joined[:budget]
    return "".join(head) + f"  ... {elided} frames elided ...\n" + "".join(tail)


def enrich_context(context: dict[str, Any] | None) -> dict[str, Any]:
    """Auto-capture runtime debugging info into alert context."""
    enriched = dict(context) if context else {}

    # --- Caller frame ---
    # Walk the stack past this package to find the actual call site.
    frame = sys._getframe()
    try:
        f = frame
        while f is not None:
            module_name = f.f_globals.get("__name__", "")
            if module_name not in _SKIP_MODULES:
                enriched["_caller"] = (
                    f"{module_name}:{f.f_code.co_name}:{f.f_lineno}"
                )
                break
            f = f.f_back
    finally:
        del frame

    # --- Active exception ---
    exc_info = sys.exc_info()
    if exc_info[1] is not None:
        enriched["_exc_type"] = type(exc_info[1]).__name__
        enriched["_exc_message"] = str(exc_info[1])[:500]
        if exc_info[2]:
            enriched["_traceback"] = _bounded_traceback(exc_info[2])

    # --- Celery task ---
    try:
        from celery import current_task

        if current_task and current_task.request:
            enriched["_task_name"] = current_task.name
            enriched["_task_id"] = current_task.request.id
    except Exception:
        pass

    # --- Request trace ---
    try:
        from opsalert._config import get_config
        cfg = get_config()
        if cfg.trace_provider is not None:
            tid, torigin = cfg.trace_provider()
            if tid is not None:
                enriched["_trace_id"] = tid
            if torigin is not None:
                enriched["_trace_origin"] = torigin
    except Exception:
        pass

    # --- Requesting identity ---
    # Optional, and never fatal: attribution is a nice-to-have, the alert is
    # not. A provider that raises (no request context, a lazy attribute that
    # hits the DB) costs the alert nothing.
    try:
        from opsalert._config import get_config
        cfg = get_config()
        if cfg.identity_provider is not None:
            user_id, org_id = cfg.identity_provider()
            if user_id is not None:
                enriched["_user_id"] = user_id
            if org_id is not None:
                enriched["_org_id"] = org_id
    except RuntimeError:
        pass  # opsalert not configured — nothing to ask
    except Exception:
        global _provider_failure_reported
        if not _provider_failure_reported:
            _provider_failure_reported = True
            logger.exception(
                "opsalert: identity_provider raised; alerts will carry no "
                "attribution until it is fixed (reported once per process)"
            )

    return enriched
