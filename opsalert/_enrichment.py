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


def compute_emit_site(stacklevel: int = 1) -> str:
    """Return "module:function" of the caller at ``stacklevel``.

    stacklevel=1 means the direct caller of the function that called this.
    stacklevel=2 means the caller's caller, etc.

    Skips opsalert internal frames first, then applies the stacklevel.
    """
    frame = sys._getframe()
    try:
        f = frame
        # First skip opsalert internal frames
        while f is not None:
            module_name = f.f_globals.get("__name__", "")
            if module_name not in _SKIP_MODULES:
                break
            f = f.f_back
        # Then skip stacklevel-1 additional frames
        for _ in range(stacklevel - 1):
            if f is not None:
                f = f.f_back
        if f is not None:
            module_name = f.f_globals.get("__name__", "")
            return f"{module_name}:{f.f_code.co_name}"
        return ""
    finally:
        del frame


def _build_structured_frames(
    tb: Any,
    in_app_prefixes: tuple[str, ...] = (),
    budget: int = 50,
) -> list[dict[str, Any]]:
    """Build structured frame list from a traceback.

    Walks tb objects directly to get real module names via
    ``tb.tb_frame.f_globals["__name__"]``.  Returns at most ``budget``
    frames, keeping in-app frames preferentially.
    """
    if tb is None:
        return []

    from opsalert.signature import _is_in_app

    result: list[dict[str, Any]] = []
    current_tb = tb
    while current_tb is not None:
        frame_obj = current_tb.tb_frame
        module = frame_obj.f_globals.get("__name__", "")
        function = frame_obj.f_code.co_name
        filename = frame_obj.f_code.co_filename
        lineno = current_tb.tb_lineno
        in_app = _is_in_app(module, filename, in_app_prefixes)
        result.append({
            "module": module,
            "function": function,
            "lineno": lineno,
            "in_app": in_app,
        })
        current_tb = current_tb.tb_next

    if len(result) <= budget:
        return result

    # Keep in-app frames preferentially + head/tail of the rest
    in_app_frames = [(i, f) for i, f in enumerate(result) if f["in_app"]]
    if len(in_app_frames) >= budget:
        return [f for _, f in in_app_frames[:budget]]

    remaining = budget - len(in_app_frames)
    in_app_indices = {i for i, _ in in_app_frames}
    other_frames = [(i, f) for i, f in enumerate(result) if i not in in_app_indices]

    # Keep head and tail of other frames
    half = remaining // 2
    kept_other = other_frames[:half] + other_frames[-(remaining - half):]
    all_kept = sorted(in_app_frames + kept_other, key=lambda x: x[0])
    return [f for _, f in all_kept]


def _build_exception_chain_for_enrichment(
    exc: BaseException | None,
) -> list[str]:
    """Build the exception chain for enrichment context."""
    from opsalert.signature import build_exception_chain

    return build_exception_chain(exc)


def enrich_context(
    context: dict[str, Any] | None,
    *,
    exc: BaseException | None = None,
    stacklevel: int = 1,
) -> dict[str, Any]:
    """Auto-capture runtime debugging info into alert context.

    ``exc`` is the explicit exception to enrich from; when None, falls back
    to ``sys.exc_info()``. ``stacklevel`` controls how many wrapper frames
    to skip for ``_emit_site``.
    """
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

    # --- Emit site (module:function only, stacklevel-aware) ---
    try:
        enriched["_emit_site"] = compute_emit_site(stacklevel=stacklevel)
    except Exception:
        pass

    # --- Active exception ---
    resolved_exc = exc
    if resolved_exc is None:
        exc_info = sys.exc_info()
        resolved_exc = exc_info[1]
    else:
        exc_info = (type(resolved_exc), resolved_exc, resolved_exc.__traceback__)

    if resolved_exc is not None:
        enriched["_exc_type"] = type(resolved_exc).__name__
        try:
            enriched["_exc_message"] = str(resolved_exc)[:500]
        except Exception:
            enriched["_exc_message"] = "<unrenderable>"
        tb = getattr(resolved_exc, "__traceback__", None) or (
            exc_info[2] if exc_info else None
        )
        if tb:
            enriched["_traceback"] = _bounded_traceback(tb)
            # Structured frames for event_json
            try:
                from opsalert._config import get_config as _gc
                _in_app = _gc().in_app_prefixes
            except (RuntimeError, ImportError):
                _in_app = ()
            enriched["_frames"] = _build_structured_frames(tb, _in_app)

        # Exception chain
        enriched["_exception_chain"] = _build_exception_chain_for_enrichment(
            resolved_exc
        )

    # --- Release from config ---
    try:
        from opsalert._config import get_config as _gc2
        _release = _gc2().release
        if _release is not None:
            enriched["_release"] = _release
    except (RuntimeError, ImportError):
        pass

    # --- Celery task ---
    try:
        from celery import current_task

        if current_task and current_task.request:
            enriched["_task_name"] = current_task.name
            enriched["_task_id"] = current_task.request.id
    except Exception:
        pass

    # --- Request trace ---
    # The trace_provider contract accepts either a 2-tuple (trace_id,
    # trace_origin) or a 3-tuple (trace_id, trace_origin, span_id) so
    # debork and vingapi can share one provider without silently losing
    # trace ids.
    try:
        from opsalert._config import get_config
        cfg = get_config()
        if cfg.trace_provider is not None:
            result = cfg.trace_provider()
            if isinstance(result, tuple):
                if len(result) >= 2:
                    tid, torigin = result[0], result[1]
                    if tid is not None:
                        enriched["_trace_id"] = tid
                    if torigin is not None:
                        enriched["_trace_origin"] = torigin
                if len(result) >= 3:
                    span_id = result[2]
                    if span_id is not None:
                        enriched["_span_id"] = span_id
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
