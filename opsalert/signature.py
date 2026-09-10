"""Condition identity — what makes two occurrences "the same problem".

Identity is ``hash(category, source, environment, template)``. The template is
either explicit (the caller passed ``params``, so the un-rendered message IS
the template) or derived from the message by :func:`normalize_message`.

The normalizer is deliberately CONSERVATIVE. Over-merging is the expensive
mistake: two genuinely different failures collapsed into one condition means
acknowledging one silences the other, and nobody ever learns the second one
exists. Under-merging only costs a longer list. So it replaces exactly the
things that are unambiguously per-occurrence data — timestamps, numbers,
uuids, long hex ids, quoted literals — and leaves everything else alone.
Emit sites that need more (an opaque url slug, say) migrate to ``params``,
where identity is exact rather than guessed.
"""
import hashlib
import logging
import re

logger = logging.getLogger(__name__)

# Order matters: the wider patterns run first so a narrower one cannot eat a
# fragment of them (a datetime is full of numbers; a quoted string can hold a
# uuid). Every substitution is a fixed placeholder, so re-normalizing an
# already-normalized message is a no-op.
_ISO_DATETIME = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?"
)
_ISO_DATE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
_QUOTED = re.compile(r"'[^']*'|\"[^\"]*\"")
_UUID = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
# A hex id: 8+ hex characters holding BOTH a digit and a hex letter. The double
# requirement is what keeps ordinary English words ("accessed", "defaced") and
# plain integers out of it — a pure-digit run is the number rule's job.
_HEX_ID = re.compile(r"\b(?=[0-9a-fA-F]*[0-9])(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{8,}\b")
_NUMBER = re.compile(r"(?<![\w<])\d+(?:\.\d+)?")

# Templates are stored in a String(500) column; identity must never depend on
# bytes the column cannot hold.
TEMPLATE_MAX_CHARS = 500


def normalize_message(message: str) -> str:
    """Derive a stable template from a free-text alert message.

    Multi-line messages collapse to their FIRST line for identity purposes.
    The tail of a multi-line message is where the per-occurrence noise lives —
    the SQL statement, the bound parameters, the stack — and prod's
    ``manager_failure`` deadlocks differ only there. The first line is the
    sentence a human would use to name the problem.
    """
    if not message:
        return ""

    template = message.splitlines()[0].strip()
    template = _ISO_DATETIME.sub("<ts>", template)
    template = _ISO_DATE.sub("<date>", template)
    template = _QUOTED.sub("<str>", template)
    template = _UUID.sub("<uuid>", template)
    template = _HEX_ID.sub("<hex>", template)
    template = _NUMBER.sub("<n>", template)
    # Collapse runs of whitespace so "a  b" and "a b" are one condition.
    template = re.sub(r"\s+", " ", template).strip()
    return template[:TEMPLATE_MAX_CHARS]


def condition_signature(
    category: str,
    source: str | None,
    environment: str | None,
    template: str,
) -> str:
    """Return the 64-char hex identity key for a condition.

    Environment is part of the identity (P9): the same failure in staging and
    in production is two conditions, so resolving the staging one does not
    silence production.

    The parts are joined with a separator that cannot appear in a hashed field
    unescaped, so ``("a|b", "c")`` and ``("a", "b|c")`` cannot collide.
    """
    parts = [
        category or "",
        source or "",
        environment or "",
        (template or "")[:TEMPLATE_MAX_CHARS],
    ]
    payload = "\x1f".join(part.replace("\x1f", " ") for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


_KIND_RE = re.compile(r"^[a-z0-9_]+(\.[a-z0-9_]+)+$")


def validate_kind(kind: str) -> bool:
    """Return True if ``kind`` is a valid dotted stable noun."""
    return bool(_KIND_RE.match(kind))


def event_signature(
    *,
    kind: str,
    environment: str | None,
    exception_chain: list[str],
    origin_frame: str,
    template: str | None = None,
) -> str:
    """V2 identity key.

    ``template`` is included ONLY for legacy fallback (``kind`` ending in
    ``.legacy``) so that two messages that v1 kept separate remain separate.

    Returns the same 64-char hex key format as ``condition_signature``.
    """
    parts = ["2", kind, environment or "", *exception_chain, origin_frame]
    if template is not None:
        parts.append((template or "")[:TEMPLATE_MAX_CHARS])
    payload = "\x1f".join(part.replace("\x1f", " ") for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def event_fingerprint_parts(
    *,
    kind: str,
    environment: str | None,
    exception_chain: list[str],
    origin_frame: str,
    template: str | None = None,
) -> list[str]:
    """Return the parts list for ``fingerprint_json``."""
    parts = ["2", kind, environment or "", *exception_chain, origin_frame]
    if template is not None:
        parts.append((template or "")[:TEMPLATE_MAX_CHARS])
    return parts


def build_exception_chain(
    exc: BaseException | None,
    *,
    max_depth: int = 4,
) -> list[str]:
    """Build the exception chain for identity, innermost first.

    Each entry is ``"ClassName"`` or ``"ClassName:<errno>"`` for DBAPI errors
    (errno from ``exc.orig.args[0]`` when int).

    Never raises — a broken ``exc`` object still yields a (possibly empty) list.
    """
    if exc is None:
        return []

    try:
        chain: list[str] = []
        current: BaseException | None = exc
        seen: set[int] = set()
        while current is not None and len(chain) < max_depth:
            if id(current) in seen:
                break
            seen.add(id(current))
            name = _exc_label(current)
            chain.append(name)
            current = getattr(current, "__cause__", None) or getattr(
                current, "__context__", None
            )
        # Innermost first — the chain is built outer→inner, reverse it
        chain.reverse()
        return chain
    except Exception:
        try:
            return [type(exc).__name__]
        except Exception:
            return []


def _exc_label(exc: BaseException) -> str:
    """Label for one exception in the chain."""
    try:
        name = type(exc).__name__
    except Exception:
        return "Unknown"

    # DBAPI errno extraction: exc.orig.args[0] when int
    try:
        orig = getattr(exc, "orig", None)
        if orig is not None:
            args = getattr(orig, "args", None)
            if args and isinstance(args[0], int):
                return f"{name}:{args[0]}"
    except Exception:
        pass

    # Also check pymysql/asyncmy style: direct args[0] int
    try:
        args = getattr(exc, "args", None)
        if args and isinstance(args[0], int):
            # Only for known DBAPI exception families
            mod = type(exc).__module__ or ""
            if any(prefix in mod for prefix in ("pymysql", "asyncmy", "MySQLdb", "mysql")):
                return f"{name}:{args[0]}"
    except Exception:
        pass

    return name


def extract_origin_frame(
    exc: BaseException | None,
    *,
    in_app_prefixes: tuple[str, ...] = (),
) -> str:
    """Return ``"module:function"`` of the innermost in-app frame.

    Walks the traceback objects directly via ``tb.tb_frame.f_globals["__name__"]``
    to get the real module name -- never guesses from the file path.

    In-app = module name starts with any of ``in_app_prefixes``. When empty,
    in-app = not stdlib/site-packages (heuristic via ``sysconfig`` paths on
    the frame's filename).

    Returns ``""`` when no exception or no in-app frame.
    Never raises.
    """
    if exc is None:
        return ""

    try:
        # Walk to the innermost exception in the chain
        innermost = exc
        seen: set[int] = set()
        while True:
            if id(innermost) in seen:
                break
            seen.add(id(innermost))
            cause = getattr(innermost, "__cause__", None) or getattr(
                innermost, "__context__", None
            )
            if cause is None:
                break
            innermost = cause

        tb = getattr(innermost, "__traceback__", None)
        if tb is None:
            return ""

        # Walk tb objects to collect (module, function, filename) tuples
        frames: list[tuple[str, str, str]] = []
        current_tb = tb
        while current_tb is not None:
            frame = current_tb.tb_frame
            module = frame.f_globals.get("__name__", "")
            function = frame.f_code.co_name
            filename = frame.f_code.co_filename
            frames.append((module, function, filename))
            current_tb = current_tb.tb_next

        if not frames:
            return ""

        # Find the innermost in-app frame (walk from the bottom)
        for module, function, filename in reversed(frames):
            if _is_in_app(module, filename, in_app_prefixes):
                return f"{module}:{function}"

        return ""
    except Exception:
        return ""


def _is_in_app(
    module: str, filename: str, in_app_prefixes: tuple[str, ...]
) -> bool:
    """Check if a frame is in-app."""
    if in_app_prefixes:
        return any(module.startswith(prefix) for prefix in in_app_prefixes)
    # Heuristic: not stdlib/site-packages
    import sysconfig

    stdlib_paths = set()
    for name in ("stdlib", "platstdlib", "purelib", "platlib"):
        path = sysconfig.get_path(name)
        if path:
            stdlib_paths.add(path)
    return not any(filename.startswith(p) for p in stdlib_paths)


class _SafeFormatDict(dict):
    """Leaves ``{missing}`` placeholders in place instead of raising."""

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def render_template(template: str, params: dict | None) -> str:
    """Render ``template`` with ``params``, never raising.

    A rendering bug must not be able to lose the alert (the hard contract), and
    it must not be able to change the alert's *identity* either — identity is
    the raw template, which is unaffected by anything that happens here. A
    missing key renders as its own placeholder so the reader can see what the
    emit site failed to supply.
    """
    if not params:
        return template
    try:
        return template.format_map(_SafeFormatDict(params))
    except Exception:  # unbalanced braces, bad format spec, weird __format__
        logger.exception(
            "opsalert: could not render alert template %r; storing it un-rendered",
            template[:120],
        )
        return template
