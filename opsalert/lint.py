"""Lint helper — AST scan for fire sites without a static ``kind=``.

For V4/D1 to call from their suites::

    from opsalert.lint import scan_fire_sites
    assert scan_fire_sites(["src/"], in_app_prefix="src.") == []
"""
import ast
import re
from dataclasses import dataclass
from pathlib import Path

_KIND_RE = re.compile(r"^[a-z0-9_]+(\.[a-z0-9_]+)+$")

# Function names we scan for
_FIRE_NAMES = frozenset({"warn", "error", "critical"})
# Attribute patterns: opsalert.warn, opsalert.error, etc.
_ATTR_MODULES = frozenset({"opsalert"})
# Also catch _alerts.fire pattern
_FIRE_ATTR_NAMES = frozenset({"fire"})
_FIRE_ATTR_MODULES = frozenset({"_alerts"})


@dataclass(frozen=True)
class Finding:
    """One lint finding."""

    path: str
    line: int
    message: str


def scan_fire_sites(
    paths: list[str],
    in_app_prefix: str,
) -> list[Finding]:
    """AST scan for ``opsalert.warn|error|critical(`` and ``_alerts.fire(``
    calls without a static ``kind=`` string, or with an invalid one.

    ``paths`` are file paths (not directories). Returns a list of findings.
    """
    findings: list[Finding] = []

    for path_str in paths:
        path = Path(path_str)
        if path.is_dir():
            for py_file in path.rglob("*.py"):
                findings.extend(_scan_file(str(py_file)))
        elif path.suffix == ".py" and path.exists():
            findings.extend(_scan_file(str(path)))

    return findings


def _scan_file(path: str) -> list[Finding]:
    """Scan a single file for fire-site findings."""
    try:
        source = Path(path).read_text(encoding="utf-8")
        tree = ast.parse(source, filename=path)
    except (SyntaxError, OSError):
        return []

    findings: list[Finding] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        if not _is_fire_call(node):
            continue

        # Check for kind= keyword argument
        kind_arg = None
        for kw in node.keywords:
            if kw.arg == "kind":
                kind_arg = kw
                break

        if kind_arg is None:
            findings.append(Finding(
                path=path,
                line=node.lineno,
                message="Missing kind= argument on opsalert fire call",
            ))
            continue

        # Check that kind= is a static string constant
        if isinstance(kind_arg.value, ast.Constant) and isinstance(
            kind_arg.value.value, str
        ):
            kind_value = kind_arg.value.value
            if not _KIND_RE.match(kind_value):
                findings.append(Finding(
                    path=path,
                    line=node.lineno,
                    message=(
                        f"Invalid kind={kind_value!r}: "
                        "must match ^[a-z0-9_]+(\\.[a-z0-9_]+)+$"
                    ),
                ))
        elif not isinstance(kind_arg.value, ast.Constant):
            # Non-constant kind= — could be a variable, which we flag
            findings.append(Finding(
                path=path,
                line=node.lineno,
                message="kind= must be a static string literal, not a variable",
            ))

    return findings


def _is_fire_call(node: ast.Call) -> bool:
    """Check if a Call node is an opsalert fire call or _alerts.fire call."""
    func = node.func

    # opsalert.warn(...), opsalert.error(...), opsalert.critical(...)
    if isinstance(func, ast.Attribute):
        if isinstance(func.value, ast.Name):
            if func.value.id in _ATTR_MODULES and func.attr in _FIRE_NAMES:
                return True
            if func.value.id in _FIRE_ATTR_MODULES and func.attr in _FIRE_ATTR_NAMES:
                return True

    return False
