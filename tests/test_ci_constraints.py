"""CI installs the versions prod runs; drift is a weekly warning (opsalert#34).

No DB, no async fixtures. The sync script is not part of the package, so it is
loaded from its path; workflows are read as text (no YAML dependency).
"""

from __future__ import annotations

import importlib.util
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "sync_ci_constraints.py"
CONSTRAINTS = REPO_ROOT / "ci-constraints.txt"
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
LATEST = WORKFLOWS / "validate-latest.yml"

PIN = re.compile(r"^[A-Za-z0-9_.\-]+==\S+$")


def _load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("sync_ci_constraints", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _norm(name: str) -> str:
    return name.lower().replace("_", "-")


def _req_name(requirement: str) -> str:
    return _norm(re.split(r"[\[<>=!~;\s]", requirement, maxsplit=1)[0])


def _pyproject() -> dict:
    return tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def _constraint_pins() -> list[str]:
    return [
        line
        for line in CONSTRAINTS.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]


def _pip_install_lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if "pip install" in line]


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        check=False,
    )


# The shapes of vingapi's requirements.lock: pins, three editable private pins
# (debork / opsalert / vingapi), plus the kinds of lines that must be dropped.
FIXTURE_LOCK = """\
aiosqlite==0.22.1
-e git+ssh://git@github.com/vingapp/debork.git@f188ae1#egg=debork
# a comment
greenlet==3.3.1

-e git+ssh://git@github.com/vingapp/opsalert.git@f4b19a6#egg=opsalert
--index-url https://pypi.org/simple
--hash=sha256:0000
SQLAlchemy==2.0.46
-e git+ssh://git@github.com/vingapp/vingapi.git@c4945bc#egg=vingapi
typing_extensions==4.15.0
"""

FIXTURE_PINS = [
    "aiosqlite==0.22.1",
    "greenlet==3.3.1",
    "SQLAlchemy==2.0.46",
    "typing_extensions==4.15.0",
]


def test_reported_34_dev_extra_declares_greenlet() -> None:
    """SQLAlchemy 2.1 installs greenlet only with [asyncio]; the tests build async
    engines, so the dev extra must ask for it or conftest fails at import."""
    dev = _pyproject()["project"]["optional-dependencies"]["dev"]
    assert any(_req_name(req) == "greenlet" for req in dev), dev


def test_runtime_dependencies_are_sqlalchemy_only() -> None:
    deps = _pyproject()["project"]["dependencies"]
    assert len(deps) == 1, deps
    (dep,) = deps
    assert _req_name(dep) == "sqlalchemy", dep
    assert "[" not in dep, f"no extras on the runtime dependency: {dep}"
    assert "<" not in dep, f"no upper bound on the runtime dependency: {dep}"


def test_sync_ci_constraints_excludes_editable_lines() -> None:
    module = _load_script()
    assert module.third_party_pins(FIXTURE_LOCK) == FIXTURE_PINS


def test_sync_ci_constraints_check_fails_on_drift(tmp_path: Path) -> None:
    lock = tmp_path / "requirements.lock"
    lock.write_text(FIXTURE_LOCK, encoding="utf-8")
    out = tmp_path / "ci-constraints.txt"

    written = _run_cli(str(lock), "--sha", "abc1234", "--out", str(out))
    assert written.returncode == 0, written.stderr
    assert out.is_file()

    match = _run_cli(str(lock), "--sha", "abc1234", "--check", "--out", str(out))
    assert match.returncode == 0, match.stdout + match.stderr

    lock.write_text(FIXTURE_LOCK.replace("greenlet==3.3.1", "greenlet==3.4.0"), encoding="utf-8")
    before = out.read_text(encoding="utf-8")
    drift = _run_cli(str(lock), "--sha", "abc1234", "--check", "--out", str(out))
    assert drift.returncode == 1, drift.stdout + drift.stderr
    assert "greenlet" in drift.stdout
    assert out.read_text(encoding="utf-8") == before, "--check must write nothing"

    missing = _run_cli(
        str(tmp_path / "nope.lock"), "--sha", "abc1234", "--check", "--out", str(out)
    )
    assert missing.returncode == 2
    assert "not found" in missing.stderr


def test_ci_constraints_has_no_editable_or_unpinned_lines() -> None:
    pins = _constraint_pins()
    bad = [line for line in pins if not PIN.match(line)]
    assert not bad, bad
    names = {_norm(line.split("==", 1)[0]) for line in pins}
    for required in ("sqlalchemy", "greenlet", "aiosqlite", "pytest", "pytest-asyncio"):
        assert required in names, f"{required} missing from {CONSTRAINTS.name}"


@pytest.mark.skipif(
    not os.environ.get("VINGAPI_LOCK"),
    reason="set VINGAPI_LOCK to the path of vingapi's requirements.lock to compare pins",
)
def test_ci_constraints_match_vingapi_lock_third_party_pins() -> None:
    lock = Path(os.environ["VINGAPI_LOCK"])
    assert lock.is_file(), f"VINGAPI_LOCK is not a file: {lock}"
    module = _load_script()
    assert _constraint_pins() == module.third_party_pins(lock.read_text(encoding="utf-8"))


def test_gating_workflows_install_with_ci_constraints() -> None:
    gating = {
        path.name: path.read_text(encoding="utf-8")
        for path in sorted(WORKFLOWS.glob("*.y*ml"))
        if "pull_request" in path.read_text(encoding="utf-8")
        or "merge_group" in path.read_text(encoding="utf-8")
    }
    assert {"validate.yml", "validate-light.yml"} <= gating.keys(), sorted(gating)
    for name, text in gating.items():
        installs = [
            line for line in _pip_install_lines(text) if "install --upgrade pip" not in line
        ]
        assert installs, f"{name}: no pip install line found"
        unconstrained = [line for line in installs if "-c ci-constraints.txt" not in line]
        assert not unconstrained, f"{name}: {unconstrained}"


def test_validate_latest_never_gates_and_is_unconstrained() -> None:
    text = LATEST.read_text(encoding="utf-8")
    assert "schedule" in text
    assert "pull_request" not in text
    assert "merge_group" not in text
    installs = _pip_install_lines(text)
    assert installs, "validate-latest.yml has no pip install line"
    assert not [line for line in installs if "-c ci-constraints.txt" in line]
