"""Tests for git gate hooks (pre-commit and pre-push).

Stub tests verify structural properties (skip on non-core, fail-closed on missing
tool, gitleaks-before-staged-py) in isolated temp repos with fake tools.

Real-tool tests exercise the hooks with the actual .venv toolchain against code
that triggers specific gate steps (ruff, mypy, gitleaks). The green end-to-end
path (all steps pass) is not tested here — every real gate run exercises it.
"""

import os
import random
import shutil
import stat
import string
import subprocess
import textwrap
from pathlib import Path

import pytest

# Absolute path to the repo root (two levels up from this test file).
REPO_ROOT = Path(__file__).resolve().parent.parent
PRE_COMMIT_HOOK = REPO_ROOT / "scripts" / "hooks" / "pre-commit"
PRE_PUSH_HOOK = REPO_ROOT / "scripts" / "hooks" / "pre-push"


def _make_executable(path: Path) -> None:
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def _init_repo(tmp_path: Path, branch: str = "main") -> Path:
    """Create a minimal git repo on the given branch with one commit."""
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-b", branch], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"], cwd=repo, check=True, capture_output=True
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"], cwd=repo, check=True, capture_output=True
    )
    # Initial commit so HEAD exists
    (repo / "README.md").write_text("init")
    subprocess.run(["git", "add", "README.md"], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "init", "--no-verify"],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    return repo


def _provision_fake_venv(repo: Path, exclude: str | None = None) -> None:
    """Create fake .venv/bin/ tools. If exclude is set, skip that tool name."""
    venv_bin = repo / ".venv" / "bin"
    venv_bin.mkdir(parents=True, exist_ok=True)
    for tool in ("ruff", "mypy", "uptake-lint", "pytest"):
        if tool == exclude:
            continue
        stub = venv_bin / tool
        stub.write_text("#!/bin/sh\nexit 0\n")
        _make_executable(stub)


def _copy_hook_and_gitleaks(repo: Path, hook_path: Path) -> None:
    """Copy a hook script and the gitleaks.sh into the temp repo."""
    scripts_hooks = repo / "scripts" / "hooks"
    scripts_hooks.mkdir(parents=True, exist_ok=True)
    # Copy the hook
    target = scripts_hooks / hook_path.name
    target.write_text(hook_path.read_text())
    _make_executable(target)

    # Copy gitleaks.sh (needed by pre-commit)
    scripts_gates = repo / "scripts" / "gates"
    scripts_gates.mkdir(parents=True, exist_ok=True)
    gitleaks_sh = REPO_ROOT / "scripts" / "gates" / "gitleaks.sh"
    if gitleaks_sh.exists():
        target_gl = scripts_gates / "gitleaks.sh"
        target_gl.write_text(gitleaks_sh.read_text())
        _make_executable(target_gl)

    # Minimal .gitleaks.toml
    (repo / ".gitleaks.toml").write_text('[extend]\nuseDefault = true\n')


def _stage_py_file(repo: Path) -> None:
    """Create and stage a .py file."""
    py = repo / "example.py"
    py.write_text("x = 1\n")
    subprocess.run(["git", "add", "example.py"], cwd=repo, check=True, capture_output=True)


# ── Pre-commit tests ──────────────────────────────────────────────


class TestPreCommitNonCoreBranch:
    """On a non-core branch, pre-commit exits 0 with a skip message."""

    def test_skip_on_feature_branch(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path, branch="feature-xyz")
        _copy_hook_and_gitleaks(repo, PRE_COMMIT_HOOK)
        result = subprocess.run(
            ["sh", "scripts/hooks/pre-commit"],
            cwd=repo,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "skipping gates" in result.stdout.lower()

    def test_skip_on_worker_branch(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path, branch="worker/123-slice")
        _copy_hook_and_gitleaks(repo, PRE_COMMIT_HOOK)
        result = subprocess.run(
            ["sh", "scripts/hooks/pre-commit"],
            cwd=repo,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "skipping gates" in result.stdout.lower()


class TestPreCommitMissingTool:
    """On a core branch with a missing tool, pre-commit exits 1 with message."""

    @pytest.mark.parametrize("missing_tool", ["ruff", "mypy", "uptake-lint", "pytest"])
    def test_fail_closed_missing_tool(self, tmp_path: Path, missing_tool: str) -> None:
        repo = _init_repo(tmp_path, branch="integration")
        _copy_hook_and_gitleaks(repo, PRE_COMMIT_HOOK)
        _provision_fake_venv(repo, exclude=missing_tool)
        _stage_py_file(repo)

        # Provide a fake gitleaks in PATH so the gitleaks step passes
        fake_bin = tmp_path / "fakebin"
        fake_bin.mkdir()
        fake_gitleaks = fake_bin / "gitleaks"
        fake_gitleaks.write_text("#!/bin/sh\nexit 0\n")
        _make_executable(fake_gitleaks)

        env = {**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}"}
        result = subprocess.run(
            ["sh", "scripts/hooks/pre-commit"],
            cwd=repo,
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 1
        assert "~/opsalert-landing" in result.stdout, (
            f"Expected provisioned-tree message mentioning ~/opsalert-landing, "
            f"got: {result.stdout!r}"
        )


class TestPreCommitGitleaksBeforeStagedPy:
    """Gitleaks step runs before the staged-py short-circuit."""

    def test_gitleaks_runs_even_without_staged_py(self, tmp_path: Path) -> None:
        """When no .py files are staged, gitleaks should still run (and be recorded)."""
        repo = _init_repo(tmp_path, branch="integration")
        _copy_hook_and_gitleaks(repo, PRE_COMMIT_HOOK)
        _provision_fake_venv(repo)

        # Stage a non-py file only
        txt = repo / "notes.txt"
        txt.write_text("hello\n")
        subprocess.run(["git", "add", "notes.txt"], cwd=repo, check=True, capture_output=True)

        # Create a recording gitleaks stub that logs its invocation
        invocation_log = tmp_path / "gitleaks_invoked"
        scripts_gates = repo / "scripts" / "gates"
        gitleaks_sh = scripts_gates / "gitleaks.sh"
        gitleaks_sh.write_text(textwrap.dedent(f"""\
            #!/bin/sh
            echo "gitleaks invoked with: $1" > {invocation_log}
            exit 0
        """))
        _make_executable(gitleaks_sh)

        result = subprocess.run(
            ["sh", "scripts/hooks/pre-commit"],
            cwd=repo,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert invocation_log.exists(), "gitleaks was not invoked before staged-py check"
        content = invocation_log.read_text()
        assert "staged" in content

    def test_gitleaks_failure_blocks_commit(self, tmp_path: Path) -> None:
        """When gitleaks fails, the hook exits 1 even without staged py files."""
        repo = _init_repo(tmp_path, branch="integration")
        _copy_hook_and_gitleaks(repo, PRE_COMMIT_HOOK)
        _provision_fake_venv(repo)

        # Stage a non-py file
        txt = repo / "notes.txt"
        txt.write_text("hello\n")
        subprocess.run(["git", "add", "notes.txt"], cwd=repo, check=True, capture_output=True)

        # Failing gitleaks stub
        scripts_gates = repo / "scripts" / "gates"
        gitleaks_sh = scripts_gates / "gitleaks.sh"
        gitleaks_sh.write_text("#!/bin/sh\nexit 1\n")
        _make_executable(gitleaks_sh)

        result = subprocess.run(
            ["sh", "scripts/hooks/pre-commit"],
            cwd=repo,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1


# ── Pre-push tests ────────────────────────────────────────────────


class TestPrePushNonCoreBranch:
    """Pre-push exits 0 with skip message when no core ref is being pushed."""

    def test_skip_on_feature_ref(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path, branch="feature-xyz")
        _copy_hook_and_gitleaks(repo, PRE_PUSH_HOOK)

        # Simulate stdin: pushing to refs/heads/feature-xyz
        result = subprocess.run(
            ["sh", "scripts/hooks/pre-push", "origin", "https://example.com"],
            cwd=repo,
            capture_output=True,
            text=True,
            input="refs/heads/feature-xyz abc123 refs/heads/feature-xyz def456\n",
        )
        assert result.returncode == 0
        assert "skipping gates" in result.stdout.lower()


class TestPrePushMissingTool:
    """On a core push with a missing tool, pre-push exits 1."""

    @pytest.mark.parametrize("missing_tool", ["ruff", "mypy", "pytest"])
    def test_fail_closed_missing_tool(self, tmp_path: Path, missing_tool: str) -> None:
        repo = _init_repo(tmp_path, branch="integration")
        _copy_hook_and_gitleaks(repo, PRE_PUSH_HOOK)
        _provision_fake_venv(repo, exclude=missing_tool)

        result = subprocess.run(
            ["sh", "scripts/hooks/pre-push", "origin", "https://example.com"],
            cwd=repo,
            capture_output=True,
            text=True,
            input="refs/heads/integration abc123 refs/heads/integration def456\n",
        )
        assert result.returncode == 1
        assert "~/opsalert-landing" in result.stdout, (
            f"Expected provisioned-tree message mentioning ~/opsalert-landing, "
            f"got: {result.stdout!r}"
        )


class TestPrePushCoreRefDetection:
    """Pre-push gates only when a core ref is in the push."""

    def test_gates_on_integration_ref(self, tmp_path: Path) -> None:
        """Pushing to integration triggers gating (fails due to missing tools)."""
        repo = _init_repo(tmp_path, branch="integration")
        _copy_hook_and_gitleaks(repo, PRE_PUSH_HOOK)
        # No .venv -> should fail closed
        result = subprocess.run(
            ["sh", "scripts/hooks/pre-push", "origin", "https://example.com"],
            cwd=repo,
            capture_output=True,
            text=True,
            input="refs/heads/integration abc123 refs/heads/integration def456\n",
        )
        assert result.returncode == 1

    def test_gates_on_staging_ref(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path, branch="staging")
        _copy_hook_and_gitleaks(repo, PRE_PUSH_HOOK)
        result = subprocess.run(
            ["sh", "scripts/hooks/pre-push", "origin", "https://example.com"],
            cwd=repo,
            capture_output=True,
            text=True,
            input="refs/heads/staging abc123 refs/heads/staging def456\n",
        )
        assert result.returncode == 1

    def test_gates_on_main_ref(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path, branch="main")
        _copy_hook_and_gitleaks(repo, PRE_PUSH_HOOK)
        result = subprocess.run(
            ["sh", "scripts/hooks/pre-push", "origin", "https://example.com"],
            cwd=repo,
            capture_output=True,
            text=True,
            input="refs/heads/main abc123 refs/heads/main def456\n",
        )
        assert result.returncode == 1


# ── Real-tool tests ──────────────────────────────────────────────


@pytest.fixture()
def real_tree(tmp_path: Path) -> Path:
    """Set up a temp git repo with the real toolchain for hook testing.

    Copies scripts/, pyproject.toml, opsalert/, .gitleaks.toml into a fresh
    git repo, symlinks the real .venv, runs install.sh, and checks out
    integration.
    """
    repo = tmp_path / "real"
    repo.mkdir()
    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"], cwd=repo, check=True, capture_output=True
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"], cwd=repo, check=True, capture_output=True
    )

    # Copy repo contents
    for item in ("scripts", "opsalert", "pyproject.toml", ".gitleaks.toml"):
        src = REPO_ROOT / item
        dst = repo / item
        if src.is_dir():
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)

    # Symlink the real .venv
    (repo / ".venv").symlink_to(REPO_ROOT / ".venv")

    # Initial commit
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "init", "--no-verify"],
        cwd=repo, check=True, capture_output=True,
    )

    # Install hooks
    subprocess.run(
        ["bash", "scripts/hooks/install.sh"], cwd=repo, check=True, capture_output=True
    )

    # Create and switch to integration
    subprocess.run(
        ["git", "checkout", "-b", "integration"],
        cwd=repo, check=True, capture_output=True,
    )
    return repo


class TestRealToolPreCommit:
    """Pre-commit with real .venv tools exercising actual gate steps."""

    def test_ruff_catches_unused_import(self, real_tree: Path) -> None:
        """Staged file with unused import -> exit 1, ruff message, no mypy/pytest."""
        bad = real_tree / "bad_import.py"
        bad.write_text("import json\n")
        subprocess.run(
            ["git", "add", "bad_import.py"], cwd=real_tree, check=True, capture_output=True
        )

        result = subprocess.run(
            ["sh", "scripts/hooks/pre-commit"],
            cwd=real_tree, capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 1
        assert "ruff found issues" in result.stdout
        # mypy and pytest should not have run (ruff short-circuits)
        assert "running mypy" not in result.stdout
        assert "running pytest" not in result.stdout

    def test_mypy_catches_type_error(self, real_tree: Path) -> None:
        """Staged file clean for ruff but with type error -> exit 1, mypy message."""
        bad = real_tree / "opsalert" / "_bad_types.py"
        bad.write_text("x: int = 's'\n")
        subprocess.run(
            ["git", "add", "opsalert/_bad_types.py"],
            cwd=real_tree, check=True, capture_output=True,
        )

        result = subprocess.run(
            ["sh", "scripts/hooks/pre-commit"],
            cwd=real_tree, capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 1
        assert "mypy found type errors" in result.stdout
        # pytest should not have run (mypy short-circuits)
        assert "running pytest" not in result.stdout

    def test_gitleaks_catches_fake_aws_key(self, real_tree: Path) -> None:
        """Staged file with a runtime-constructed AWS key -> exit 1, gitleaks message."""
        # Generate a fake AKIA key at runtime (never a literal in test source)
        suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=16))
        fake_key = "AKIA" + suffix
        secret_file = real_tree / "leaked.py"
        secret_file.write_text(f'AWS_KEY = "{fake_key}"\n')
        subprocess.run(
            ["git", "add", "leaked.py"], cwd=real_tree, check=True, capture_output=True
        )

        result = subprocess.run(
            ["sh", "scripts/hooks/pre-commit"],
            cwd=real_tree, capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 1
        assert "gitleaks found a secret" in result.stdout


class TestRealToolPrePush:
    """Pre-push with real .venv tools exercising actual gate steps."""

    def test_ruff_violation_blocks_push(self, real_tree: Path) -> None:
        """A ruff violation in the tree blocks a push to integration."""
        bad = real_tree / "opsalert" / "_bad.py"
        bad.write_text("import json\n")
        subprocess.run(
            ["git", "add", str(bad)], cwd=real_tree, check=True, capture_output=True
        )
        subprocess.run(
            ["git", "commit", "-m", "bad", "--no-verify"],
            cwd=real_tree, check=True, capture_output=True,
        )

        head_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=real_tree, capture_output=True, text=True, check=True,
        ).stdout.strip()

        result = subprocess.run(
            ["sh", "scripts/hooks/pre-push", "origin", "https://example.com"],
            cwd=real_tree, capture_output=True, text=True, timeout=60,
            input=f"refs/heads/integration {head_sha} refs/heads/integration 0000\n",
        )
        assert result.returncode == 1
        assert "ruff found issues" in result.stdout
