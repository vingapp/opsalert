#!/usr/bin/env bash
# scripts/gates/gitleaks.sh — secret scanning via gitleaks
#
# Subcommands:
#   ensure  — install the pinned gitleaks binary (idempotent)
#   staged  — ensure + scan staged changes (pre-commit hook)
#   ci      — ensure + tree scan + PR-range commit scan (CI workflow)
#
# Fails loudly on any error; never falls back to "skip".

set -euo pipefail

# ── Pinned release ──────────────────────────────────────────────────
GITLEAKS_VERSION="8.30.1"
GITLEAKS_SHA256="551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb"
GITLEAKS_URL="https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}/gitleaks_${GITLEAKS_VERSION}_linux_x64.tar.gz"

# Where to put the binary.  Default = .venv/bin (repo-local, gitignored).
# CI sets GITLEAKS_BIN_DIR to $RUNNER_TEMP/bin (no .venv in a hosted runner).
BIN_DIR="${GITLEAKS_BIN_DIR:-.venv/bin}"
GITLEAKS="${BIN_DIR}/gitleaks"

# ── ensure ──────────────────────────────────────────────────────────
ensure() {
    # Fast path: binary exists and is the right version.
    if [ -x "$GITLEAKS" ]; then
        installed=$("$GITLEAKS" version 2>/dev/null || true)
        if [ "$installed" = "$GITLEAKS_VERSION" ]; then
            return 0
        fi
    fi

    echo "gitleaks: installing v${GITLEAKS_VERSION} → ${GITLEAKS}"
    mkdir -p "$BIN_DIR"

    tmpdir=$(mktemp -d)
    trap 'rm -rf "$tmpdir"' EXIT

    curl -fsSL -o "${tmpdir}/gitleaks.tar.gz" "$GITLEAKS_URL"

    # Verify sha256 checksum against the release's checksums.txt value.
    if ! echo "${GITLEAKS_SHA256}  ${tmpdir}/gitleaks.tar.gz" | sha256sum -c --strict - >/dev/null 2>&1; then
        echo "gitleaks: CHECKSUM MISMATCH — download is corrupted or tampered."
        echo "  expected: ${GITLEAKS_SHA256}"
        echo "  got:      $(sha256sum "${tmpdir}/gitleaks.tar.gz" | awk '{print $1}')"
        exit 1
    fi

    tar xzf "${tmpdir}/gitleaks.tar.gz" -C "${tmpdir}" gitleaks
    mv "${tmpdir}/gitleaks" "$GITLEAKS"
    chmod +x "$GITLEAKS"

    # Confirm the installed version matches.
    installed=$("$GITLEAKS" version 2>/dev/null || true)
    if [ "$installed" != "$GITLEAKS_VERSION" ]; then
        echo "gitleaks: version mismatch after install — expected ${GITLEAKS_VERSION}, got '${installed}'"
        exit 1
    fi

    echo "gitleaks: v${GITLEAKS_VERSION} installed."
}

# ── staged (pre-commit) ────────────────────────────────────────────
staged() {
    ensure
    echo "gitleaks: scanning staged changes..."
    "$GITLEAKS" git --pre-commit --staged --redact --config .gitleaks.toml --no-banner
}

# ── ci (workflow) ──────────────────────────────────────────────────
ci() {
    ensure

    echo "gitleaks: scanning checked-out tree..."
    "$GITLEAKS" dir . --redact --config .gitleaks.toml --no-banner

    if [ -n "${GITLEAKS_BASE_SHA:-}" ] && [ -n "${GITLEAKS_HEAD_SHA:-}" ]; then
        echo "gitleaks: scanning PR commits (${GITLEAKS_BASE_SHA}..${GITLEAKS_HEAD_SHA})..."
        "$GITLEAKS" git --log-opts="${GITLEAKS_BASE_SHA}..${GITLEAKS_HEAD_SHA}" --redact --config .gitleaks.toml --no-banner
    else
        echo "gitleaks: GITLEAKS_BASE_SHA / GITLEAKS_HEAD_SHA not set — skipping PR-range scan."
    fi
}

# ── dispatch ───────────────────────────────────────────────────────
case "${1:-}" in
    ensure) ensure ;;
    staged) staged ;;
    ci)     ci ;;
    *)
        echo "Usage: $0 {ensure|staged|ci}" >&2
        exit 1
        ;;
esac
