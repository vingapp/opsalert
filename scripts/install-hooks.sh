#!/bin/sh
# Install this repo's git hooks into .git/hooks. Run once after cloning:
#     sh scripts/install-hooks.sh
# Git hooks live under .git/ (not version-controlled), so the source of truth is
# scripts/hooks/ and each clone installs from there.
set -e
ROOT=$(git rev-parse --show-toplevel)
for hook in "$ROOT"/scripts/hooks/*; do
    name=$(basename "$hook")
    cp "$hook" "$ROOT/.git/hooks/$name"
    chmod +x "$ROOT/.git/hooks/$name"
    echo "installed .git/hooks/$name"
done
