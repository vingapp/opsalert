#!/bin/sh
# Install git hooks by symlinking scripts/hooks/* into .git/hooks/.
#
# Relative symlinks (../../scripts/hooks/<name>) so they survive rsync -a to
# the construct gate runner and work inside the runner container without
# adjusting paths.
#
# Linked worktrees have a .git *file* (pointer), not a directory. Their hooks
# run from the main repo's .git/hooks/, not their own — installing here would
# silently do nothing. Refuse instead of lying about it.
#
# Usage (from repo root):
#   bash scripts/hooks/install.sh

set -e

if [ ! -d .git ]; then
    echo "install.sh: .git is not a directory (linked worktree or not a repo root). Refusing." >&2
    exit 1
fi

for hook in pre-commit pre-push; do
    if [ -f "scripts/hooks/$hook" ]; then
        ln -sfn "../../scripts/hooks/$hook" ".git/hooks/$hook"
        echo "installed: .git/hooks/$hook -> ../../scripts/hooks/$hook"
    fi
done
