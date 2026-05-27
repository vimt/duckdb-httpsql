#!/usr/bin/env bash
#
# Bump the pinned DuckDB and extension-ci-tools version.
# Usage: scripts/bump-duckdb-version.sh <new-tag like v1.5.4>
#
set -euo pipefail

NEW_VERSION="${1:?Usage: $0 <new-tag like v1.5.4>}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OLD_VERSION=$(git -C duckdb describe --exact-match --tags HEAD 2>/dev/null || true)
if [[ -z "$OLD_VERSION" ]]; then
  echo "Cannot determine current pinned tag (duckdb HEAD is not on a tag)" >&2
  exit 1
fi

if [[ "$OLD_VERSION" == "$NEW_VERSION" ]]; then
  echo "Already on $NEW_VERSION, nothing to do."
  exit 0
fi

echo "Bumping duckdb $OLD_VERSION -> $NEW_VERSION"

git -C duckdb fetch --tags origin "$NEW_VERSION"
git -C duckdb checkout "$NEW_VERSION"

git -C extension-ci-tools fetch origin "$NEW_VERSION"
git -C extension-ci-tools checkout "$NEW_VERSION"

sed -i "s|branch = $OLD_VERSION|branch = $NEW_VERSION|" .gitmodules
sed -i "s|$OLD_VERSION|$NEW_VERSION|g" .github/workflows/MainDistributionPipeline.yml

echo "Done. Summary:"
git diff --stat
