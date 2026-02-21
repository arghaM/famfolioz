#!/bin/bash
#
# Release script for Famfolioz
#
# Usage:
#   ./release.sh patch    # 1.0.0 -> 1.0.1  (bug fixes)
#   ./release.sh minor    # 1.0.0 -> 1.1.0  (new features)
#   ./release.sh major    # 1.0.0 -> 2.0.0  (breaking changes)
#
set -e

BUMP_TYPE="${1:-}"
INIT_FILE="cas_parser/__init__.py"
CHANGELOG="CHANGELOG.md"

if [[ ! "$BUMP_TYPE" =~ ^(patch|minor|major)$ ]]; then
    echo "Usage: ./release.sh <patch|minor|major>"
    echo ""
    echo "  patch  - bug fixes, small tweaks         (1.0.0 -> 1.0.1)"
    echo "  minor  - new features, backward-compatible (1.0.0 -> 1.1.0)"
    echo "  major  - breaking changes                 (1.0.0 -> 2.0.0)"
    exit 1
fi

# Check for uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Error: You have uncommitted changes. Commit or stash them first."
    git status --short
    exit 1
fi

# Read current version
CURRENT=$(grep '__version__' "$INIT_FILE" | sed 's/.*"\(.*\)"/\1/')
if [[ -z "$CURRENT" ]]; then
    echo "Error: Could not read version from $INIT_FILE"
    exit 1
fi

IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"

case "$BUMP_TYPE" in
    major) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
    minor) MINOR=$((MINOR + 1)); PATCH=0 ;;
    patch) PATCH=$((PATCH + 1)) ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
TODAY=$(date +%Y-%m-%d)

echo "Releasing: $CURRENT -> $NEW_VERSION"
echo ""

# Check that [Unreleased] section has content
if ! grep -q "^### " "$CHANGELOG" 2>/dev/null; then
    echo "Warning: No changes listed under [Unreleased] in $CHANGELOG"
    read -p "Continue anyway? (y/N) " confirm
    [[ "$confirm" == "y" ]] || exit 1
fi

# 1. Update version in __init__.py
sed -i '' "s/__version__ = \"$CURRENT\"/__version__ = \"$NEW_VERSION\"/" "$INIT_FILE"
echo "Updated $INIT_FILE: $CURRENT -> $NEW_VERSION"

# 2. Update CHANGELOG: rename [Unreleased] -> [X.Y.Z] - date, add new [Unreleased]
sed -i '' "s/## \[Unreleased\]/## [Unreleased]\n\n## [$NEW_VERSION] - $TODAY/" "$CHANGELOG"
echo "Updated $CHANGELOG with [$NEW_VERSION] - $TODAY"

# 3. Run tests
echo ""
echo "Running tests..."
python3 -m pytest cas_parser/tests/ -q
echo ""

# 4. Commit and tag
git add "$INIT_FILE" "$CHANGELOG"
git commit -m "Release v$NEW_VERSION"
git tag -a "v$NEW_VERSION" -m "Release v$NEW_VERSION"

echo ""
echo "Done! Created commit and tag v$NEW_VERSION"
echo ""
echo "Next steps:"
echo "  git log --oneline -3          # review"
echo "  git push origin main --tags   # push to GitHub"
echo ""
