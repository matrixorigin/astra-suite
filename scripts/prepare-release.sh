#!/usr/bin/env bash
# Prepare an astra-gateway release commit and tag.
#
# Usage:
#   scripts/prepare-release.sh 0.3.1
#   scripts/prepare-release.sh v0.3.1 --push
#
# The GitHub release workflow builds binaries when a v* tag is pushed.

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/prepare-release.sh <version|vversion> [--push] [--no-check]

Examples:
  scripts/prepare-release.sh 0.3.1
  scripts/prepare-release.sh v0.3.1 --push

Options:
  --push      Push the release commit and tag after creating them.
  --no-check  Skip cargo check after updating Cargo.toml.
USAGE
}

if [[ $# -lt 1 || "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

version="${1#v}"
tag="v${version}"
shift

push=false
run_check=true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --push)
      push=true
      ;;
    --no-check)
      run_check=false
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
  shift
done

if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "invalid semver version: $version" >&2
  exit 2
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "working tree is dirty; commit or stash changes before preparing a release" >&2
  exit 1
fi

if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
  echo "tag already exists locally: ${tag}" >&2
  exit 1
fi

if git ls-remote --exit-code --tags origin "refs/tags/${tag}" >/dev/null 2>&1; then
  echo "tag already exists on origin: ${tag}" >&2
  exit 1
fi

python3 - "$version" <<'PY'
from pathlib import Path
import re
import sys

version = sys.argv[1]
path = Path("Cargo.toml")
text = path.read_text()
pattern = re.compile(r'(?m)^version = "[^"]+"$', re.MULTILINE)
new_text, count = pattern.subn(f'version = "{version}"', text, count=1)
if count != 1:
    raise SystemExit("failed to update workspace package version in Cargo.toml")
path.write_text(new_text)
PY

if [[ "$run_check" == true ]]; then
  cargo check --workspace --all-targets
else
  cargo metadata --locked --format-version 1 >/dev/null
fi

git add Cargo.toml Cargo.lock
git commit -m "Release ${tag}"
git tag -a "${tag}" -m "Release ${tag}"

echo "prepared release ${tag}"
if [[ "$push" == true ]]; then
  git push origin HEAD
  git push origin "${tag}"
  echo "pushed ${tag}; GitHub Actions will build release binaries"
else
  echo "next steps:"
  echo "  git push origin HEAD"
  echo "  git push origin ${tag}"
fi
