#!/usr/bin/env bash
# Publishes one workspace crate to crates.io, tags it, and creates its GitHub
# release. Skips cleanly when the crate@vX.Y.Z tag already exists, which makes
# the workspace release workflow resumable: re-running it only releases the
# crates that have not shipped yet.
#
# Usage: release-crate.sh <crate-name>
# Env:   CARGO_REGISTRY_TOKEN (crates.io), GITHUB_TOKEN (tag push + gh release)
set -euo pipefail

crate="$1"

version=$(cargo metadata --format-version 1 --no-deps |
  jq -r ".packages[] | select(.name == \"${crate}\") | .version")
if [ -z "${version}" ] || [ "${version}" = "null" ]; then
  echo "!! could not resolve version for ${crate}" >&2
  exit 1
fi
tag="${crate}@v${version}"

if git ls-remote --exit-code --tags origin "refs/tags/${tag}" >/dev/null 2>&1; then
  echo "==> ${tag} already released; skipping"
  echo "- \`${tag}\`: skipped (already released)" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
  exit 0
fi

echo "==> publishing ${tag}"
# cargo blocks until the published crate is available in the registry index, so
# the next crate in dependency order can resolve it immediately.
cargo publish -p "${crate}"

git tag -a "${tag}" -m "Release ${tag}"
git push origin "${tag}"
gh release create "${tag}" --title "${tag}" --generate-notes

echo "- \`${tag}\`: published" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
