#!/usr/bin/env bash
# Idempotently releases one workspace crate. The three effects — publish to
# crates.io, push the crate@vX.Y.Z tag, create the GitHub release — are checked
# and performed independently, so re-running after a partial failure completes
# exactly the missing steps: a publish whose tag push failed is not re-published,
# a tagged crate whose GitHub release failed still gets its release, etc.
#
# Emits `published=true|false` (whether THIS run performed the crates.io
# publish) to GITHUB_OUTPUT so the workflow can attest provenance for exactly
# the crates packaged in this run.
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
summary="${GITHUB_STEP_SUMMARY:-/dev/null}"

published_on_registry() {
  curl -fsSL --retry 3 \
    -H "User-Agent: jetstreamer-release-workflow (github.com/anza-xyz/jetstreamer)" \
    "https://crates.io/api/v1/crates/${crate}/${version}" 2>/dev/null |
    jq -e ".version.num == \"${version}\"" >/dev/null 2>&1
}

published_this_run=false
if published_on_registry; then
  echo "==> ${tag}: already on crates.io; skipping publish"
  echo "- \`${tag}\`: publish skipped (already on crates.io)" >> "${summary}"
else
  echo "==> ${tag}: publishing to crates.io"
  # cargo blocks until the published crate is available in the registry index,
  # so the next crate in dependency order can resolve it immediately.
  cargo publish -p "${crate}"
  published_this_run=true
  echo "- \`${tag}\`: published to crates.io" >> "${summary}"
fi

if git ls-remote --exit-code --tags origin "refs/tags/${tag}" >/dev/null 2>&1; then
  echo "==> ${tag}: tag already exists; skipping tag push"
  echo "- \`${tag}\`: tag skipped (already exists)" >> "${summary}"
else
  echo "==> ${tag}: pushing tag"
  git tag -a "${tag}" -m "Release ${tag}"
  git push origin "${tag}"
  echo "- \`${tag}\`: tag pushed" >> "${summary}"
fi

if gh release view "${tag}" >/dev/null 2>&1; then
  echo "==> ${tag}: GitHub release already exists; skipping"
  echo "- \`${tag}\`: GitHub release skipped (already exists)" >> "${summary}"
else
  echo "==> ${tag}: creating GitHub release"
  gh release create "${tag}" --title "${tag}" --generate-notes
  echo "- \`${tag}\`: GitHub release created" >> "${summary}"
fi

echo "published=${published_this_run}" >> "${GITHUB_OUTPUT:-/dev/null}"
