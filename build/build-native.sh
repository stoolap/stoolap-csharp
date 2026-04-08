#!/usr/bin/env bash
# Build the libstoolap native binary for the current host and copy it into
# the matching runtimes/<rid>/native folder. Run from the stoolap-csharp root.
#
# Source resolution order:
#   1. $STOOLAP_ROOT if set and valid
#   2. ../stoolap if it exists next to this repo
#   3. Auto-clone github.com/stoolap/stoolap at $STOOLAP_ENGINE_REF into
#      build/.stoolap-engine (gitignored)
set -euo pipefail

# Pin the stoolap engine version in one place. The CI workflows read the
# same value via the STOOLAP_ENGINE_REF env var in .github/workflows/*.yml.
STOOLAP_ENGINE_REF="${STOOLAP_ENGINE_REF:-v0.4.0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CSHARP_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Resolve stoolap source.
if [[ -n "${STOOLAP_ROOT:-}" && -f "${STOOLAP_ROOT}/Cargo.toml" ]]; then
  echo "Using \$STOOLAP_ROOT: ${STOOLAP_ROOT}"
elif [[ -f "${CSHARP_ROOT}/../stoolap/Cargo.toml" ]]; then
  STOOLAP_ROOT="$(cd "${CSHARP_ROOT}/../stoolap" && pwd)"
  echo "Using sibling checkout: ${STOOLAP_ROOT}"
else
  STOOLAP_ROOT="${CSHARP_ROOT}/build/.stoolap-engine"
  if [[ ! -f "${STOOLAP_ROOT}/Cargo.toml" ]]; then
    echo "Cloning stoolap ${STOOLAP_ENGINE_REF} into ${STOOLAP_ROOT}..."
    rm -rf "${STOOLAP_ROOT}"
    git clone --depth 1 --branch "${STOOLAP_ENGINE_REF}" \
      https://github.com/stoolap/stoolap.git "${STOOLAP_ROOT}"
  else
    echo "Using cached clone: ${STOOLAP_ROOT}"
  fi
fi

echo "Building libstoolap (release, features=ffi)..."
(cd "${STOOLAP_ROOT}" && cargo build --release --features ffi)

unameOut="$(uname -s)"
arch="$(uname -m)"
case "${unameOut}" in
  Darwin)
    case "${arch}" in
      arm64) rid="osx-arm64" ;;
      x86_64) rid="osx-x64" ;;
      *) echo "Unsupported macOS arch: ${arch}" >&2; exit 1 ;;
    esac
    src="${STOOLAP_ROOT}/target/release/libstoolap.dylib"
    dst_name="libstoolap.dylib"
    ;;
  Linux)
    case "${arch}" in
      x86_64) rid="linux-x64" ;;
      aarch64) rid="linux-arm64" ;;
      *) echo "Unsupported Linux arch: ${arch}" >&2; exit 1 ;;
    esac
    src="${STOOLAP_ROOT}/target/release/libstoolap.so"
    dst_name="libstoolap.so"
    ;;
  MINGW*|MSYS*|CYGWIN*)
    case "${arch}" in
      x86_64) rid="win-x64" ;;
      aarch64|arm64) rid="win-arm64" ;;
      *) echo "Unsupported Windows arch: ${arch}" >&2; exit 1 ;;
    esac
    src="${STOOLAP_ROOT}/target/release/stoolap.dll"
    dst_name="stoolap.dll"
    ;;
  *)
    echo "Unsupported platform: ${unameOut}" >&2
    exit 1
    ;;
esac

dst_dir="${CSHARP_ROOT}/runtimes/${rid}/native"
mkdir -p "${dst_dir}"
cp "${src}" "${dst_dir}/${dst_name}"
echo "Installed ${dst_name} -> ${dst_dir}"
