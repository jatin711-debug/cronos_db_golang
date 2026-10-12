#!/usr/bin/env bash
# Builds what a release ships besides the container image: the server and the
# admin tool for Linux, macOS and Windows, one archive for each system, and
# their checksums.
#
#   scripts/build-release-binaries.sh <version> [out-dir]
#
# The binaries are built without cgo, so that each can be built from any
# system and runs without a library beside it. They use the bloom filter
# written in Go. The container image is built with cgo and uses the one
# written in Rust, which is faster; the two keep nothing on disk, so a data
# directory can go from one to the other.
set -euo pipefail

version="${1:?usage: build-release-binaries.sh <version> [out-dir]}"
out="${2:-dist}"
targets="linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64"

cd "$(dirname "$0")/.."
rm -rf "$out"
mkdir -p "$out"

for target in $targets; do
  os="${target%/*}"
  arch="${target#*/}"
  name="cronos-db_${version}_${os}_${arch}"
  dir="$out/$name"
  ext=""
  if [ "$os" = windows ]; then
    ext=".exe"
  fi
  mkdir -p "$dir"
  echo "building $name"
  CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath -ldflags "-s -w" -o "$dir/cronos-api$ext" ./cmd/api
  CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath -ldflags "-s -w" -o "$dir/cronos-admin$ext" ./cmd/admin
  cp LICENSE README.md "$dir/"
  if [ "$os" = windows ]; then
    (cd "$out" && zip -qr "$name.zip" "$name")
  else
    tar -C "$out" -czf "$out/$name.tar.gz" "$name"
  fi
  rm -rf "$dir"
done

(cd "$out" && sha256sum cronos-db_* > SHA256SUMS)
echo "built into $out:"
ls -l "$out"
