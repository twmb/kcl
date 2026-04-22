#!/bin/bash

set -exu

# Build per-platform binaries, tagging each with the current git
# describe output so `kcl --version` (and the Kafka wire ClientID)
# report the release tag instead of a dev pseudo-version.
VERSION="$(git describe --tags --always --dirty)"
LDFLAGS="-X main.version=${VERSION}"

build() {
	local os="$1" arch="$2" suffix="${3:-}"
	local out="kcl_${os}_${arch}${suffix}"
	CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -ldflags "$LDFLAGS" -o "$out"
	gzip -9 -f "$out"
}

build windows amd64 .exe
build darwin  amd64
build darwin  arm64
build linux   amd64
build linux   arm64
