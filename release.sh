#!/bin/bash

set -exu

# Build per-platform binaries, tagging each with the current git
# describe output so `kcl --version` (and the Kafka wire ClientID)
# report the release tag instead of a dev pseudo-version.
VERSION="$(git describe --tags --always --dirty)"
LDFLAGS="-X main.version=${VERSION}"

build() {
	local os="$1" arch="$2" exe="$3"
	CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -ldflags "$LDFLAGS" -o "$exe"
	gzip -9 "$exe"
	mv "$exe.gz" "kcl_${os}_${arch}.gz"
}

build windows amd64 kcl.exe
build darwin  amd64 kcl
build darwin  arm64 kcl
build linux   amd64 kcl
build linux   arm64 kcl
