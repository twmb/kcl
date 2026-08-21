#!/bin/bash

set -eu

# Build per-platform binaries with the release version baked in, so that
# `kcl --version` (and the Kafka wire ClientID) report it rather than a dev
# pseudo-version.
#
# The version is an argument rather than `git describe` output: the binaries
# are built before the tag is created, so describing HEAD produced the
# previous tag plus a commit distance, e.g. "v0.18.0-30-g66f4393".

if [ $# -ne 1 ]; then
	echo "usage: $0 VERSION" >&2
	echo "   eg: $0 v0.19.0" >&2
	exit 1
fi

VERSION="$1"

# A dirty tree does not build the release it claims to; say so in the binary
# rather than shipping something that cannot be reproduced from the tag.
if [ -n "$(git status --porcelain)" ]; then
	VERSION="${VERSION}-dirty"
	echo "warning: working tree is dirty, building ${VERSION}" >&2
fi

set -x
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
