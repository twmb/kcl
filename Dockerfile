# syntax=docker/dockerfile:1

# Build natively on BUILDPLATFORM and cross-compile the (CGO-free) Go binary
# for TARGETARCH, same as release.sh -- avoids paying for QEMU emulation on
# every multi-arch build.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
	go build -ldflags "-X main.version=${VERSION}" -o /out/kcl .

# distroless/static: no shell, no package manager, just a CA bundle -- kcl is
# a static binary and only needs certs for TLS-secured Kafka/Schema Registry.
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/kcl /usr/local/bin/kcl
ENTRYPOINT ["/usr/local/bin/kcl"]
