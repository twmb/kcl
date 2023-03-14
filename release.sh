#!/bin/bash

set -exu

CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build && gzip -9 kcl.exe && mv kcl.exe.gz kcl_windows_amd64.gz
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build && gzip -9 kcl && mv kcl.gz kcl_darwin_amd64.gz
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build && gzip -9 kcl && mv kcl.gz kcl_linux_arm64.gz
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build && gzip -9 kcl && mv kcl.gz kcl_linux_amd64.gz
