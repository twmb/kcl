#!/bin/bash

set -exu

GOOS=windows GOARCH=amd64 go build && gzip -9 kcl.exe && mv kcl.exe.gz kcl_windows_amd64.gz
GOOS=darwin GOARCH=amd64 go build && gzip -9 kcl && mv kcl.gz kcl_darwin_amd64.gz
GOOS=linux GOARCH=arm64 go build && gzip -9 kcl && mv kcl.gz kcl_linux_arm64.gz
GOOS=linux GOARCH=amd64 go build && gzip -9 kcl && mv kcl.gz kcl_linux_amd64.gz
