#!/usr/bin/env bash
dirs=$(go list -f {{.Dir}} ./... | grep -v /vendor/)
for d in $dirs; do goimports -w $d/*.go; done