#!/usr/bin/env sh
set -euo pipefail

# log current go version
go version

# install gcc
apk --no-progress add build-base

# install ginkgo
go install -mod=mod github.com/onsi/ginkgo/v2/ginkgo

# run unit tests
"$GOPATH"/bin/ginkgo  --skip-file=integration ./...
