#!/usr/bin/env sh
set -euo pipefail

export CGO_ENABLED=0

# log current go version
go version

# install ginkgo
go install -mod=mod github.com/onsi/ginkgo/v2/ginkgo

# run unit tests
"$GOPATH"/bin/ginkgo  --skip-file=integration ./...
