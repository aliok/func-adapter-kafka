#!/usr/bin/env bash

# Copyright 2026 The Knative Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script runs the presubmit tests; it is started by prow for each PR.
# For convenience, it can also be executed manually.
# Running the script without parameters, or with the --all-tests flag, runs all
# tests in order. Use --build-tests, --unit-tests, or --integration-tests to run
# a specific set.

export DISABLE_MD_LINTING=1
export GO111MODULE=on

source "$(go run knative.dev/hack/cmd/script presubmit-tests.sh)"

# Override the default build phase with the repo's own checks (gofmt + go vet),
# which the Makefile already encodes.
function build_tests() {
  go vet ./... && gofmt_check
}

function gofmt_check() {
  local out
  out="$(gofmt -l .)"
  if [[ -n "${out}" ]]; then
    echo "The following files need gofmt:"
    echo "${out}"
    return 1
  fi
}

# Run the unit tests.
function unit_tests() {
  go test ./...
}

main "$@"
