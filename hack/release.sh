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

# Documentation about this script and how to use it can be found
# at https://github.com/knative/hack

set -o errexit
set -o nounset
set -o pipefail

ORG_NAME=knative-extensions

source "$(go run knative.dev/hack/cmd/script release.sh)"

# Yaml files to generate, and the source config dir for them. The Kafka runtime
# ships as a single container image that func injects as a sidecar (there is no
# standalone Deployment), so config/ holds only a ConfigMap that references the
# image via ko://. `ko resolve` publishes the image and rewrites that reference
# to the published digest, giving the release a signed, pinned image reference.
declare -A COMPONENTS
COMPONENTS=(
  ["func-adapter-kafka.yaml"]="config"
)
readonly COMPONENTS

function build_release() {
  local all_yamls=()
  for yaml in "${!COMPONENTS[@]}"; do
    local config="${COMPONENTS[${yaml}]}"
    echo "Resolving ${config}/ into ${yaml}"
    ko resolve ${KO_FLAGS} -f "${config}/" > "${yaml}"
    all_yamls+=("${yaml}")
  done
  ARTIFACTS_TO_PUBLISH="${all_yamls[*]}"
}

main "$@"
