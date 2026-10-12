/*
Copyright 2026 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"strings"
	"testing"
	"time"
)

// TestWaitForTarget_RejectsInvalidTargets verifies that a target which is not an
// absolute http(s) URL with a host is rejected fast, rather than silently
// resolving to probing ":80" on localhost. These inputs fail validation before
// any dial, so the call returns immediately regardless of the timeout.
func TestWaitForTarget_RejectsInvalidTargets(t *testing.T) {
	cases := map[string]string{
		"bare word":    "myfunc",
		"host:port":    "127.0.0.1:8080", // parsed as scheme "127.0.0.1", no host
		"empty":        "",
		"wrong scheme": "ftp://example.com",
		"no host":      "http://",
	}

	for name, target := range cases {
		t.Run(name, func(t *testing.T) {
			err := waitForTarget(target, time.Second)
			if err == nil {
				t.Fatalf("waitForTarget(%q) = nil, want an error", target)
			}
			if !strings.Contains(err.Error(), "FUNCTION_TARGET") {
				t.Errorf("waitForTarget(%q) error = %v, want a FUNCTION_TARGET validation error", target, err)
			}
		})
	}
}
