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

package kafka

import (
	"strings"
	"testing"
	"time"
)

// Unset every FUNCTION_DELIVERY_* variable for the duration of a test so the
// environment of the test runner cannot leak in. t.Setenv restores them on
// cleanup.
func clearDeliveryEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"FUNCTION_DELIVERY_RETRY",
		"FUNCTION_DELIVERY_BACKOFF_POLICY",
		"FUNCTION_DELIVERY_BACKOFF_DELAY",
		"FUNCTION_DELIVERY_BACKOFF_MAX_DELAY",
	} {
		t.Setenv(name, "")
	}
}

// With no variables set the config reproduces the runtime's original behavior:
// unlimited exponential retries from 100ms to 30s.
func TestLoadDeliveryConfig_Defaults(t *testing.T) {
	clearDeliveryEnv(t)

	c, err := loadDeliveryConfig()
	if err != nil {
		t.Fatalf("loadDeliveryConfig: %v", err)
	}
	if !c.unlimited() {
		t.Errorf("unlimited() = false; want true by default")
	}
	if c.maxAttempts() != -1 {
		t.Errorf("maxAttempts() = %d; want -1 (unlimited)", c.maxAttempts())
	}
	if c.policy != backoffExponential {
		t.Errorf("policy = %q; want exponential", c.policy)
	}
	if c.baseDelay != 100*time.Millisecond {
		t.Errorf("baseDelay = %s; want 100ms", c.baseDelay)
	}
	if c.maxDelay != 30*time.Second {
		t.Errorf("maxDelay = %s; want 30s", c.maxDelay)
	}
}

// A blank value is treated as unset and falls back to the default.
func TestLoadDeliveryConfig_BlankIsDefault(t *testing.T) {
	clearDeliveryEnv(t)
	t.Setenv("FUNCTION_DELIVERY_RETRY", "   ")

	c, err := loadDeliveryConfig()
	if err != nil {
		t.Fatalf("loadDeliveryConfig: %v", err)
	}
	if !c.unlimited() {
		t.Errorf("blank FUNCTION_DELIVERY_RETRY should keep the unlimited default")
	}
}

func TestLoadDeliveryConfig_Valid(t *testing.T) {
	clearDeliveryEnv(t)
	t.Setenv("FUNCTION_DELIVERY_RETRY", "5")
	t.Setenv("FUNCTION_DELIVERY_BACKOFF_POLICY", "LINEAR") // case-insensitive
	t.Setenv("FUNCTION_DELIVERY_BACKOFF_DELAY", "250ms")
	t.Setenv("FUNCTION_DELIVERY_BACKOFF_MAX_DELAY", "10s")

	c, err := loadDeliveryConfig()
	if err != nil {
		t.Fatalf("loadDeliveryConfig: %v", err)
	}
	if c.unlimited() {
		t.Errorf("unlimited() = true; want false when retry is set")
	}
	if c.retry != 5 || c.maxAttempts() != 6 {
		t.Errorf("retry=%d maxAttempts=%d; want 5 and 6", c.retry, c.maxAttempts())
	}
	if c.policy != backoffLinear {
		t.Errorf("policy = %q; want linear", c.policy)
	}
	if c.baseDelay != 250*time.Millisecond || c.maxDelay != 10*time.Second {
		t.Errorf("baseDelay=%s maxDelay=%s; want 250ms and 10s", c.baseDelay, c.maxDelay)
	}
}

// retry=0 is a valid finite budget: one attempt, no retry.
func TestLoadDeliveryConfig_ZeroRetry(t *testing.T) {
	clearDeliveryEnv(t)
	t.Setenv("FUNCTION_DELIVERY_RETRY", "0")

	c, err := loadDeliveryConfig()
	if err != nil {
		t.Fatalf("loadDeliveryConfig: %v", err)
	}
	if c.unlimited() {
		t.Errorf("retry=0 must not be unlimited")
	}
	if c.maxAttempts() != 1 {
		t.Errorf("maxAttempts() = %d; want 1", c.maxAttempts())
	}
}

// When only the base delay is set and it exceeds the 30s default max, the
// validation error must flag the max as an unset default rather than naming a
// variable the operator never set.
func TestLoadDeliveryConfig_MaxBelowBaseNamesDefault(t *testing.T) {
	clearDeliveryEnv(t)
	t.Setenv("FUNCTION_DELIVERY_BACKOFF_DELAY", "60s") // max defaults to 30s

	_, err := loadDeliveryConfig()
	if err == nil {
		t.Fatal("loadDeliveryConfig accepted base (60s) above the default max (30s); want error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "FUNCTION_DELIVERY_BACKOFF_MAX_DELAY (default 30s, unset)") {
		t.Errorf("error should flag the max as an unset default; got: %v", err)
	}
	if !strings.Contains(msg, "FUNCTION_DELIVERY_BACKOFF_DELAY (1m0s)") {
		t.Errorf("error should name the base as a set value; got: %v", err)
	}
}

func TestLoadDeliveryConfig_Invalid(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
	}{
		{"non-numeric retry", map[string]string{"FUNCTION_DELIVERY_RETRY": "lots"}},
		{"negative retry", map[string]string{"FUNCTION_DELIVERY_RETRY": "-1"}},
		{"unknown policy", map[string]string{"FUNCTION_DELIVERY_BACKOFF_POLICY": "fibonacci"}},
		{"bad delay", map[string]string{"FUNCTION_DELIVERY_BACKOFF_DELAY": "soon"}},
		{"negative delay", map[string]string{"FUNCTION_DELIVERY_BACKOFF_DELAY": "-1s"}},
		{"bad max", map[string]string{"FUNCTION_DELIVERY_BACKOFF_MAX_DELAY": "later"}},
		{"max below base", map[string]string{
			"FUNCTION_DELIVERY_BACKOFF_DELAY":     "10s",
			"FUNCTION_DELIVERY_BACKOFF_MAX_DELAY": "1s",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearDeliveryEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if _, err := loadDeliveryConfig(); err == nil {
				t.Fatalf("loadDeliveryConfig accepted invalid config %v; want error", tc.env)
			}
		})
	}
}

func TestBackoffFor_Exponential(t *testing.T) {
	c := deliveryConfig{policy: backoffExponential, baseDelay: 100 * time.Millisecond, maxDelay: 30 * time.Second}
	want := map[int]time.Duration{
		1:  100 * time.Millisecond,
		2:  200 * time.Millisecond,
		3:  400 * time.Millisecond,
		4:  800 * time.Millisecond,
		9:  25600 * time.Millisecond,
		10: 30 * time.Second, // 51.2s capped to 30s
		50: 30 * time.Second, // far past the cap, no overflow
	}
	for n, w := range want {
		if got := c.backoffFor(n); got != w {
			t.Errorf("backoffFor(%d) = %s; want %s", n, got, w)
		}
	}
}

func TestBackoffFor_Linear(t *testing.T) {
	c := deliveryConfig{policy: backoffLinear, baseDelay: 100 * time.Millisecond, maxDelay: 1 * time.Second}
	want := map[int]time.Duration{
		1:  100 * time.Millisecond,
		5:  500 * time.Millisecond,
		10: 1 * time.Second, // 1000ms exactly at cap
		11: 1 * time.Second, // past cap
		99: 1 * time.Second, // far past, no overflow
	}
	for n, w := range want {
		if got := c.backoffFor(n); got != w {
			t.Errorf("backoffFor(%d) = %s; want %s", n, got, w)
		}
	}
}

// A zero base delay yields no wait regardless of policy or attempt number.
func TestBackoffFor_ZeroBase(t *testing.T) {
	for _, p := range []backoffPolicy{backoffExponential, backoffLinear} {
		c := deliveryConfig{policy: p, baseDelay: 0, maxDelay: 30 * time.Second}
		if got := c.backoffFor(5); got != 0 {
			t.Errorf("policy %q: backoffFor(5) = %s; want 0", p, got)
		}
	}
}
