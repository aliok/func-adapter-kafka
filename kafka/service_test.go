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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	svc, err := New("http://127.0.0.1:8080/")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return svc
}

func TestReady_NotReady(t *testing.T) {
	svc := newTestService(t)
	// ready defaults to false (no partitions assigned yet)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/health/readiness", nil)
	svc.Ready(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}
}

func TestReady_Ready(t *testing.T) {
	svc := newTestService(t)
	svc.ready.Store(true)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/health/readiness", nil)
	svc.Ready(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}
	if w.Body.String() != "READY" {
		t.Errorf("expected body %q, got %q", "READY", w.Body.String())
	}
}

// TestShutdown_WaitsForConsumer verifies shutdown joins the consumer goroutine:
// it must not return until consumeDone is closed (which, in the real service,
// happens only after the consumer's deferred client.Close() commits offsets and
// leaves the group). Without the join, shutdown would return as soon as it
// cancelled the context and the process could exit mid-cleanup.
func TestShutdown_WaitsForConsumer(t *testing.T) {
	svc := newTestService(t)
	svc.consumeDone = make(chan struct{})

	var finished atomic.Bool
	// Simulate a consumer that, once signalled to stop, takes a moment to run its
	// cleanup before closing consumeDone.
	svc.cancelConsume = func() {
		go func() {
			time.Sleep(50 * time.Millisecond)
			finished.Store(true)
			close(svc.consumeDone)
		}()
	}

	if err := svc.shutdown(nil); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}
	if !finished.Load() {
		t.Fatal("shutdown returned before the consumer goroutine finished (no join)")
	}
}

// TestShutdown_TimesOutOnStuckConsumer verifies the join is bounded: a consumer
// that never stops (e.g. wedged on an in-flight delivery) must not block exit
// forever. shutdown waits up to shutdownTimeout, then returns an error.
func TestShutdown_TimesOutOnStuckConsumer(t *testing.T) {
	svc := newTestService(t)
	svc.consumeDone = make(chan struct{}) // never closed: the consumer is wedged
	svc.cancelConsume = func() {}         // stop signal ignored
	svc.shutdownTimeout = 50 * time.Millisecond

	start := time.Now()
	err := svc.shutdown(nil)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a shutdown error when the consumer does not stop in time")
	}
	if elapsed < svc.shutdownTimeout {
		t.Errorf("shutdown returned after %s, before the %s timeout elapsed", elapsed, svc.shutdownTimeout)
	}
	if elapsed > 2*time.Second {
		t.Errorf("shutdown took %s; the timeout was not honored", elapsed)
	}
}

func TestAlive_Default(t *testing.T) {
	svc := newTestService(t)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/health/liveness", nil)
	svc.Alive(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}
	if w.Body.String() != "ALIVE" {
		t.Errorf("expected body %q, got %q", "ALIVE", w.Body.String())
	}
}
