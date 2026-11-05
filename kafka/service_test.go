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
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
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

// refusedTarget reserves an ephemeral port and immediately releases it, returning
// an http URL whose connections are reliably refused. This keeps waitForTarget in
// its retry loop without relying on a well-known or possibly-privileged port.
func refusedTarget(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a refused port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return "http://" + addr
}

// waitForHealthServer blocks until the service's health server is listening, so a
// test can be sure Start is past Listen and into the target-wait phase.
func waitForHealthServer(t *testing.T, svc *Service) net.Addr {
	t.Helper()
	for i := 0; i < 200; i++ {
		if addr := svc.Addr(); addr != nil {
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("health server did not start listening")
	return nil
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

// TestWaitForTarget_RejectsInvalidTargets verifies the target is validated as an
// absolute http(s) URL with a host before any dialing, so a misconfigured
// FUNCTION_TARGET fails fast with a clear error instead of silently probing the
// wrong address.
func TestWaitForTarget_RejectsInvalidTargets(t *testing.T) {
	cases := map[string]string{
		"bare word":    "myfunc",
		"host:port":    "127.0.0.1:8080",
		"empty":        "",
		"wrong scheme": "ftp://example.com",
		"no host":      "http://",
	}
	for name, target := range cases {
		t.Run(name, func(t *testing.T) {
			err := waitForTarget(context.Background(), target, time.Second)
			if err == nil {
				t.Fatalf("expected an error for target %q, got nil", target)
			}
			if !strings.Contains(err.Error(), "FUNCTION_TARGET") {
				t.Errorf("expected error mentioning FUNCTION_TARGET, got: %v", err)
			}
		})
	}
}

// TestStart_ServesLivenessBeforeTargetReady is the regression test for the
// restart-loop bug: the health server must report ALIVE while Start is still
// waiting for a slow function target to accept connections. Previously the
// server came up only after the target wait, so an injected liveness probe saw
// connection-refused during a slow (e.g. JVM) cold start and the kubelet could
// restart-loop the Pod before the function was ever up. Readiness stays 503
// until the consumer has partitions assigned.
func TestStart_ServesLivenessBeforeTargetReady(t *testing.T) {
	t.Setenv("LISTEN_ADDRESS", "127.0.0.1:0")

	svc := newService(stubAdapter{})
	// Point the target at a refused port so waitForTarget keeps retrying and Start
	// stays in the target-wait phase for the duration of the test (the consumer
	// never starts).
	svc.target = refusedTarget(t)
	svc.targetWaitTimeout = 10 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Start(ctx) }()

	addr := waitForHealthServer(t, svc)
	base := "http://" + addr.String()

	// A bounded client so a regression (serving only after the target wait) fails
	// fast here instead of hanging until the test binary's global timeout.
	client := &http.Client{Timeout: 2 * time.Second}

	resp, err := client.Get(base + "/health/liveness")
	if err != nil {
		cancel()
		t.Fatalf("GET liveness: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("liveness during target wait = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	rresp, err := client.Get(base + "/health/readiness")
	if err != nil {
		cancel()
		t.Fatalf("GET readiness: %v", err)
	}
	rresp.Body.Close()
	if rresp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("readiness during target wait = %d, want %d", rresp.StatusCode, http.StatusServiceUnavailable)
	}

	// Cancelling during the wait is a clean shutdown: Start returns nil.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start after cancel during the wait = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after context cancel")
	}
}

// TestWaitForTarget_SucceedsWhenListening verifies waitForTarget returns nil once
// the target accepts a TCP connection.
func TestWaitForTarget_SucceedsWhenListening(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	target := "http://" + l.Addr().String()
	errc := make(chan error, 1)
	go func() { errc <- waitForTarget(context.Background(), target, 2*time.Second) }()

	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("waitForTarget on a listening target = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waitForTarget did not return for a reachable target")
	}
}

// TestWaitForTarget_TimesOutWhenRefused verifies waitForTarget gives up with a
// timeout error once the deadline passes against an unreachable target, instead
// of retrying forever. Bounded so a missing deadline check fails the test rather
// than hanging until the global test timeout.
func TestWaitForTarget_TimesOutWhenRefused(t *testing.T) {
	target := refusedTarget(t)
	errc := make(chan error, 1)
	go func() { errc <- waitForTarget(context.Background(), target, 100*time.Millisecond) }()

	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("waitForTarget on a refused target = nil, want a timeout error")
		}
		if !strings.Contains(err.Error(), "timed out") {
			t.Errorf("expected a timeout error, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waitForTarget did not time out; deadline not enforced?")
	}
}

// TestWaitForTarget_CancelDuringWait verifies a context cancellation during the
// wait returns context.Canceled well before the timeout, so a shutdown aborts the
// wait promptly.
func TestWaitForTarget_CancelDuringWait(t *testing.T) {
	target := refusedTarget(t)
	ctx, cancel := context.WithCancel(context.Background())

	errc := make(chan error, 1)
	go func() { errc <- waitForTarget(ctx, target, 10*time.Second) }()

	// Let the first dial fail and the wait enter its retry sleep, then cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waitForTarget after cancel = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waitForTarget did not return promptly after cancel")
	}
}

// TestStart_ReturnsErrorOnTargetTimeout verifies Start returns a non-nil error
// when the function target never becomes reachable within targetWaitTimeout, so
// the process exits and the kubelet restarts it.
func TestStart_ReturnsErrorOnTargetTimeout(t *testing.T) {
	t.Setenv("LISTEN_ADDRESS", "127.0.0.1:0")

	svc := newService(stubAdapter{})
	svc.target = refusedTarget(t)
	svc.targetWaitTimeout = 100 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- svc.Start(context.Background()) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Start returned nil on a target-wait timeout; want a non-nil error so the process exits")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after the target wait timed out")
	}
}

// TestStart_FailsFastOnInvalidDeliveryConfig verifies a malformed FUNCTION_DELIVERY_*
// value is rejected at process startup, before the function-target wait -- so a bad
// value is not masked by (and reported only after) a target-wait timeout when the
// function is slow to start or unavailable. The target points at a refused port with
// a long wait timeout; Start must return the delivery-config error well before that
// timeout could elapse.
func TestStart_FailsFastOnInvalidDeliveryConfig(t *testing.T) {
	t.Setenv("LISTEN_ADDRESS", "127.0.0.1:0")
	t.Setenv("FUNCTION_DELIVERY_RETRY", "not-a-number")

	svc := newService(stubAdapter{})
	svc.target = refusedTarget(t)
	svc.targetWaitTimeout = 30 * time.Second // long: the test must not wait this out

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- svc.Start(context.Background()) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Start accepted an invalid FUNCTION_DELIVERY_RETRY; want an error")
		}
		if !strings.Contains(err.Error(), "configuring delivery") {
			t.Errorf("expected a delivery-config error, got: %v", err)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Errorf("Start took %s to reject bad config; it should fail before the %s target wait", elapsed, svc.targetWaitTimeout)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not fail fast on invalid delivery config; it appears to be waiting for the target")
	}
}

// TestStart_ReturnsNilOnStopDuringWait verifies the SIGTERM path: a clean stop
// signalled while Start is still waiting for the target returns nil, joining the
// wait goroutine through shutdown rather than treating the stop as a failure.
func TestStart_ReturnsNilOnStopDuringWait(t *testing.T) {
	t.Setenv("LISTEN_ADDRESS", "127.0.0.1:0")

	svc := newService(stubAdapter{})
	svc.target = refusedTarget(t)
	svc.targetWaitTimeout = 10 * time.Second

	done := make(chan error, 1)
	go func() { done <- svc.Start(context.Background()) }()

	// Once the health server is up, Start is in the target wait; signal a clean
	// stop as the signal handler would on SIGTERM.
	waitForHealthServer(t, svc)
	svc.sendStop(nil)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start after a clean stop during the wait = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after a clean stop during the wait")
	}
}
