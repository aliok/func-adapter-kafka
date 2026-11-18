//go:build unix

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
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

// TestHandleSignals_CleanupStopsDelivery verifies that after the cleanup returned by
// handleSignals runs, a delivered SIGINT no longer reaches the Service. On the pre-fix
// code (cleanup a no-op) the handler stays registered and sendStop fires, so this
// fails.
func TestHandleSignals_CleanupStopsDelivery(t *testing.T) {
	// Keep SIGINT from killing the test binary once we deliver it below.
	guard := make(chan os.Signal, 1)
	signal.Notify(guard, syscall.SIGINT)
	defer signal.Stop(guard)

	svc := newService(stubAdapter{})
	stop := svc.handleSignals()
	stop()

	if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	// Confirm the signal was actually delivered to the process.
	select {
	case <-guard:
	case <-time.After(5 * time.Second):
		t.Fatal("SIGINT not delivered")
	}
	// Give a still-registered handler a chance to call sendStop before asserting none did.
	time.Sleep(100 * time.Millisecond)
	select {
	case <-svc.stop:
		t.Fatal("signal reached the Service after cleanup; handler still registered")
	default:
	}
}

// TestHandleSignals_CleanupRestoresDefaultTermination verifies the actual bug: after
// cleanup, SIGTERM again terminates the process (Go's default) rather than being
// swallowed by a still-registered handler. It runs the signal-sending half in a child
// process so the parent can observe how the child died.
func TestHandleSignals_CleanupRestoresDefaultTermination(t *testing.T) {
	if os.Getenv("HANDLE_SIGNALS_CHILD") == "1" {
		svc := newService(stubAdapter{})
		stop := svc.handleSignals()
		stop()
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		time.Sleep(3 * time.Second)
		os.Exit(0) // reached only if SIGTERM was swallowed
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestHandleSignals_CleanupRestoresDefaultTermination$")
	cmd.Env = append(os.Environ(), "HANDLE_SIGNALS_CHILD=1")
	var ee *exec.ExitError
	if err := cmd.Run(); !errors.As(err, &ee) {
		t.Fatalf("child exited cleanly (err=%v); SIGTERM was swallowed", err)
	}
	if ws := ee.Sys().(syscall.WaitStatus); !ws.Signaled() || ws.Signal() != syscall.SIGTERM {
		t.Fatalf("child status = %v, want killed by SIGTERM", ws)
	}
}

// TestStart_UnregistersSignalHandlerOnReturn exercises the fix at its call site: it
// drives the real Start, cancels it, waits for it to return, then delivers a SIGINT
// and asserts the Service's handler no longer fires. The handleSignals tests above
// call the cleanup directly; this one proves Start actually invokes it (defer
// stopSignals). Removing that defer makes this fail: the leaked handler sends to
// svc.stop.
func TestStart_UnregistersSignalHandlerOnReturn(t *testing.T) {
	// Bind an ephemeral loopback port like the other Start tests; without this Start
	// binds DefaultListenAddress ([::]:8080), which is a fixed port -- often taken (it
	// is the function's own default), making net.Listen fail before the health server
	// ever starts.
	t.Setenv("LISTEN_ADDRESS", "127.0.0.1:0")

	// Keep SIGINT from killing the test binary once we deliver it below.
	guard := make(chan os.Signal, 1)
	signal.Notify(guard, syscall.SIGINT)
	defer signal.Stop(guard)

	svc := newService(stubAdapter{})
	svc.target = refusedTarget(t) // Start blocks in the target wait until we cancel.
	svc.targetWaitTimeout = 30 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Start(ctx) }()

	// Once the health server is listening, Start is past Listen (and thus past
	// handleSignals); cancel for a clean shutdown and wait for Start to return nil.
	waitForHealthServer(t, svc)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start after a clean context cancel = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after context cancel")
	}

	// Start has returned, so its deferred cleanup must have unregistered the handler: a
	// SIGINT must reach only the guard, not the Service.
	if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	select {
	case <-guard:
	case <-time.After(5 * time.Second):
		t.Fatal("SIGINT not delivered")
	}
	time.Sleep(100 * time.Millisecond) // let a leaked handler fire, if any
	select {
	case <-svc.stop:
		t.Fatal("SIGINT reached the Service after Start returned; handler still registered")
	default:
	}
}
