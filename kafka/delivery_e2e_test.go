//go:build e2e

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
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Timeouts used across scenarios. Generous, because a real broker's assignment
// and commit latencies vary; the tests wait on observable state, so a healthy
// run returns well before these elapse.
const (
	deliverTimeout = 30 * time.Second
	commitTimeout  = 15 * time.Second
	readyTimeout   = 45 * time.Second
)

// alwaysOK is the response script for a function that accepts every delivery.
func alwaysOK(string, int) (int, time.Duration) { return 200, 0 }

// TestE2E_HappyPath (scenario 1): every record is delivered exactly once and its
// offset committed, leaving zero lag.
func TestE2E_HappyPath(t *testing.T) {
	h := newHarness(t)
	fs := newFuncServer(t, alwaysOK)

	rs := h.startService(t, fs.url(), nil)
	rs.waitReady(t, readyTimeout)
	h.settle()

	values := []string{"r0", "r1", "r2", "r3", "r4"}
	h.produce(values...)

	waitFor(t, deliverTimeout, "all records delivered", func() bool {
		return fs.totalRequests() >= len(values)
	})
	for _, v := range values {
		if got := fs.attempts(v); got != 1 {
			t.Errorf("record %q delivered %d times, want exactly 1", v, got)
		}
	}
	waitFor(t, commitTimeout, "offsets committed (zero lag)", func() bool {
		return h.lag() == 0
	})
	if err := rs.stop(t); err != nil {
		t.Errorf("service returned error on stop: %v", err)
	}
}

// TestE2E_BoundedRetriesExhaustion (scenario 2): with a finite retry budget, a
// record the function never accepts is attempted exactly retry+1 times, then
// skipped and its offset committed so the partition advances -- and the next good
// record is delivered.
func TestE2E_BoundedRetriesExhaustion(t *testing.T) {
	h := newHarness(t)
	fs := newFuncServer(t, func(body string, _ int) (int, time.Duration) {
		if body == "poison" {
			return 500, 0
		}
		return 200, 0
	})

	rs := h.startService(t, fs.url(), map[string]string{
		envFunctionDeliveryRetry:      "2", // 3 attempts total
		envFunctionDeliveryBackoffDel: "50ms",
		envFunctionDeliveryBackoffMax: "50ms",
	})
	rs.waitReady(t, readyTimeout)
	h.settle()

	h.produce("good-before", "poison", "good-after")

	// The poison record exhausts its budget (3 attempts), is skipped, and the
	// partition advances to the following good record.
	waitFor(t, deliverTimeout, "poison attempted up to its budget", func() bool {
		return fs.attempts("poison") >= 3
	})
	waitFor(t, deliverTimeout, "good-after delivered past skipped poison", func() bool {
		return fs.attempts("good-after") >= 1
	})

	// Give any erroneous extra retry a chance to land before asserting the exact
	// count: retry=2 must mean exactly 3 attempts, no more.
	time.Sleep(1 * time.Second)
	if got := fs.attempts("poison"); got != 3 {
		t.Errorf("poison attempted %d times, want exactly 3 (retry=2 => 3 attempts)", got)
	}

	waitFor(t, commitTimeout, "all offsets committed after skip (zero lag)", func() bool {
		return h.lag() == 0
	})
	if err := rs.stop(t); err != nil {
		t.Errorf("service returned error on stop: %v", err)
	}
}

// TestE2E_UnlimitedRetriesDefault (scenario 3): with no retry budget configured
// (the default is unlimited), a failing record is never committed -- lag stays --
// and is redelivered until the function recovers, at which point it is committed.
func TestE2E_UnlimitedRetriesDefault(t *testing.T) {
	h := newHarness(t)
	const failUntil = 3
	fs := newFuncServer(t, func(body string, n int) (int, time.Duration) {
		if body == "recover" && n <= failUntil {
			return 503, 0
		}
		return 200, 0
	})

	rs := h.startService(t, fs.url(), map[string]string{
		// No FUNCTION_DELIVERY_RETRY => unlimited.
		envFunctionDeliveryBackoffDel: "50ms",
		envFunctionDeliveryBackoffMax: "100ms",
	})
	rs.waitReady(t, readyTimeout)
	h.settle()

	h.produce("recover")

	// While the function is still rejecting it, the record is retried but never
	// committed: the single produced record remains as lag.
	waitFor(t, deliverTimeout, "record retried while function fails", func() bool {
		return fs.attempts("recover") >= 2
	})
	if lag := h.lag(); lag == 0 {
		t.Error("offset committed while delivery still failing; unlimited retries must not commit an unacked record")
	}

	// Once the function starts accepting it, the record is delivered and committed.
	waitFor(t, deliverTimeout, "record delivered after function recovers", func() bool {
		return fs.attempts("recover") > failUntil
	})
	waitFor(t, commitTimeout, "offset committed after recovery (zero lag)", func() bool {
		return h.lag() == 0
	})
	if err := rs.stop(t); err != nil {
		t.Errorf("service returned error on stop: %v", err)
	}
}

// TestE2E_OffsetSafetyRedelivery (scenario 4): an acked record's offset is
// committed and never redelivered, while a record still failing when the consumer
// stops is NOT committed and is redelivered from the last committed offset after
// a restart (at-least-once).
func TestE2E_OffsetSafetyRedelivery(t *testing.T) {
	h := newHarness(t)
	var failPending atomic.Bool
	failPending.Store(true)
	fs := newFuncServer(t, func(body string, _ int) (int, time.Duration) {
		if body == "pending" && failPending.Load() {
			return 500, 0
		}
		return 200, 0
	})

	extraEnv := map[string]string{
		envFunctionDeliveryBackoffDel: "50ms",
		envFunctionDeliveryBackoffMax: "50ms",
	}

	// First session: deliver and commit "committed" (offset 0), then start failing
	// "pending" (offset 1) under unlimited retries.
	rs := h.startService(t, fs.url(), extraEnv)
	rs.waitReady(t, readyTimeout)
	h.settle()

	h.produce("committed")
	waitFor(t, deliverTimeout, "committed record acked", func() bool {
		return fs.successes("committed") >= 1
	})
	waitFor(t, commitTimeout, "committed offset advanced to 1", func() bool {
		return h.committedOffset() == 1
	})

	h.produce("pending")
	waitFor(t, deliverTimeout, "pending retried while failing", func() bool {
		return fs.attempts("pending") >= 2
	})

	// Stop mid-retry. "pending" never succeeded, so its offset must not advance.
	if err := rs.stop(t); err != nil {
		t.Errorf("service returned error on first stop: %v", err)
	}
	if got := h.committedOffset(); got != 1 {
		t.Errorf("committed offset=%d after aborting an unacked record; want 1 (only the acked record committed)", got)
	}

	// Second session: the function now accepts "pending". It must be redelivered
	// from the last committed offset (1) and then committed.
	failPending.Store(false)
	rs2 := h.startService(t, fs.url(), extraEnv)
	rs2.waitReady(t, readyTimeout)

	waitFor(t, deliverTimeout, "pending redelivered and acked after restart", func() bool {
		return fs.successes("pending") >= 1
	})
	waitFor(t, commitTimeout, "all offsets committed after redelivery (zero lag)", func() bool {
		return h.lag() == 0
	})
	if err := rs2.stop(t); err != nil {
		t.Errorf("service returned error on second stop: %v", err)
	}

	// The acked record from the first session was never redelivered.
	if got := fs.attempts("committed"); got != 1 {
		t.Errorf("committed record delivered %d times; a committed offset must not be redelivered after restart", got)
	}
}

// TestE2E_Backoff (scenario 5): the observed inter-attempt delays follow the
// configured policy -- exponential growth, linear growth, and the max-delay cap.
func TestE2E_Backoff(t *testing.T) {
	// assertAtLeast guards the robust lower bound: a retry can never arrive sooner
	// than its backoff (the runtime waits at least that long), so each gap must be
	// at least ~90% of the expected delay. Upper bounds are left loose because
	// scheduling and delivery round-trips only ever add time.
	assertAtLeast := func(t *testing.T, label string, got, want time.Duration) {
		t.Helper()
		if got < want*9/10 {
			t.Errorf("%s: gap %s shorter than expected backoff %s", label, got, want)
		}
	}

	t.Run("exponential", func(t *testing.T) {
		h := newHarness(t)
		const failUntil = 3
		fs := newFuncServer(t, func(body string, n int) (int, time.Duration) {
			if body == "retryme" && n <= failUntil {
				return 500, 0
			}
			return 200, 0
		})
		rs := h.startService(t, fs.url(), map[string]string{
			envFunctionDeliveryBackoffPol: "exponential",
			envFunctionDeliveryBackoffDel: "200ms",
			envFunctionDeliveryBackoffMax: "10s",
		})
		rs.waitReady(t, readyTimeout)
		h.settle()
		h.produce("retryme")
		waitFor(t, deliverTimeout, "record retried past the failing attempts", func() bool {
			return fs.attempts("retryme") >= failUntil+1
		})
		gaps := fs.gaps("retryme")
		if len(gaps) < 3 {
			t.Fatalf("got %d gaps, want >= 3", len(gaps))
		}
		assertAtLeast(t, "gap 1", gaps[0], 200*time.Millisecond)
		assertAtLeast(t, "gap 2", gaps[1], 400*time.Millisecond)
		assertAtLeast(t, "gap 3", gaps[2], 800*time.Millisecond)
		if !(gaps[1] > gaps[0] && gaps[2] > gaps[1]) {
			t.Errorf("gaps not growing exponentially: %v", gaps)
		}
		_ = rs.stop(t)
	})

	t.Run("linear", func(t *testing.T) {
		h := newHarness(t)
		const failUntil = 3
		fs := newFuncServer(t, func(body string, n int) (int, time.Duration) {
			if body == "retryme" && n <= failUntil {
				return 500, 0
			}
			return 200, 0
		})
		rs := h.startService(t, fs.url(), map[string]string{
			envFunctionDeliveryBackoffPol: "linear",
			envFunctionDeliveryBackoffDel: "150ms",
			envFunctionDeliveryBackoffMax: "10s",
		})
		rs.waitReady(t, readyTimeout)
		h.settle()
		h.produce("retryme")
		waitFor(t, deliverTimeout, "record retried past the failing attempts", func() bool {
			return fs.attempts("retryme") >= failUntil+1
		})
		gaps := fs.gaps("retryme")
		if len(gaps) < 3 {
			t.Fatalf("got %d gaps, want >= 3", len(gaps))
		}
		assertAtLeast(t, "gap 1", gaps[0], 150*time.Millisecond)
		assertAtLeast(t, "gap 2", gaps[1], 300*time.Millisecond)
		assertAtLeast(t, "gap 3", gaps[2], 450*time.Millisecond)
		if !(gaps[1] > gaps[0] && gaps[2] > gaps[1]) {
			t.Errorf("gaps not growing linearly: %v", gaps)
		}
		_ = rs.stop(t)
	})

	t.Run("maxDelayCap", func(t *testing.T) {
		h := newHarness(t)
		const failUntil = 4
		fs := newFuncServer(t, func(body string, n int) (int, time.Duration) {
			if body == "retryme" && n <= failUntil {
				return 500, 0
			}
			return 200, 0
		})
		rs := h.startService(t, fs.url(), map[string]string{
			envFunctionDeliveryBackoffPol: "exponential",
			envFunctionDeliveryBackoffDel: "200ms",
			envFunctionDeliveryBackoffMax: "400ms", // caps at 400ms, so never reaches 800ms
		})
		rs.waitReady(t, readyTimeout)
		h.settle()
		h.produce("retryme")
		waitFor(t, deliverTimeout, "record retried past the failing attempts", func() bool {
			return fs.attempts("retryme") >= failUntil+1
		})
		gaps := fs.gaps("retryme")
		if len(gaps) < 4 {
			t.Fatalf("got %d gaps, want >= 4", len(gaps))
		}
		// The third gap onward would be 800ms+ uncapped; the cap holds them near
		// 400ms. Assert they are capped (well below the uncapped 800ms) yet still
		// honor the configured delay.
		for i := 2; i < 4; i++ {
			assertAtLeast(t, "capped gap", gaps[i], 400*time.Millisecond)
			if gaps[i] > 700*time.Millisecond {
				t.Errorf("gap %d = %s exceeds the 400ms cap (should not grow toward 800ms)", i+1, gaps[i])
			}
		}
		_ = rs.stop(t)
	})
}

// TestE2E_PerAttemptTimeout (scenario 6): an attempt that exceeds
// FUNCTION_REQUEST_TIMEOUT counts as a failed delivery and is retried; once an
// attempt responds within the timeout the record is delivered and committed.
func TestE2E_PerAttemptTimeout(t *testing.T) {
	h := newHarness(t)
	const slowAttempts = 2
	fs := newFuncServer(t, func(body string, n int) (int, time.Duration) {
		if body == "slow" && n <= slowAttempts {
			// Respond well after the 300ms per-attempt timeout: the client cancels
			// first, so the attempt is a failure even though the status is 200.
			return 200, 500 * time.Millisecond
		}
		return 200, 0
	})

	rs := h.startService(t, fs.url(), map[string]string{
		envFunctionRequestTimeout:     "300ms",
		envFunctionDeliveryBackoffDel: "50ms",
		envFunctionDeliveryBackoffMax: "50ms",
	})
	rs.waitReady(t, readyTimeout)
	h.settle()

	h.produce("slow")

	// It must retry past the timed-out attempts and eventually succeed on a fast
	// one, then commit.
	waitFor(t, deliverTimeout, "record retried past timed-out attempts", func() bool {
		return fs.attempts("slow") > slowAttempts
	})
	waitFor(t, deliverTimeout, "record eventually acked within timeout", func() bool {
		return fs.successes("slow") >= 1
	})
	waitFor(t, commitTimeout, "offset committed after timeout retries (zero lag)", func() bool {
		return h.lag() == 0
	})
	if err := rs.stop(t); err != nil {
		t.Errorf("service returned error on stop: %v", err)
	}
}

// TestE2E_ZeroRetriesSingleAttempt (scenario 7): retry=0 means a single attempt;
// a rejected record is not retried, is skipped and committed, and the next good
// record is delivered.
func TestE2E_ZeroRetriesSingleAttempt(t *testing.T) {
	h := newHarness(t)
	fs := newFuncServer(t, func(body string, _ int) (int, time.Duration) {
		if body == "poison" {
			return 500, 0
		}
		return 200, 0
	})

	rs := h.startService(t, fs.url(), map[string]string{
		envFunctionDeliveryRetry: "0", // single attempt
	})
	rs.waitReady(t, readyTimeout)
	h.settle()

	h.produce("good0", "poison", "good1")

	waitFor(t, deliverTimeout, "poison attempted and good1 delivered", func() bool {
		return fs.attempts("poison") >= 1 && fs.attempts("good1") >= 1
	})

	// No retry: let any erroneous second attempt land before asserting the count.
	time.Sleep(1 * time.Second)
	if got := fs.attempts("poison"); got != 1 {
		t.Errorf("poison attempted %d times, want exactly 1 (retry=0 => single attempt)", got)
	}

	waitFor(t, commitTimeout, "all offsets committed (zero lag)", func() bool {
		return h.lag() == 0
	})
	if err := rs.stop(t); err != nil {
		t.Errorf("service returned error on stop: %v", err)
	}
}

// TestE2E_RestartDurability (scenario 8): offsets committed in one session
// survive a full restart -- already-delivered records are not redelivered, and
// records produced after the restart are delivered from where the group left off.
func TestE2E_RestartDurability(t *testing.T) {
	h := newHarness(t)
	fs := newFuncServer(t, alwaysOK)

	// First session: deliver and commit a, b, c.
	rs := h.startService(t, fs.url(), nil)
	rs.waitReady(t, readyTimeout)
	h.settle()
	h.produce("a", "b", "c")
	waitFor(t, deliverTimeout, "first batch delivered", func() bool {
		return fs.totalRequests() >= 3
	})
	waitFor(t, commitTimeout, "first batch committed (zero lag)", func() bool {
		return h.lag() == 0
	})
	if err := rs.stop(t); err != nil {
		t.Errorf("service returned error on first stop: %v", err)
	}

	// Second session on the same group: produce d, e. The committed records must
	// not reappear; the new records must be delivered.
	rs2 := h.startService(t, fs.url(), nil)
	rs2.waitReady(t, readyTimeout)
	h.settle()
	h.produce("d", "e")
	waitFor(t, deliverTimeout, "new records delivered after restart", func() bool {
		return fs.successes("d") >= 1 && fs.successes("e") >= 1
	})
	waitFor(t, commitTimeout, "new records committed (zero lag)", func() bool {
		return h.lag() == 0
	})
	if err := rs2.stop(t); err != nil {
		t.Errorf("service returned error on second stop: %v", err)
	}

	for _, v := range []string{"a", "b", "c"} {
		if got := fs.attempts(v); got != 1 {
			t.Errorf("record %q delivered %d times; committed records must not be redelivered after restart", v, got)
		}
	}
}

// TestE2E_InvalidConfigFailsFast (scenario 9): an invalid FUNCTION_DELIVERY_*
// value is rejected at startup -- before the function-target wait -- so the
// process exits promptly rather than running with a silently ignored setting.
func TestE2E_InvalidConfigFailsFast(t *testing.T) {
	// Gated on a broker only to keep the whole suite behind one switch; this
	// scenario fails before any broker connection is attempted.
	requireBrokers(t)

	t.Setenv(envKafkaBrokers, "localhost:9092")
	t.Setenv(envKafkaTopic, "irrelevant")
	t.Setenv(envKafkaConsumerGroup, "irrelevant")
	// Unlimited is expressed by omitting the variable; a negative value is a typo
	// the runtime must reject rather than silently treat as "retry forever".
	t.Setenv(envFunctionDeliveryRetry, "-1")

	svc, err := New("http://127.0.0.1:1/")
	if err != nil {
		t.Fatalf("creating service: %v", err)
	}
	// Long target wait: if the bad config were not caught first, Start would block
	// here, and the elapsed-time check below would fail.
	svc.targetWaitTimeout = 30 * time.Second

	start := time.Now()
	err = svc.Start(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Start returned nil; expected it to reject the invalid FUNCTION_DELIVERY_RETRY")
	}
	if !strings.Contains(err.Error(), "FUNCTION_DELIVERY_RETRY") {
		t.Errorf("error %q does not mention the offending variable FUNCTION_DELIVERY_RETRY", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("Start took %s to reject bad config; it must fail before the %s target wait", elapsed, svc.targetWaitTimeout)
	}
}
