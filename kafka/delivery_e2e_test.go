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
	"errors"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
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
	// Scenario 1 requires the record to arrive as the mapped CloudEvent, not merely
	// as some HTTP body: verify the binary-mode CloudEvent metadata for one record.
	rec, ok := fs.firstDelivery("r0")
	if !ok {
		t.Fatal("no delivery recorded for r0")
	}
	// r0 is the first record on the fresh single-partition topic (seeded at 0).
	assertBinaryCloudEvent(t, h, rec, "r0", 0)
	waitFor(t, commitTimeout, "offsets committed (zero lag)", func() bool {
		return h.lag() == 0
	})
	if err := rs.stop(t); err != nil {
		t.Errorf("service returned error on stop: %v", err)
	}
	// The clean shutdown must leave the consumer group: the deferred client.Close in
	// the consumer runs (committing final offsets) and the member departs, rather than
	// lingering until its session times out. This is the only scenario that checks it.
	h.assertGroupEmpty(t)
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
		envFunctionDeliveryRetry:           "2", // 3 attempts total
		envFunctionDeliveryBackoffDelay:    "50ms",
		envFunctionDeliveryBackoffMaxDelay: "50ms",
	})
	rs.waitReady(t, readyTimeout)

	// Produce good-before (offset 0) and poison (offset 1) only -- withhold the next
	// good record so the exhaustion commit can be observed on its own.
	h.produce("good-before", "poison")

	// The poison record exhausts its budget (3 attempts), then is skipped.
	waitFor(t, deliverTimeout, "poison attempted up to its budget", func() bool {
		return fs.attempts("poison") >= 3
	})
	// The committed offset must reach 2: good-before (0) acked and committed, poison (1)
	// skipped and committed. Observing offset 2 BEFORE the next record is produced is
	// what proves the exhaustion skip committed poison -- a later successful record's
	// commit would otherwise sweep the offset past poison and hide a missing skip.
	waitFor(t, commitTimeout, "poison's offset committed by the exhaustion skip (committed offset 2)", func() bool {
		return h.committedOffset() == 2
	})
	// Check the exact count only after the skip has committed: by now no further attempt
	// can land, so an off-by-one that allowed a 4th attempt (attempt > maxAttempts rather
	// than >=) is visible here. Checking right after the ">= 3" wait would run ~50ms after
	// the 3rd attempt and miss a 4th that arrives after its backoff.
	if got := fs.attempts("poison"); got != 3 {
		t.Errorf("poison attempted %d times, want exactly 3 (retry=2 => 3 attempts)", got)
	}

	// Only now produce the next good record; it must be delivered, confirming the
	// partition continues past the skipped poison.
	h.produce("good-after")
	waitFor(t, deliverTimeout, "good-after delivered past skipped poison", func() bool {
		return fs.attempts("good-after") >= 1
	})
	waitFor(t, commitTimeout, "all offsets committed (zero lag)", func() bool {
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
	// The record fails until this flag is flipped, not after a fixed number of
	// attempts, so recovery cannot race the lag assertion below: a count-based recovery
	// could start succeeding (and commit) in the window between observing N attempts and
	// sampling lag, which would report an unacked commit against a correct runtime.
	var failing atomic.Bool
	failing.Store(true)
	fs := newFuncServer(t, func(body string, _ int) (int, time.Duration) {
		if body == "recover" && failing.Load() {
			return 503, 0
		}
		return 200, 0
	})

	rs := h.startService(t, fs.url(), map[string]string{
		// No FUNCTION_DELIVERY_RETRY => unlimited.
		envFunctionDeliveryBackoffDelay:    "100ms",
		envFunctionDeliveryBackoffMaxDelay: "200ms",
	})
	rs.waitReady(t, readyTimeout)

	h.produce("recover")

	// Retry ~10 times at ~200ms backoff (~2s), crossing sarama's ~1s auto-commit
	// interval while the function keeps rejecting the record. This proves two things: no
	// unacked offset is committed while retrying (lag stays >= 1 across the interval,
	// below), and the default budget is not small (it reaches a 10th attempt). It does
	// not by itself prove the default is strictly unlimited -- a very large finite
	// default would also pass; the unlimited default is pinned by the unit test in
	// delivery_test.go.
	waitFor(t, deliverTimeout, "record retried ~10 times while function fails", func() bool {
		return fs.attempts("recover") >= 10
	})
	if lag := h.lag(); lag < 1 {
		t.Errorf("lag=%d while delivery still failing; unlimited retries must not commit an unacked record", lag)
	}

	// Now let the record succeed; it must be delivered (acked) and committed.
	failing.Store(false)
	waitFor(t, deliverTimeout, "record acked after function recovers", func() bool {
		return fs.successes("recover") >= 1
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
		envFunctionDeliveryBackoffDelay:    "50ms",
		envFunctionDeliveryBackoffMaxDelay: "50ms",
	}

	// First session: deliver and commit "committed" (offset 0), then start failing
	// "pending" (offset 1) under unlimited retries.
	rs := h.startService(t, fs.url(), extraEnv)
	rs.waitReady(t, readyTimeout)

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
	// assertAtMost bounds an early gap from above. The ascending lower bounds alone do
	// not prove the backoff GREW: a fixed-delay retry loop (e.g. one that always waited
	// the largest backoff) would clear every lower bound. Because the first gap is the
	// smallest expected delay, a generous ceiling on it rejects such a loop without
	// flaking on scheduling jitter (which only ever adds time).
	assertAtMost := func(t *testing.T, label string, got, want time.Duration) {
		t.Helper()
		if got > want {
			t.Errorf("%s: gap %s exceeds %s -- looks like a fixed/large delay, not growth from the base", label, got, want)
		}
	}

	t.Run("exponential", func(t *testing.T) {
		h := newHarness(t)
		const failUntil = 4
		fs := newFuncServer(t, func(body string, n int) (int, time.Duration) {
			if body == "retryme" && n <= failUntil {
				return 500, 0
			}
			return 200, 0
		})
		rs := h.startService(t, fs.url(), map[string]string{
			envFunctionDeliveryBackoffPolicy:   "exponential",
			envFunctionDeliveryBackoffDelay:    "200ms",
			envFunctionDeliveryBackoffMaxDelay: "10s",
		})
		rs.waitReady(t, readyTimeout)
		h.produce("retryme")
		waitFor(t, deliverTimeout, "record retried past the failing attempts", func() bool {
			return fs.attempts("retryme") >= failUntil+1
		})
		gaps := fs.gaps("retryme")
		if len(gaps) < failUntil {
			t.Fatalf("got %d gaps, want >= %d", len(gaps), failUntil)
		}
		// This subtest is the only guard against "exponential configured, linear
		// applied", so the ceiling that rules out linear has to have real margin. Gap 4
		// is ~1600ms exponential vs ~800ms linear; the 1600ms lower bound (1440ms after
		// the 90% slack) sits ~640ms above linear's 800ms, well clear of per-attempt
		// overhead -- unlike gap 3, whose 800ms bound (720ms after slack) is only ~120ms
		// above linear's 600ms, a margin overhead could close. The ascending lower bounds
		// also require the gaps to grow, so no separate strict-monotonic check is needed.
		assertAtLeast(t, "gap 1", gaps[0], 200*time.Millisecond)
		assertAtLeast(t, "gap 2", gaps[1], 400*time.Millisecond)
		assertAtLeast(t, "gap 3", gaps[2], 800*time.Millisecond)
		assertAtLeast(t, "gap 4", gaps[3], 1600*time.Millisecond)
		// Gap 1 is the base (~200ms). A fixed-delay loop stuck at this subtest's largest
		// backoff (gap 4, ~1600ms) would make gap 1 ~1600ms, so the 1000ms ceiling rejects
		// it with margin on both sides (real ~200ms well under it; a >=1600ms constant well
		// over it). This subtest alone would still accept a constant near 1000-1440ms, but
		// the linear subtest rejects every constant delay and backoffFor is shared, so the
		// suite as a whole does.
		assertAtMost(t, "gap 1 (growth, not fixed delay)", gaps[0], 1000*time.Millisecond)
		if err := rs.stop(t); err != nil {
			t.Errorf("service returned error on stop: %v", err)
		}
	})

	t.Run("linear", func(t *testing.T) {
		h := newHarness(t)
		// Fail enough attempts that linear and exponential diverge widely: linear
		// gives 150/300/450/600/750ms, exponential 150/300/600/1200/2400ms. Lower
		// bounds alone cannot tell them apart (exponential clears every linear lower
		// bound), so the wide upper bound on the last gap below is what proves the
		// runtime used linear and not exponential.
		const failUntil = 5
		fs := newFuncServer(t, func(body string, n int) (int, time.Duration) {
			if body == "retryme" && n <= failUntil {
				return 500, 0
			}
			return 200, 0
		})
		rs := h.startService(t, fs.url(), map[string]string{
			envFunctionDeliveryBackoffPolicy:   "linear",
			envFunctionDeliveryBackoffDelay:    "150ms",
			envFunctionDeliveryBackoffMaxDelay: "10s",
		})
		rs.waitReady(t, readyTimeout)
		h.produce("retryme")
		waitFor(t, deliverTimeout, "record retried past the failing attempts", func() bool {
			return fs.attempts("retryme") >= failUntil+1
		})
		gaps := fs.gaps("retryme")
		if len(gaps) < failUntil {
			t.Fatalf("got %d gaps, want >= %d", len(gaps), failUntil)
		}
		// The ascending lower bounds already require the gaps to grow (ruling out a
		// base-only constant delay), and the upper bound below rules out exponential, so
		// no separate strict-monotonic check is needed -- adjacent linear gaps differ by
		// only 150ms, which a single GC/scheduler pause under -race could invert on a
		// correct run.
		assertAtLeast(t, "gap 1", gaps[0], 150*time.Millisecond)
		assertAtLeast(t, "gap 2", gaps[1], 300*time.Millisecond)
		assertAtLeast(t, "gap 3", gaps[2], 450*time.Millisecond)
		assertAtLeast(t, "gap 4", gaps[3], 600*time.Millisecond)
		assertAtLeast(t, "gap 5", gaps[4], 750*time.Millisecond)
		// Gap 1 is the base (~150ms); a fixed-delay loop would make it large (e.g. a
		// constant ~750ms). The 500ms ceiling rejects that while leaving headroom.
		assertAtMost(t, "gap 1 (growth, not fixed delay)", gaps[0], 500*time.Millisecond)
		// Linear's 5th gap backoff is ~750ms; exponential's would be a hard minimum of
		// 2400ms. The runtime sleeps at least the backoff before a retry, so scheduling,
		// -race and broker round trips can only ADD to a gap, never pull it below the
		// policy's value -- an exponential 5th gap is therefore always >= 2400ms. A
		// 2000ms ceiling sits below that hard floor yet leaves ~1250ms of headroom on the
		// ~750ms linear gap, so it separates the policies without rejecting a slow but
		// correct linear run.
		if gaps[4] >= 2000*time.Millisecond {
			t.Errorf("gap 5 = %s looks exponential, not linear (linear expects ~750ms, exponential >=2400ms): %v", gaps[4], gaps)
		}
		if err := rs.stop(t); err != nil {
			t.Errorf("service returned error on stop: %v", err)
		}
	})

	t.Run("maxDelayCap", func(t *testing.T) {
		h := newHarness(t)
		const failUntil = 5
		fs := newFuncServer(t, func(body string, n int) (int, time.Duration) {
			if body == "retryme" && n <= failUntil {
				return 500, 0
			}
			return 200, 0
		})
		rs := h.startService(t, fs.url(), map[string]string{
			envFunctionDeliveryBackoffPolicy:   "exponential",
			envFunctionDeliveryBackoffDelay:    "100ms",
			envFunctionDeliveryBackoffMaxDelay: "400ms", // caps at 400ms; uncapped would reach 1600ms
		})
		rs.waitReady(t, readyTimeout)
		h.produce("retryme")
		waitFor(t, deliverTimeout, "record retried past the failing attempts", func() bool {
			return fs.attempts("retryme") >= failUntil+1
		})
		gaps := fs.gaps("retryme")
		if len(gaps) < failUntil {
			t.Fatalf("got %d gaps, want >= %d", len(gaps), failUntil)
		}
		// Uncapped exponential from 100ms would be 100, 200, 400, 800, 1600 ms; the 400ms
		// cap holds every gap from the third onward at ~400ms. Real-broker, -race and CI
		// scheduling only ever ADD time, so assert with that asymmetry in mind:
		//   - lower bound on the capped gaps (index >= 2, where backoff has reached the
		//     cap): the configured 400ms backoff is honored.
		for i := 2; i < len(gaps); i++ {
			assertAtLeast(t, "capped gap", gaps[i], 400*time.Millisecond)
		}
		//   - upper bound on the last gap, whose uncapped backoff would be a hard minimum
		//     of 1600ms (100ms*2^4). Because the runtime sleeps at least the backoff, an
		//     uncapped last gap is always >= 1600ms; a 1000ms ceiling sits below that yet
		//     leaves ~600ms of headroom on the ~400ms capped gap.
		last := len(gaps) - 1
		if gaps[last] >= 1000*time.Millisecond {
			t.Errorf("gap %d = %s is not capped: with a 400ms cap it must stay well below its uncapped value (>=1600ms): %v", last+1, gaps[last], gaps)
		}
		// No early-gap ceiling here: the only fixed-delay value that would also satisfy
		// the capped lower bound is ~400ms (the cap itself), so distinguishing it from the
		// real ~100ms first gap needs a bound too tight to be reliable under load. A
		// fixed-delay regression is global (backoffFor is shared), so the exponential and
		// linear subtests' first-gap ceilings already reject it.
		if err := rs.stop(t); err != nil {
			t.Errorf("service returned error on stop: %v", err)
		}
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
		envFunctionRequestTimeout:          "300ms",
		envFunctionDeliveryBackoffDelay:    "50ms",
		envFunctionDeliveryBackoffMaxDelay: "50ms",
	})
	rs.waitReady(t, readyTimeout)

	h.produce("slow")

	// The first slowAttempts attempts exceed the per-attempt timeout; the client gives
	// up on each (abandoned, not acked) and retries. A later fast attempt then succeeds
	// within the timeout and commits.
	waitFor(t, deliverTimeout, "record retried past timed-out attempts", func() bool {
		return fs.attempts("slow") > slowAttempts
	})
	waitFor(t, deliverTimeout, "record eventually acked within timeout", func() bool {
		return fs.successes("slow") >= 1
	})
	// Exactly the slow attempts were abandoned: this proves the timeout fired on them
	// (and that successes() did not count a still-in-flight slow attempt as the ack).
	waitFor(t, deliverTimeout, "exactly the slow attempts were abandoned", func() bool {
		return fs.abandoned("slow") == slowAttempts
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

	// Produce good0 (offset 0) and poison (offset 1) only -- withhold good1 so the skip
	// commit can be observed on its own.
	h.produce("good0", "poison")

	// poison gets a single attempt (retry=0), then is skipped.
	waitFor(t, deliverTimeout, "poison attempted once", func() bool {
		return fs.attempts("poison") >= 1
	})
	// The committed offset must reach 2: good0 (0) acked and committed, poison (1)
	// skipped and committed. Observing offset 2 BEFORE the next record is produced is
	// what proves the skip committed poison, rather than a later record's commit
	// sweeping past it.
	waitFor(t, commitTimeout, "poison's offset committed by the skip (committed offset 2)", func() bool {
		return h.committedOffset() == 2
	})
	if got := fs.attempts("poison"); got != 1 {
		t.Errorf("poison attempted %d times, want exactly 1 (retry=0 => single attempt)", got)
	}

	// Only now produce the next good record; it must be delivered past the skipped poison.
	h.produce("good1")
	waitFor(t, deliverTimeout, "good1 delivered past skipped poison", func() bool {
		return fs.attempts("good1") >= 1
	})
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

	// First session: deliver and commit a, b, c (offsets 0-2 => committed offset 3).
	rs := h.startService(t, fs.url(), nil)
	rs.waitReady(t, readyTimeout)
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

	// Produce d, e (offsets 3, 4) while NO consumer is running. A consumer that
	// resumes from the committed offset (3) delivers them; one that ignored the commit
	// and started at OffsetNewest would set its position to the high-water mark (5) and
	// never deliver d, e. Producing before the restart is what makes this prove the
	// resume -- producing after the second consumer is ready would be delivered either
	// way.
	h.produce("d", "e")

	// Second session on the same group must resume from offset 3 and deliver d, e,
	// while the committed a, b, c must not reappear.
	rs2 := h.startService(t, fs.url(), nil)
	rs2.waitReady(t, readyTimeout)
	waitFor(t, deliverTimeout, "records produced before restart delivered from committed offset", func() bool {
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

// TestE2E_RebalanceDurability (scenario 9): while a record is failing
// under unlimited retries, a second Service joins the same group and forces a
// rebalance of the single partition -- exercising two paths reachable only through a
// real group rebalance: the consume-loop re-entry (client.Consume returning and being
// called again) and the deliveryAborted path on partition revocation. Across the
// rebalance the unacked record's offset must not advance; once the function accepts
// it, it is delivered and committed.
func TestE2E_RebalanceDurability(t *testing.T) {
	h := newHarness(t)
	var failRec atomic.Bool
	failRec.Store(true)
	fs := newFuncServer(t, func(body string, _ int) (int, time.Duration) {
		if body == "rebal" && failRec.Load() {
			return 500, 0
		}
		return 200, 0
	})
	// No FUNCTION_DELIVERY_RETRY => unlimited, so the record keeps being retried across
	// the rebalance rather than being skipped and committed.
	extraEnv := map[string]string{
		envFunctionDeliveryBackoffDelay:    "50ms",
		envFunctionDeliveryBackoffMaxDelay: "50ms",
	}

	// Member A joins alone and starts retrying the failing record.
	rsA := h.startService(t, fs.url(), extraEnv)
	rsA.waitReady(t, readyTimeout)
	h.produce("rebal")
	waitFor(t, deliverTimeout, "record retried by the first member while failing", func() bool {
		return fs.attempts("rebal") >= 2
	})

	// Member B joins the same group, forcing a rebalance of partition 0.
	rsB := h.startService(t, fs.url(), extraEnv)
	rsB.waitReady(t, readyTimeout)

	// A must re-enter its consume loop after the rebalance. sarama calls Setup -- which
	// sets ready -- on the new generation even for a member with zero claims, and A's
	// previous session's Cleanup cleared ready, so ready returns to true only if consume
	// was called again. A runtime that consumed once and returned would leave A
	// not-ready here: a pod that silently stops consuming after its first rebalance
	// while its liveness probe stays green.
	waitFor(t, readyTimeout, "member A re-entered its consume loop after the rebalance", func() bool {
		return rsA.svc.ready.Load()
	})

	// The record never succeeded, so across the rebalance the committed offset must
	// stay exactly at the seeded start (0). The seed guarantees it is 0; committedOffset
	// fails the test on an errored block and returns -1 for a missing one, so the != 0
	// check catches any drift -- an advanced offset, a missing block, or an error.
	if got := h.committedOffset(); got != 0 {
		t.Errorf("committed offset=%d for a never-acked record across a rebalance; want 0 (seeded start, nothing newly committed)", got)
	}

	// Stop B while the record is still failing. The partition moves back to A, so the
	// delivery below can only happen if A is actively consuming -- not merely joined.
	if err := rsB.stop(t); err != nil {
		t.Errorf("service returned error on stop (member B): %v", err)
	}
	failRec.Store(false)
	waitFor(t, deliverTimeout, "record acked by member A after B leaves", func() bool {
		return fs.successes("rebal") >= 1
	})
	waitFor(t, commitTimeout, "offset committed after rebalance (zero lag)", func() bool {
		return h.lag() == 0
	})
	if err := rsA.stop(t); err != nil {
		t.Errorf("service returned error on stop (member A): %v", err)
	}

	// The record must be acked at least once. Delivery is at-least-once, not
	// exactly-once: a rebalance that cancels the session after the function answers but
	// before the runtime reads the reply could legitimately redeliver, so do not assert
	// a single ack -- the zero-lag wait above already confirms the offset advanced.
	if got := fs.successes("rebal"); got < 1 {
		t.Errorf("record acked %d times; want at least 1 across the rebalance", got)
	}
}

// TestE2E_InvalidConfigFailsFast (scenario 10): an invalid FUNCTION_DELIVERY_* value
// is rejected at the PROCESS level -- the built runtime binary exits non-zero with a
// message naming the offending variable, before the function-target wait. Running the
// real binary is what distinguishes this from the loader unit tests (which already
// cover the negative-value rejection via an in-process call) and answers #30's "confirm
// at the process level".
func TestE2E_InvalidConfigFailsFast(t *testing.T) {
	// Gated on a broker only to keep the whole suite behind one switch; this scenario
	// never connects to a broker (it fails on config first).
	requireBrokers(t)

	// Build the runtime binary from the module root (the test runs in ./kafka).
	bin := filepath.Join(t.TempDir(), "fkafka-runtime")
	build := exec.Command("go", "build", "-o", bin, "./cmd/fkafka-runtime")
	build.Dir = ".."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building fkafka-runtime: %v\n%s", err, out)
	}

	// Point FUNCTION_TARGET at a port known to be closed (reserve one, then release it),
	// so a regression that moved config validation after the function-target wait would
	// hang on that wait and be caught by the timeout below -- rather than connecting at
	// once to whatever happens to listen on the default 127.0.0.1:8080 and passing.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a closed port: %v", err)
	}
	closedTarget := "http://" + ln.Addr().String() + "/"
	_ = ln.Close()

	// Run with a controlled env (not os.Environ) so an inherited FUNCTION_* value
	// cannot make the process fail for the wrong reason. Unlimited is expressed by
	// omitting FUNCTION_DELIVERY_RETRY; a negative value is a typo the runtime must
	// reject rather than silently treat as "retry forever".
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin)
	cmd.Env = []string{
		EnvFunctionTarget + "=" + closedTarget,
		envKafkaBrokers + "=localhost:9092",
		envKafkaTopic + "=irrelevant",
		envKafkaConsumerGroup + "=irrelevant",
		envFunctionDeliveryRetry + "=-1",
	}

	start := time.Now()
	out, err := cmd.CombinedOutput()
	elapsed := time.Since(start)

	if ctx.Err() != nil {
		t.Fatalf("process did not exit within the timeout; it must fail fast on bad config. output:\n%s", out)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("process did not exit non-zero (err=%v); want rejection of the invalid config. output:\n%s", err, out)
	}
	if code := exitErr.ExitCode(); code != 1 {
		t.Errorf("process exit code = %d, want 1. output:\n%s", code, out)
	}
	if !strings.Contains(string(out), "FUNCTION_DELIVERY_RETRY") {
		t.Errorf("process output does not name the offending variable FUNCTION_DELIVERY_RETRY:\n%s", out)
	}
	if elapsed > 10*time.Second {
		t.Errorf("process took %s to reject bad config; it must fail before the function-target wait", elapsed)
	}
}

// TestE2E_NewGroupStartsAtNewest (scenario 11, default start position): a brand-new group with no
// committed offset starts at OffsetNewest (consumer.go), so a record produced before
// the consumer resolves its start position is NOT delivered, while records produced
// afterward are. This is the runtime's real first-deploy behavior, which the seeded
// scenarios deliberately bypass -- switching the runtime to OffsetOldest (replaying a
// topic's whole history on first deploy) would fail here. It uses an unseeded harness.
func TestE2E_NewGroupStartsAtNewest(t *testing.T) {
	h := newHarnessUnseeded(t)
	fs := newFuncServer(t, alwaysOK)

	// Produce before any consumer exists. At OffsetNewest this record (offset 0) is
	// below the start position and must never be delivered.
	h.produce("old")

	rs := h.startService(t, fs.url(), nil)
	rs.waitReady(t, readyTimeout)

	// Produce new records until one is delivered. Only a record produced after the
	// consumer resolved its OffsetNewest start position is delivered; this pacing loop
	// ends as soon as consumption is observed.
	var delivered bool
	for i := 0; i < 150 && !delivered; i++ {
		h.produce(fmt.Sprintf("new-%d", i))
		time.Sleep(100 * time.Millisecond)
		delivered = fs.totalRequests() > 0
	}
	if !delivered {
		t.Fatal("no record was delivered; the consumer never began consuming")
	}

	// Drain to zero lag, then assert the pre-start record was skipped.
	waitFor(t, commitTimeout, "committed past the delivered records (zero lag)", func() bool {
		return h.lag() == 0
	})
	if got := fs.attempts("old"); got != 0 {
		t.Errorf("record produced before the consumer resolved its start position was delivered %d times; OffsetNewest must skip it", got)
	}
	if err := rs.stop(t); err != nil {
		t.Errorf("service returned error on stop: %v", err)
	}
}
