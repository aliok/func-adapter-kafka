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

	h.produce("good-before", "poison", "good-after")

	// The poison record exhausts its budget (3 attempts), is skipped, and the
	// partition advances to the following good record.
	waitFor(t, deliverTimeout, "poison attempted up to its budget", func() bool {
		return fs.attempts("poison") >= 3
	})
	// A partition is consumed one record at a time -- the handler blocks in deliver
	// until poison is skipped -- so good-after cannot arrive until poison is done.
	// Once it has, poison's attempt count is final; no extra wait is needed.
	waitFor(t, deliverTimeout, "good-after delivered past skipped poison", func() bool {
		return fs.attempts("good-after") >= 1
	})
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
	// Keep failing for many attempts over a window longer than sarama's ~1s
	// auto-commit interval, so this tells unlimited apart from any finite budget (not
	// just budgets under 4) and the lag check below cannot pass merely because a
	// wrongly-marked offset has not been flushed yet.
	const failUntil = 12
	fs := newFuncServer(t, func(body string, n int) (int, time.Duration) {
		if body == "recover" && n <= failUntil {
			return 503, 0
		}
		return 200, 0
	})

	rs := h.startService(t, fs.url(), map[string]string{
		// No FUNCTION_DELIVERY_RETRY => unlimited.
		envFunctionDeliveryBackoffDel: "100ms",
		envFunctionDeliveryBackoffMax: "200ms",
	})
	rs.waitReady(t, readyTimeout)

	h.produce("recover")

	// Retry ~10 times at ~200ms backoff (~2s), crossing the auto-commit interval
	// while the function keeps rejecting the record. A finite budget of fewer than 4
	// retries would have given up by now; unlimited keeps going.
	waitFor(t, deliverTimeout, "record retried ~10 times while function fails", func() bool {
		return fs.attempts("recover") >= 10
	})
	if lag := h.lag(); lag < 1 {
		t.Errorf("lag=%d while delivery still failing; unlimited retries must not commit an unacked record", lag)
	}

	// Once the function starts accepting it (after failUntil), the record is delivered
	// and committed.
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
		h.produce("retryme")
		waitFor(t, deliverTimeout, "record retried past the failing attempts", func() bool {
			return fs.attempts("retryme") >= failUntil+1
		})
		gaps := fs.gaps("retryme")
		if len(gaps) < 3 {
			t.Fatalf("got %d gaps, want >= 3", len(gaps))
		}
		// The 800ms lower bound on gap 3 also rules out linear growth (which would
		// give 600ms there), so this subtest fails if the runtime ignored the policy
		// and used linear.
		assertAtLeast(t, "gap 1", gaps[0], 200*time.Millisecond)
		assertAtLeast(t, "gap 2", gaps[1], 400*time.Millisecond)
		assertAtLeast(t, "gap 3", gaps[2], 800*time.Millisecond)
		if !(gaps[1] > gaps[0] && gaps[2] > gaps[1]) {
			t.Errorf("gaps not growing exponentially: %v", gaps)
		}
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
			envFunctionDeliveryBackoffPol: "linear",
			envFunctionDeliveryBackoffDel: "150ms",
			envFunctionDeliveryBackoffMax: "10s",
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
		assertAtLeast(t, "gap 1", gaps[0], 150*time.Millisecond)
		assertAtLeast(t, "gap 2", gaps[1], 300*time.Millisecond)
		assertAtLeast(t, "gap 3", gaps[2], 450*time.Millisecond)
		assertAtLeast(t, "gap 4", gaps[3], 600*time.Millisecond)
		assertAtLeast(t, "gap 5", gaps[4], 750*time.Millisecond)
		if !(gaps[1] > gaps[0] && gaps[2] > gaps[1] && gaps[3] > gaps[2] && gaps[4] > gaps[3]) {
			t.Errorf("gaps not growing monotonically: %v", gaps)
		}
		// Linear's 5th gap is ~750ms; exponential's would be ~2400ms. A wide upper
		// bound here (room for scheduling jitter, which only adds time) fails if the
		// runtime grew the backoff exponentially instead of linearly.
		if gaps[4] >= 1500*time.Millisecond {
			t.Errorf("gap 5 = %s looks exponential, not linear (linear expects ~750ms): %v", gaps[4], gaps)
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
			envFunctionDeliveryBackoffPol: "exponential",
			envFunctionDeliveryBackoffDel: "200ms",
			envFunctionDeliveryBackoffMax: "400ms", // caps at 400ms; uncapped would reach 1600ms+
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
		// Uncapped exponential from 200ms would be 200, 400, 800, 1600, 3200 ms; the
		// 400ms cap holds every gap from the third onward near 400ms. Real-broker and
		// CI scheduling only ever ADD time, so assert with that asymmetry in mind:
		//   - lower bound on the capped gaps: the configured 400ms backoff is honored
		//     (a retry can never arrive sooner than its backoff);
		//   - upper bound ONLY on the gaps whose uncapped value (>=1600ms) sits far
		//     above the cap, so scheduling jitter cannot be mistaken for uncapped
		//     growth. A capped ~400ms gap stays well under 1000ms even under load,
		//     while an uncapped gap here would be 1600ms or 3200ms. The earlier gaps
		//     (uncapped 800ms) are too close to the cap to bound without flaking.
		for i := 2; i < len(gaps); i++ {
			assertAtLeast(t, "capped gap", gaps[i], 400*time.Millisecond)
		}
		for i := 3; i < len(gaps); i++ {
			if gaps[i] >= 1000*time.Millisecond {
				t.Errorf("gap %d = %s is not capped: with a 400ms cap it must stay well below its uncapped value (>=1600ms)", i+1, gaps[i])
			}
		}
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
		envFunctionRequestTimeout:     "300ms",
		envFunctionDeliveryBackoffDel: "50ms",
		envFunctionDeliveryBackoffMax: "50ms",
	})
	rs.waitReady(t, readyTimeout)

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

	h.produce("good0", "poison", "good1")

	// good1 (produced after poison) can only be delivered once poison's single
	// attempt is done and its offset committed -- the partition is consumed one
	// record at a time -- so poison's count is final here; no extra wait is needed.
	waitFor(t, deliverTimeout, "poison attempted and good1 delivered", func() bool {
		return fs.attempts("poison") >= 1 && fs.attempts("good1") >= 1
	})
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

// TestE2E_RebalanceDurability (scenario 8, rebalance half): while a record is failing
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
		envFunctionDeliveryBackoffDel: "50ms",
		envFunctionDeliveryBackoffMax: "50ms",
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

	// The record never succeeded, so across the rebalance its offset must not advance
	// past the seeded start (0). A committed offset > 0 would mean an unacked record
	// was marked.
	if got := h.committedOffset(); got > 0 {
		t.Errorf("committed offset=%d for a never-acked record across a rebalance; want <= 0 (nothing committed)", got)
	}

	// Let whichever member now owns the partition deliver it successfully.
	failRec.Store(false)
	waitFor(t, deliverTimeout, "record acked after rebalance", func() bool {
		return fs.successes("rebal") >= 1
	})
	waitFor(t, commitTimeout, "offset committed after rebalance (zero lag)", func() bool {
		return h.lag() == 0
	})

	if err := rsB.stop(t); err != nil {
		t.Errorf("service returned error on stop (member B): %v", err)
	}
	if err := rsA.stop(t); err != nil {
		t.Errorf("service returned error on stop (member A): %v", err)
	}

	// The record was rejected until after the rebalance settled, so exactly one member
	// delivered it successfully -- no duplicate ack despite the at-least-once churn.
	if got := fs.successes("rebal"); got != 1 {
		t.Errorf("record acked %d times; want exactly 1 across the rebalance", got)
	}
}

// TestE2E_InvalidConfigFailsFast (scenario 9): an invalid FUNCTION_DELIVERY_* value
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

	// Run with a controlled env (not os.Environ) so an inherited FUNCTION_* value
	// cannot make the process fail for the wrong reason. Unlimited is expressed by
	// omitting FUNCTION_DELIVERY_RETRY; a negative value is a typo the runtime must
	// reject rather than silently treat as "retry forever".
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin)
	cmd.Env = []string{
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
