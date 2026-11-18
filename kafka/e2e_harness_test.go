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

// Package-level end-to-end tests that drive the runtime against a REAL Kafka
// broker rather than mocks. They are compiled only under the `e2e` build tag and
// skipped unless KAFKA_E2E_BOOTSTRAP names a reachable broker, so the ordinary
// `go test ./...` unit run (and the stock CI) never touches a broker.
//
// Why a real broker: the unit tests prove the delivery/retry state machine
// against a fake adapter, but they cannot prove that the whole consume loop --
// consumer-group assignment, offset commit semantics, redelivery from the last
// committed offset, clean group close on shutdown -- behaves correctly against a
// live Kafka. These tests exercise exactly that: produce real records, drive the
// public entry point (New + Start) pointed at a scripted HTTP function, and
// assert on committed offsets, consumer lag, and per-record delivery attempts.
//
// The suite does not stand up a broker itself: point it at any Kafka -- a local
// container, docker-compose, or a Strimzi cluster -- via KAFKA_E2E_BOOTSTRAP, and
// run `make test-e2e`.
package kafka

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// bootstrapEnv names the env var carrying the Kafka bootstrap address(es)
// (comma-separated) the suite runs against. Unset means skip.
const bootstrapEnv = "KAFKA_E2E_BOOTSTRAP"

// The env-var names the runtime reads are the contract constants from contract.go
// (exported EnvFunctionTarget / EnvListenAddress, unexported env* for the rest);
// this suite uses those directly rather than redeclaring them.

// runtimeOverrideEnvVars are the delivery/timeout/concurrency variables a scenario
// drives via extraEnv. startService clears any inherited value before applying the
// scenario's own, so a value already in the caller's environment (CI, a developer
// shell) cannot leak in and silently change a scenario -- e.g. an inherited
// FUNCTION_DELIVERY_RETRY would corrupt the unlimited-retry default scenario, and
// an inherited invalid FUNCTION_REQUEST_TIMEOUT or FUNCTION_MAX_CONCURRENCY would
// make New fail before the test even runs.
var runtimeOverrideEnvVars = []string{
	envFunctionDeliveryRetry,
	envFunctionDeliveryBackoffPolicy,
	envFunctionDeliveryBackoffDelay,
	envFunctionDeliveryBackoffMaxDelay,
	envFunctionRequestTimeout,
	envFunctionMaxConcurrency,
}

// runID distinguishes topic and consumer-group names across separate `go test`
// runs against the same (possibly long-lived) broker, so a run that was killed
// before its cleanup ran cannot collide with a later run on the same name
// (admin.CreateTopic would otherwise fail with TopicAlreadyExists).
var runID = strconv.FormatInt(time.Now().UnixNano(), 36)

// topicSeq makes topic/group names unique across scenarios within one run. The
// suite is strictly sequential (t.Setenv forbids parallel tests), so a plain
// counter suffices.
var topicSeq int64

// requireBrokers returns the configured bootstrap brokers or skips the test.
func requireBrokers(t *testing.T) []string {
	t.Helper()
	v := strings.TrimSpace(os.Getenv(bootstrapEnv))
	if v == "" {
		t.Skipf("%s not set; skipping real-broker E2E (set it to a Kafka bootstrap address, e.g. localhost:9092, to run)", bootstrapEnv)
	}
	return splitAndTrim(v)
}

// uniqueName builds a broker-safe, collision-free name from the test name, a
// per-process run ID, and a per-run counter.
func uniqueName(t *testing.T, prefix string) string {
	n := atomic.AddInt64(&topicSeq, 1)
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		default:
			return '-'
		}
	}, t.Name())
	return prefix + "-" + safe + "-" + runID + "-" + strconv.FormatInt(n, 10)
}

// harness owns the broker-side fixtures for one scenario: a dedicated topic and
// consumer group, a producer, and admin/client handles for offset and lag
// assertions. Everything is torn down via t.Cleanup.
type harness struct {
	t        *testing.T
	brokers  []string
	topic    string
	group    string
	producer sarama.SyncProducer
	client   sarama.Client
	admin    sarama.ClusterAdmin
}

// newHarness builds the fixtures and seeds the group's start offset to 0, so the
// runtime reads deterministically from the beginning regardless of when records are
// produced. Most scenarios want this. Use newHarnessUnseeded to exercise the
// runtime's real new-group start position (OffsetNewest).
func newHarness(t *testing.T) *harness {
	h := newHarnessUnseeded(t)
	h.seedStartOffset()
	return h
}

func newHarnessUnseeded(t *testing.T) *harness {
	t.Helper()
	brokers := requireBrokers(t)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_0_0_0
	cfg.Producer.Return.Successes = true
	// Apply the same TLS/SASL settings the runtime uses (read from
	// KAFKA_SECURITY_PROTOCOL / KAFKA_TLS_* / KAFKA_SASL_*) so the fixtures -- admin,
	// producer, client -- can reach a secured broker too. Without this the
	// "broker-agnostic" suite could only ever run against a PLAINTEXT broker, even
	// though the runtime under test supports secured ones. For the default
	// (unset/PLAINTEXT) case configureSecurity is a no-op.
	if err := configureSecurity(cfg); err != nil {
		t.Fatalf("configuring fixture security from KAFKA_* env: %v", err)
	}

	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		t.Fatalf("creating cluster admin: %v", err)
	}
	topic := uniqueName(t, "e2e")
	// One partition keeps offset/lag reasoning simple: the whole suite asserts on
	// partition 0.
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		_ = admin.Close()
		t.Fatalf("creating topic %s: %v", topic, err)
	}

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		_ = admin.DeleteTopic(topic)
		_ = admin.Close()
		t.Fatalf("creating producer: %v", err)
	}
	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		_ = producer.Close()
		_ = admin.DeleteTopic(topic)
		_ = admin.Close()
		t.Fatalf("creating client: %v", err)
	}

	h := &harness{
		t:        t,
		brokers:  brokers,
		topic:    topic,
		group:    uniqueName(t, "grp"),
		producer: producer,
		client:   client,
		admin:    admin,
	}
	t.Cleanup(func() {
		_ = producer.Close()
		// Delete the consumer group so a long-lived broker (docker-compose, Strimzi)
		// does not accumulate a grp-* entry per scenario. Safe here because harness
		// cleanup runs after the per-service cleanups (t.Cleanup is LIFO), so the
		// group has no remaining members by now.
		_ = admin.DeleteConsumerGroup(h.group)
		_ = client.Close()
		_ = admin.DeleteTopic(topic)
		_ = admin.Close()
	})
	return h
}

// seedStartOffset commits a starting offset of 0 for the harness group on partition
// 0 before any consumer joins. The runtime consumes from OffsetNewest, which for a
// group with no committed offset would skip any record produced before the consumer
// finishes resolving its start position -- a real race the suite otherwise has to
// paper over with a fixed sleep. Committing 0 up front makes the runtime resume from
// the beginning deterministically, so records may be produced at any time (before or
// after the consumer is ready) and none is skipped.
//
// The first group operation against a fresh broker creates the internal
// __consumer_offsets topic, whose partitions answer NOT_COORDINATOR /
// OffsetsLoadInProgress until they finish loading. sarama's ManagePartition retries
// only a few times with no backoff, inside that window, so the seed is retried here
// until the coordinator is ready. Commit() reports errors only to sarama's discarded
// logger, so the committed offset is read back to confirm the commit actually landed
// -- otherwise a silently-failed commit would bring back the OffsetNewest race this
// seed exists to remove.
func (h *harness) seedStartOffset() {
	h.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := h.trySeedStartOffset()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("seeding start offset: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (h *harness) trySeedStartOffset() error {
	om, err := sarama.NewOffsetManagerFromClient(h.group, h.client)
	if err != nil {
		return err
	}
	defer func() { _ = om.Close() }()
	pom, err := om.ManagePartition(h.topic, 0)
	if err != nil {
		return err
	}
	defer func() { _ = pom.Close() }()
	// MarkOffset sets the next offset to consume. A group with no prior commit fetches
	// an initial offset of -1, so MarkOffset(0) is a forward move and takes effect.
	pom.MarkOffset(0, "")
	om.Commit()

	resp, err := h.admin.ListConsumerGroupOffsets(h.group, map[string][]int32{h.topic: {0}})
	if err != nil {
		return err
	}
	block := resp.GetBlock(h.topic, 0)
	if block == nil || block.Err != sarama.ErrNoError || block.Offset != 0 {
		return fmt.Errorf("commit did not land (block=%+v); retrying", block)
	}
	return nil
}

// produce sends each value as a record to the harness topic, in order.
func (h *harness) produce(values ...string) {
	h.t.Helper()
	for _, v := range values {
		if _, _, err := h.producer.SendMessage(&sarama.ProducerMessage{
			Topic: h.topic,
			Value: sarama.StringEncoder(v),
		}); err != nil {
			h.t.Fatalf("producing %q: %v", v, err)
		}
	}
}

// committedOffset returns the group's committed offset for partition 0 (the next
// offset to be consumed), or -1 if the group has committed nothing yet.
func (h *harness) committedOffset() int64 {
	h.t.Helper()
	resp, err := h.admin.ListConsumerGroupOffsets(h.group, map[string][]int32{h.topic: {0}})
	if err != nil {
		h.t.Fatalf("listing group offsets: %v", err)
	}
	block := resp.GetBlock(h.topic, 0)
	if block == nil {
		return -1
	}
	if block.Err != sarama.ErrNoError {
		h.t.Fatalf("listing group offsets: partition block error: %v", block.Err)
	}
	return block.Offset
}

// assertGroupEmpty asserts the consumer group has no members and is in the Empty
// state. Called right after a clean stop, it proves the member left the group (the
// deferred client.Close in the consumer ran, committing final offsets) rather than
// merely going silent until its session times out. A regression that dropped
// client.Close or the consumeDone join on shutdown would leave the member present
// here -- the broker does not expire it until the session timeout (~10s), long after
// this check runs.
func (h *harness) assertGroupEmpty(t *testing.T) {
	t.Helper()
	descs, err := h.admin.DescribeConsumerGroups([]string{h.group})
	if err != nil {
		t.Fatalf("describing consumer group: %v", err)
	}
	if len(descs) != 1 {
		t.Fatalf("describing consumer group: got %d descriptions, want 1", len(descs))
	}
	d := descs[0]
	if d.State != "Empty" || len(d.Members) != 0 {
		t.Errorf("consumer group after clean stop: state=%q, members=%d; want Empty with 0 members (the member must leave the group on shutdown, not expire by session timeout)", d.State, len(d.Members))
	}
}

// highWaterMark returns the next offset that will be assigned on partition 0,
// i.e. the count of records produced so far.
func (h *harness) highWaterMark() int64 {
	h.t.Helper()
	off, err := h.client.GetOffset(h.topic, 0, sarama.OffsetNewest)
	if err != nil {
		h.t.Fatalf("reading high-water mark: %v", err)
	}
	return off
}

// lag returns the number of produced-but-not-yet-committed records on partition
// 0. With nothing committed, every produced record counts as lag.
func (h *harness) lag() int64 {
	committed := h.committedOffset()
	if committed < 0 {
		return h.highWaterMark()
	}
	return h.highWaterMark() - committed
}

// runningService is a Service started in the background, stoppable by cancelling
// its context (which triggers the runtime's clean consumer-group close).
type runningService struct {
	svc      *Service
	errc     chan error
	cancel   context.CancelFunc
	stopOnce sync.Once
	stopErr  error
}

// startService configures the runtime's env from the harness, applies any extra
// delivery env, and starts New+Start in the background. The health server binds
// an ephemeral port so sequential scenarios never collide on it.
func (h *harness) startService(t *testing.T, target string, extraEnv map[string]string) *runningService {
	t.Helper()
	t.Setenv(envKafkaBrokers, strings.Join(h.brokers, ","))
	t.Setenv(envKafkaTopic, h.topic)
	t.Setenv(envKafkaConsumerGroup, h.group)
	t.Setenv(EnvListenAddress, "127.0.0.1:0")
	// Start each scenario from a clean slate: unset any inherited runtime override so
	// only the scenario's extraEnv applies. t.Setenv records the original for
	// restoration; os.Unsetenv then truly removes it -- a present-but-empty
	// FUNCTION_DELIVERY_RETRY is itself a startup error, so it must be unset, not
	// blanked.
	for _, k := range runtimeOverrideEnvVars {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
	for k, v := range extraEnv {
		t.Setenv(k, v)
	}

	svc, err := New(target)
	if err != nil {
		t.Fatalf("creating service: %v", err)
	}
	svc.targetWaitTimeout = 30 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- svc.Start(ctx) }()
	rs := &runningService{svc: svc, errc: errc, cancel: cancel}
	// Ensure the Service is always torn down, even when a waitFor failure aborts the
	// scenario with t.Fatal before it reaches an explicit stop -- otherwise the
	// consumer and health server keep running after their topic is deleted and their
	// function server is closed, and their errors land in later tests' logs. stop is
	// idempotent (sync.Once), so an explicit stop still works.
	t.Cleanup(func() {
		// Surface an early/last error from Start rather than discarding it: if a waitFor
		// aborted the scenario, this is where Start's real error (e.g. a consumer error)
		// first becomes visible.
		if err := rs.stop(t); err != nil {
			t.Logf("service returned error on cleanup stop: %v", err)
		}
	})
	return rs
}

// waitReady blocks until the consumer reports ready (partitions assigned). If Start
// returns before then -- an early failure such as a consumer error -- it fails
// immediately with that error instead of waiting out the timeout with an opaque
// "timed out" message.
func (rs *runningService) waitReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if rs.svc.ready.Load() {
			return
		}
		select {
		case err := <-rs.errc:
			// Start exited before becoming ready. Record the result so stop() (and the
			// registered cleanup) does not block re-reading errc, then fail with the real
			// cause.
			rs.stopOnce.Do(func() { rs.stopErr = err })
			t.Fatalf("service exited before becoming ready: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for consumer ready (partitions assigned)", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// stop cancels the service context and waits for Start to return, which includes
// the runtime's clean consumer-group close (committing final offsets). It returns
// Start's error. It is idempotent: the registered t.Cleanup also calls it, and any
// call after the first returns the first result without blocking.
func (rs *runningService) stop(t *testing.T) error {
	t.Helper()
	rs.stopOnce.Do(func() {
		rs.cancel()
		select {
		case rs.stopErr = <-rs.errc:
		case <-time.After(45 * time.Second):
			t.Error("service did not stop within 45s of context cancel")
		}
	})
	return rs.stopErr
}

// funcServer is a scripted HTTP CloudEvents receiver standing in for the user
// function. Each delivery arrives as a binary-mode CloudEvent whose body is the
// raw record value, so scenarios key their response script on the produced
// string. Every request is recorded (arrival time, the status returned, the
// request headers, and whether the client abandoned the attempt) for assertions on
// attempt counts, ordering, backoff timing, and CloudEvent metadata.
type funcServer struct {
	srv     *httptest.Server
	respond func(body string, attempt int) (status int, delay time.Duration)

	mu     sync.Mutex
	counts map[string]int
	log    []reqRecord
}

type reqRecord struct {
	body   string
	at     time.Time
	status int
	header http.Header
	// done is set once the handler has finished (after any scripted delay). Until then
	// the attempt is still in flight and must not count as a success -- a slow handler
	// logged with a 2xx has not yet been received by the client.
	done bool
	// abandoned is true when the client (the runtime) gave up on the attempt before
	// the handler responded -- i.e. the request context was cancelled during the
	// scripted delay, as a per-attempt timeout does. Such an attempt is not a
	// delivered ack even if its scripted status is 2xx.
	abandoned bool
}

// newFuncServer starts a scripted receiver. respond is called per request with
// the record body and the 1-based attempt number for that body; it returns the
// HTTP status to send and an optional delay to sleep before responding (used to
// exceed a per-attempt timeout).
func newFuncServer(t *testing.T, respond func(body string, attempt int) (status int, delay time.Duration)) *funcServer {
	t.Helper()
	fs := &funcServer{respond: respond, counts: map[string]int{}}
	fs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		at := time.Now()
		b, _ := io.ReadAll(r.Body)
		body := string(b)
		// Clone the headers so the CloudEvent metadata the runtime mapped onto the
		// record (the Ce-* context attributes and Content-Type) can be asserted
		// later, not just the body.
		hdr := r.Header.Clone()

		// Publish the attempt count and its arrival record in ONE critical section, so a
		// reader that sees the incremented count via attempts() always finds the matching
		// fs.log entry too. The backoff subtests wait on attempts() and then read gaps();
		// incrementing the count and appending the record separately would let a
		// scheduling pause between them drop a gap. Appending here (before the scripted
		// delay) also keeps gaps()/arrivals() in arrival order when a slow handler is
		// still sleeping as the next retry arrives. The scripted status is filled in
		// after respond, and done/abandoned after the delay, but those are only read via
		// successes(), which requires done.
		fs.mu.Lock()
		fs.counts[body]++
		n := fs.counts[body]
		idx := len(fs.log)
		fs.log = append(fs.log, reqRecord{body: body, at: at, header: hdr})
		fs.mu.Unlock()

		status, delay := fs.respond(body, n)
		fs.mu.Lock()
		fs.log[idx].status = status
		fs.mu.Unlock()

		if delay > 0 {
			time.Sleep(delay)
		}
		// Mark the attempt complete once the handler has finished. If the client gave up
		// before we responded -- e.g. a per-attempt timeout elapsed during the delay
		// above -- its request context is cancelled, so record the attempt as abandoned:
		// successes() counts only completed, non-abandoned 2xx responses, so neither an
		// in-flight slow attempt nor one the runtime timed out on is mistaken for an ack.
		fs.mu.Lock()
		fs.log[idx].done = true
		fs.log[idx].abandoned = r.Context().Err() != nil
		fs.mu.Unlock()

		w.WriteHeader(status)
	}))
	t.Cleanup(fs.srv.Close)
	return fs
}

// url is the target to hand the runtime (trailing slash, as the sidecar uses).
func (fs *funcServer) url() string { return fs.srv.URL + "/" }

// attempts returns how many times body has been delivered (any status).
func (fs *funcServer) attempts(body string) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.counts[body]
}

// successes returns how many deliveries of body the runtime actually received as a
// 2xx ack -- i.e. completed, non-abandoned 2xx responses. An attempt still in flight
// (handler sleeping), or one the client timed out on before the handler responded,
// does not count even though its scripted status is 2xx.
func (fs *funcServer) successes(body string) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n := 0
	for _, r := range fs.log {
		if r.body == body && r.done && !r.abandoned && r.status >= 200 && r.status < 300 {
			n++
		}
	}
	return n
}

// abandoned returns how many delivery attempts of body the client gave up on before
// the handler responded (a per-attempt timeout).
func (fs *funcServer) abandoned(body string) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n := 0
	for _, r := range fs.log {
		if r.body == body && r.abandoned {
			n++
		}
	}
	return n
}

// firstDelivery returns the recorded request for the first delivery of body and
// whether any delivery of body has been received. Used to assert the CloudEvent
// metadata the runtime mapped onto at least one record.
func (fs *funcServer) firstDelivery(body string) (reqRecord, bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for _, r := range fs.log {
		if r.body == body {
			return r, true
		}
	}
	return reqRecord{}, false
}

// totalRequests returns the number of deliveries received across all records.
func (fs *funcServer) totalRequests() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return len(fs.log)
}

// arrivals returns the arrival times of each delivery of body, in order.
func (fs *funcServer) arrivals(body string) []time.Time {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var out []time.Time
	for _, r := range fs.log {
		if r.body == body {
			out = append(out, r.at)
		}
	}
	return out
}

// gaps returns the intervals between successive deliveries of body (len = number
// of deliveries - 1), i.e. the observed inter-attempt backoff.
func (fs *funcServer) gaps(body string) []time.Duration {
	arr := fs.arrivals(body)
	if len(arr) < 2 {
		return nil
	}
	gaps := make([]time.Duration, 0, len(arr)-1)
	for i := 1; i < len(arr); i++ {
		gaps = append(gaps, arr[i].Sub(arr[i-1]))
	}
	return gaps
}

// waitFor polls cond until it is true or timeout elapses, failing the test on
// timeout. Real-broker timing varies, so scenarios wait on observable state
// rather than sleeping a fixed duration.
func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, desc)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// assertBinaryCloudEvent verifies a delivery carried the binary-mode CloudEvent the
// runtime maps a plain (non-CloudEvent) Kafka record to -- see kafkaMessageToEvent.
// The context attributes travel as Ce-* HTTP headers and the record value is the
// body with an application/octet-stream content type. This proves scenario 1's
// CloudEvent mapping end to end: a regression in kafkaMessageToEvent or the binary
// encoding would otherwise pass a body-only check. wantOffset is the record's Kafka
// offset, which the runtime encodes into the event ID and the kafka* extensions; the
// caller knows it because each scenario produces to a fresh single-partition topic
// seeded at offset 0.
func assertBinaryCloudEvent(t *testing.T, h *harness, rec reqRecord, wantBody string, wantOffset int64) {
	t.Helper()
	if rec.body != wantBody {
		t.Errorf("delivered body = %q, want %q", rec.body, wantBody)
	}
	off := strconv.FormatInt(wantOffset, 10)
	checks := map[string]string{
		"Ce-Specversion":    "1.0",
		"Ce-Type":           "dev.knative.kafka.event",
		"Ce-Source":         "kafka://" + strings.Join(h.brokers, ",") + "/" + h.topic,
		"Ce-Id":             "partition:0/offset:" + off,
		"Ce-Kafkatopic":     h.topic,
		"Ce-Kafkapartition": "0",
		"Ce-Kafkaoffset":    off,
		"Content-Type":      "application/octet-stream",
	}
	for header, want := range checks {
		if got := rec.header.Get(header); got != want {
			t.Errorf("delivered %s = %q, want %q", header, got, want)
		}
	}
}
