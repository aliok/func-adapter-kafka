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
// The suite does not stand up a broker itself (see the chosen design): point it
// at any Kafka -- a local container, docker-compose, or a Strimzi cluster -- via
// KAFKA_E2E_BOOTSTRAP, and run `make test-e2e`.
package kafka

import (
	"context"
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

// Env var names the runtime reads. Duplicated here as literals rather than
// imported, so the suite builds against main (which reads these as literals) and
// does not depend on the separate env-name-constant refactor.
const (
	envKafkaBrokers               = "KAFKA_BROKERS"
	envKafkaTopic                 = "KAFKA_TOPIC"
	envKafkaConsumerGroup         = "KAFKA_CONSUMER_GROUP"
	envListenAddress              = "LISTEN_ADDRESS"
	envFunctionDeliveryRetry      = "FUNCTION_DELIVERY_RETRY"
	envFunctionDeliveryBackoffPol = "FUNCTION_DELIVERY_BACKOFF_POLICY"
	envFunctionDeliveryBackoffDel = "FUNCTION_DELIVERY_BACKOFF_DELAY"
	envFunctionDeliveryBackoffMax = "FUNCTION_DELIVERY_BACKOFF_MAX_DELAY"
	envFunctionRequestTimeout     = "FUNCTION_REQUEST_TIMEOUT"
)

// runtimeOverrideEnvVars are the delivery/timeout variables a scenario drives via
// extraEnv. startService clears any inherited value before applying the scenario's
// own, so a value already in the caller's environment (CI, a developer shell)
// cannot leak in and silently change a scenario -- e.g. an inherited
// FUNCTION_DELIVERY_RETRY would corrupt the unlimited-retry default scenario, and
// an inherited invalid FUNCTION_REQUEST_TIMEOUT would make New fail before the test
// even runs.
var runtimeOverrideEnvVars = []string{
	envFunctionDeliveryRetry,
	envFunctionDeliveryBackoffPol,
	envFunctionDeliveryBackoffDel,
	envFunctionDeliveryBackoffMax,
	envFunctionRequestTimeout,
}

// settleAfterReady is a short pause between the consumer reporting ready
// (partitions assigned) and the test producing records. The runtime consumes
// from OffsetNewest, so a record produced before the fetch position is
// established would be skipped; the settle closes that race without coupling the
// test to sarama internals. Kept small so it adds little to each scenario.
const settleAfterReady = 1 * time.Second

// topicSeq makes topic/group names unique across scenarios in one run. The
// suite is strictly sequential (t.Setenv forbids parallel tests), so a plain
// counter suffices and keeps names deterministic.
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

// uniqueName builds a broker-safe, collision-free name from the test name and a
// per-run counter.
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
	return prefix + "-" + safe + "-" + strconv.FormatInt(n, 10)
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

func newHarness(t *testing.T) *harness {
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
		_ = client.Close()
		_ = admin.DeleteTopic(topic)
		_ = admin.Close()
	})
	return h
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
	return block.Offset
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
	svc    *Service
	errc   chan error
	cancel context.CancelFunc
}

// startService configures the runtime's env from the harness, applies any extra
// delivery env, and starts New+Start in the background. The health server binds
// an ephemeral port so sequential scenarios never collide on it.
func (h *harness) startService(t *testing.T, target string, extraEnv map[string]string) *runningService {
	t.Helper()
	t.Setenv(envKafkaBrokers, strings.Join(h.brokers, ","))
	t.Setenv(envKafkaTopic, h.topic)
	t.Setenv(envKafkaConsumerGroup, h.group)
	t.Setenv(envListenAddress, "127.0.0.1:0")
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
	return &runningService{svc: svc, errc: errc, cancel: cancel}
}

// waitReady blocks until the consumer reports ready (partitions assigned).
func (rs *runningService) waitReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, "consumer ready (partitions assigned)", func() bool {
		return rs.svc.ready.Load()
	})
}

// stop cancels the service context and waits for Start to return, which includes
// the runtime's clean consumer-group close (committing final offsets). It
// returns Start's error.
func (rs *runningService) stop(t *testing.T) error {
	t.Helper()
	rs.cancel()
	select {
	case err := <-rs.errc:
		return err
	case <-time.After(45 * time.Second):
		t.Fatal("service did not stop within 45s of context cancel")
		return nil
	}
}

// funcServer is a scripted HTTP CloudEvents receiver standing in for the user
// function. Each delivery arrives as a binary-mode CloudEvent whose body is the
// raw record value, so scenarios key their response script on the produced
// string. Every request is recorded (arrival time, 1-based attempt number for
// that body, and the status returned) for assertions on attempt counts, ordering
// and backoff timing.
type funcServer struct {
	srv     *httptest.Server
	respond func(body string, attempt int) (status int, delay time.Duration)

	mu     sync.Mutex
	counts map[string]int
	log    []reqRecord
}

type reqRecord struct {
	body    string
	attempt int
	at      time.Time
	status  int
	header  http.Header
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

		fs.mu.Lock()
		fs.counts[body]++
		n := fs.counts[body]
		fs.mu.Unlock()

		status, delay := fs.respond(body, n)

		fs.mu.Lock()
		fs.log = append(fs.log, reqRecord{body: body, attempt: n, at: at, status: status, header: hdr})
		fs.mu.Unlock()

		if delay > 0 {
			time.Sleep(delay)
		}
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

// successes returns how many deliveries of body were answered with a 2xx.
func (fs *funcServer) successes(body string) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n := 0
	for _, r := range fs.log {
		if r.body == body && r.status >= 200 && r.status < 300 {
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

// settle pauses briefly after readiness before producing (see settleAfterReady).
func (h *harness) settle() { time.Sleep(settleAfterReady) }

// assertBinaryCloudEvent verifies a delivery carried the binary-mode CloudEvent
// the runtime maps a plain (non-CloudEvent) Kafka record to -- see
// kafkaMessageToEvent. The context attributes travel as Ce-* HTTP headers and the
// record value is the body with an application/octet-stream content type. This
// proves scenario 1's CloudEvent mapping end to end: a regression in
// kafkaMessageToEvent or the binary encoding would otherwise pass a body-only
// check. The event ID embeds the partition/offset, which vary, so it is matched by
// shape rather than exact value.
func assertBinaryCloudEvent(t *testing.T, h *harness, rec reqRecord, wantBody string) {
	t.Helper()
	if rec.body != wantBody {
		t.Errorf("delivered body = %q, want %q", rec.body, wantBody)
	}
	checks := map[string]string{
		"Ce-Specversion": "1.0",
		"Ce-Type":        "dev.knative.kafka.event",
		"Ce-Source":      "kafka://" + strings.Join(h.brokers, ",") + "/" + h.topic,
		"Ce-Kafkatopic":  h.topic,
		"Content-Type":   "application/octet-stream",
	}
	for header, want := range checks {
		if got := rec.header.Get(header); got != want {
			t.Errorf("delivered %s = %q, want %q", header, got, want)
		}
	}
	if got := rec.header.Get("Ce-Id"); !strings.HasPrefix(got, "partition:0/offset:") {
		t.Errorf("delivered Ce-Id = %q, want it to encode partition 0 and an offset (partition:0/offset:<n>)", got)
	}
}
