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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cloudevents/sdk-go/v2/event"
)

// These tests cover the full in-sidecar delivery path at the consumer-group
// level: a record is read from a claim, converted to a CloudEvent, delivered
// through the adapter, and the offset is marked only when delivery succeeds.
//
// The adapter/HTTP half (2xx commits, non-2xx/transport errors retry) is covered
// in http_adapter_test.go; deliver's retry/cancel return value in consumer_test.go.
// What was previously untested is the ConsumeClaim glue: that the record fields
// (key, timestamp, headers) are converted, that session.MarkMessage is called
// after a successful delivery, and that nothing is marked -- and no later record
// is attempted -- when delivery never succeeds. sarama ships no ConsumerGroup
// mock, so we drive ConsumeClaim with hand-written session/claim doubles.

// recordingSession is a sarama.ConsumerGroupSession double that records the
// messages passed to MarkMessage and exposes a cancellable context.
type recordingSession struct {
	ctx    context.Context
	marked chan *sarama.ConsumerMessage
}

func (s *recordingSession) Claims() map[string][]int32               { return nil }
func (s *recordingSession) MemberID() string                         { return "" }
func (s *recordingSession) GenerationID() int32                      { return 0 }
func (s *recordingSession) MarkOffset(string, int32, int64, string)  {}
func (s *recordingSession) Commit()                                  {}
func (s *recordingSession) ResetOffset(string, int32, int64, string) {}
func (s *recordingSession) Context() context.Context                 { return s.ctx }
func (s *recordingSession) MarkMessage(msg *sarama.ConsumerMessage, _ string) {
	s.marked <- msg
}

// channelClaim is a sarama.ConsumerGroupClaim double backed by a channel.
type channelClaim struct {
	topic     string
	partition int32
	messages  chan *sarama.ConsumerMessage
}

func (c *channelClaim) Topic() string                            { return c.topic }
func (c *channelClaim) Partition() int32                         { return c.partition }
func (c *channelClaim) InitialOffset() int64                     { return 0 }
func (c *channelClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *channelClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

// capturingAdapter records every Invoke (count, the delivered event, and the ID
// of each attempt in order) in a race-safe way and returns a configurable error,
// so a test running ConsumeClaim in another goroutine can observe delivery
// attempts without a data race.
type capturingAdapter struct {
	err    error
	calls  atomic.Int64
	called chan struct{}

	mu   sync.Mutex
	last event.Event
	ids  []string
}

func (a *capturingAdapter) Invoke(_ context.Context, e event.Event) (*event.Event, error) {
	a.calls.Add(1)
	a.mu.Lock()
	a.last = e
	a.ids = append(a.ids, e.ID())
	a.mu.Unlock()
	// Non-blocking signal: never stall delivery if no one is reading (or the
	// buffer is full); callers only need to observe that attempts are happening.
	select {
	case a.called <- struct{}{}:
	default:
	}
	return nil, a.err
}

func (a *capturingAdapter) lastEvent() event.Event {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.last
}

// attemptIDs returns the event ID of every Invoke attempt, in order.
func (a *capturingAdapter) attemptIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.ids...)
}

// TestConsumeClaim_MarksOffsetOnSuccess verifies the happy path: each record is
// converted to a CloudEvent, delivered, and only then is its offset marked
// (committed). Two records are used so this also pins that ConsumeClaim keeps
// consuming -- a mutant that returns after the first record would mark only 42.
func TestConsumeClaim_MarksOffsetOnSuccess(t *testing.T) {
	adapter := &capturingAdapter{called: make(chan struct{}, 2)}
	h := &consumerGroupHandler{adapter: adapter, brokers: "broker:9092"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &recordingSession{ctx: ctx, marked: make(chan *sarama.ConsumerMessage, 2)}
	claim := &channelClaim{topic: "orders", partition: 0, messages: make(chan *sarama.ConsumerMessage, 2)}

	claim.messages <- &sarama.ConsumerMessage{Topic: "orders", Partition: 0, Offset: 42, Value: []byte("a")}
	claim.messages <- &sarama.ConsumerMessage{Topic: "orders", Partition: 0, Offset: 43, Value: []byte("b")}
	// Closing lets ConsumeClaim return once both buffered records are drained.
	close(claim.messages)

	done := make(chan error, 1)
	go func() { done <- h.ConsumeClaim(session, claim) }()

	// Both offsets must be marked, in order, each only after its delivery succeeded.
	for _, want := range []int64{42, 43} {
		select {
		case marked := <-session.marked:
			if marked.Offset != want {
				t.Errorf("marked offset = %d, want %d", marked.Offset, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("offset %d was not marked after a successful (2xx) delivery", want)
		}
	}

	if got := adapter.calls.Load(); got != 2 {
		t.Errorf("adapter Invoke called %d times, want 2", got)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeClaim did not return after the claim was closed")
	}
}

// TestConsumeClaim_ConvertsRecordFields verifies the glue copies a plain record's
// key and timestamp into the wrapped CloudEvent: a header-safe key becomes the
// subject, the record timestamp becomes the event time, and the ID encodes the
// partition/offset. Without this, a mutant dropping the key or timestamp copy
// would survive (the other tests use records that carry neither).
func TestConsumeClaim_ConvertsRecordFields(t *testing.T) {
	adapter := &capturingAdapter{called: make(chan struct{}, 1)}
	h := &consumerGroupHandler{adapter: adapter, brokers: "broker:9092"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &recordingSession{ctx: ctx, marked: make(chan *sarama.ConsumerMessage, 1)}
	claim := &channelClaim{topic: "orders", partition: 0, messages: make(chan *sarama.ConsumerMessage, 1)}

	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	claim.messages <- &sarama.ConsumerMessage{
		Topic: "orders", Partition: 0, Offset: 7,
		Key:       []byte("order-key"),
		Value:     []byte("hello"),
		Timestamp: ts,
	}

	done := make(chan error, 1)
	go func() { done <- h.ConsumeClaim(session, claim) }()

	select {
	case <-session.marked:
	case <-time.After(5 * time.Second):
		t.Fatal("record was not delivered/marked")
	}

	e := adapter.lastEvent()
	if e.Subject() != "order-key" {
		t.Errorf("event subject = %q, want %q (from the record key)", e.Subject(), "order-key")
	}
	if !e.Time().Equal(ts) {
		t.Errorf("event time = %v, want %v (from the record timestamp)", e.Time(), ts)
	}
	if e.ID() != "partition:0/offset:7" {
		t.Errorf("event ID = %q, want %q", e.ID(), "partition:0/offset:7")
	}

	close(claim.messages)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeClaim did not return after the claim was closed")
	}
}

// TestConsumeClaim_PassesThroughCloudEventHeaders verifies the glue copies the
// record's headers through, so a record already in CloudEvents binary content
// mode (ce_* headers) is passed through as its own CloudEvent rather than
// re-wrapped. A nil header entry also exercises the nil-skip in the copy loop.
// A broken header copy would silently turn pass-through into re-wrapping with no
// other test failing.
func TestConsumeClaim_PassesThroughCloudEventHeaders(t *testing.T) {
	adapter := &capturingAdapter{called: make(chan struct{}, 1)}
	h := &consumerGroupHandler{adapter: adapter, brokers: "broker:9092"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &recordingSession{ctx: ctx, marked: make(chan *sarama.ConsumerMessage, 1)}
	claim := &channelClaim{topic: "orders", partition: 0, messages: make(chan *sarama.ConsumerMessage, 1)}

	claim.messages <- &sarama.ConsumerMessage{
		Topic: "orders", Partition: 0, Offset: 9,
		Value: []byte("{}"),
		Headers: []*sarama.RecordHeader{
			{Key: []byte("ce_specversion"), Value: []byte("1.0")},
			{Key: []byte("ce_id"), Value: []byte("upstream-id")},
			{Key: []byte("ce_source"), Value: []byte("/upstream")},
			{Key: []byte("ce_type"), Value: []byte("com.example.event")},
			nil, // must be skipped by the copy loop
		},
	}

	done := make(chan error, 1)
	go func() { done <- h.ConsumeClaim(session, claim) }()

	select {
	case <-session.marked:
	case <-time.After(5 * time.Second):
		t.Fatal("record was not delivered/marked")
	}

	// A passed-through CloudEvent keeps its own ID; a re-wrapped one would be
	// "partition:0/offset:9". So this proves the headers reached the converter and
	// that the nil header did not break (or panic) the copy.
	if id := adapter.lastEvent().ID(); id != "upstream-id" {
		t.Errorf("delivered event ID = %q, want %q (headers not passed through?)", id, "upstream-id")
	}

	close(claim.messages)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeClaim did not return after the claim was closed")
	}
}

// TestConsumeClaim_DoesNotMarkOffsetOnFailure verifies the negative path: when a
// record's delivery never succeeds, ConsumeClaim keeps retrying that same record
// and never advances to a later one -- marking nothing -- so no record is skipped
// past the failed offset (at-least-once / head-of-line blocking). A second record
// (43) is queued behind the failing one (42) specifically so a mutant that gives
// up after N tries and advances would be caught: it would attempt 43 and/or mark.
func TestConsumeClaim_DoesNotMarkOffsetOnFailure(t *testing.T) {
	adapter := &capturingAdapter{err: errors.New("non-2xx from function"), called: make(chan struct{}, 16)}
	h := &consumerGroupHandler{adapter: adapter, brokers: "broker:9092"}

	ctx, cancel := context.WithCancel(context.Background())

	session := &recordingSession{ctx: ctx, marked: make(chan *sarama.ConsumerMessage, 1)}
	claim := &channelClaim{topic: "orders", partition: 0, messages: make(chan *sarama.ConsumerMessage, 2)}

	claim.messages <- &sarama.ConsumerMessage{Topic: "orders", Partition: 0, Offset: 42, Value: []byte("a")}
	claim.messages <- &sarama.ConsumerMessage{Topic: "orders", Partition: 0, Offset: 43, Value: []byte("b")}

	done := make(chan error, 1)
	go func() { done <- h.ConsumeClaim(session, claim) }()

	// Wait for 4 attempts: enough to outlast a bounded-retry mutant (e.g. give up
	// after 3) that would then advance past the failed record. Backoff starts at
	// 100ms, so 4 attempts land within ~1s.
	for i := 0; i < 4; i++ {
		select {
		case <-adapter.called:
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatalf("delivery was not retried enough; adapter called only %d times", adapter.calls.Load())
		}
	}

	// Stop the retry loop (as a rebalance/shutdown would) and let ConsumeClaim return.
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeClaim did not return after context cancel")
	}

	// Every attempt must have been for offset 42; 43 must never have been tried.
	for i, id := range adapter.attemptIDs() {
		if id != "partition:0/offset:42" {
			t.Errorf("attempt %d delivered event ID = %q, want partition:0/offset:42 (advanced past a failed offset?)", i, id)
		}
	}

	// And nothing may have been marked for an undelivered record.
	select {
	case m := <-session.marked:
		t.Fatalf("offset %d was marked despite delivery never succeeding", m.Offset)
	default:
	}
}
