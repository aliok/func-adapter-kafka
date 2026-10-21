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
// What was previously untested is the ConsumeClaim glue: that session.MarkMessage
// is called after a successful delivery and is NOT called when delivery never
// succeeds. sarama ships no ConsumerGroup mock, so we drive ConsumeClaim with
// hand-written session/claim doubles.

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

// capturingAdapter records every Invoke (count and the delivered event) in a
// race-safe way and returns a configurable error, so a test running ConsumeClaim
// in another goroutine can observe delivery attempts without a data race.
type capturingAdapter struct {
	err    error
	calls  atomic.Int64
	called chan struct{}

	mu   sync.Mutex
	last event.Event
}

func (a *capturingAdapter) Invoke(_ context.Context, e event.Event) (*event.Event, error) {
	a.calls.Add(1)
	a.mu.Lock()
	a.last = e
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

// TestConsumeClaim_MarksOffsetOnSuccess verifies the happy path: a record is
// converted to a CloudEvent, delivered, and only then is its offset marked
// (committed). This is the record -> CloudEvent -> ack -> commit path end to end
// within the sidecar.
func TestConsumeClaim_MarksOffsetOnSuccess(t *testing.T) {
	adapter := &capturingAdapter{called: make(chan struct{}, 1)}
	h := &consumerGroupHandler{adapter: adapter, brokers: "broker:9092"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &recordingSession{ctx: ctx, marked: make(chan *sarama.ConsumerMessage, 1)}
	claim := &channelClaim{topic: "orders", partition: 0, messages: make(chan *sarama.ConsumerMessage, 1)}

	msg := &sarama.ConsumerMessage{Topic: "orders", Partition: 0, Offset: 42, Value: []byte("hello")}
	claim.messages <- msg

	done := make(chan error, 1)
	go func() { done <- h.ConsumeClaim(session, claim) }()

	select {
	case marked := <-session.marked:
		if marked.Offset != 42 {
			t.Errorf("marked offset = %d, want 42", marked.Offset)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("offset was not marked after a successful (2xx) delivery")
	}

	if got := adapter.calls.Load(); got != 1 {
		t.Errorf("adapter Invoke called %d times, want 1", got)
	}
	// The record was delivered as the wrapped CloudEvent for this partition/offset.
	if id := adapter.lastEvent().ID(); id != "partition:0/offset:42" {
		t.Errorf("delivered event ID = %q, want %q", id, "partition:0/offset:42")
	}

	// Close the claim so ConsumeClaim returns (as on a rebalance).
	close(claim.messages)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeClaim did not return after the claim was closed")
	}
}

// TestConsumeClaim_DoesNotMarkOffsetOnFailure verifies the negative path: when
// delivery never succeeds (the function keeps returning a non-2xx / the adapter
// keeps erroring), the record is retried and its offset is NEVER marked, so it is
// redelivered rather than silently skipped. This preserves at-least-once.
func TestConsumeClaim_DoesNotMarkOffsetOnFailure(t *testing.T) {
	adapter := &capturingAdapter{err: errors.New("non-2xx from function"), called: make(chan struct{}, 8)}
	h := &consumerGroupHandler{adapter: adapter, brokers: "broker:9092"}

	ctx, cancel := context.WithCancel(context.Background())

	session := &recordingSession{ctx: ctx, marked: make(chan *sarama.ConsumerMessage, 1)}
	claim := &channelClaim{topic: "orders", partition: 0, messages: make(chan *sarama.ConsumerMessage, 1)}

	msg := &sarama.ConsumerMessage{Topic: "orders", Partition: 0, Offset: 42, Value: []byte("hello")}
	claim.messages <- msg

	done := make(chan error, 1)
	go func() { done <- h.ConsumeClaim(session, claim) }()

	// Wait until the record has been attempted at least twice, i.e. a failed
	// delivery is being retried (backoff starts at 100ms).
	for i := 0; i < 2; i++ {
		select {
		case <-adapter.called:
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatalf("delivery was not retried; adapter called only %d times", adapter.calls.Load())
		}
	}

	// Stop the retry loop (as a rebalance/shutdown would) and let ConsumeClaim return.
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeClaim did not return after context cancel")
	}

	// The offset must never have been marked for an undelivered record.
	select {
	case m := <-session.marked:
		t.Fatalf("offset %d was marked despite delivery never succeeding", m.Offset)
	default:
	}
}
