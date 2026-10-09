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
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cloudevents/sdk-go/v2/event"
)

func TestKafkaMessageToEvent(t *testing.T) {
	ts := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	msg := Message{
		Key:       []byte("my-key"),
		Value:     []byte(`{"hello":"world"}`),
		Topic:     "my-topic",
		Partition: 2,
		Offset:    42,
		Timestamp: ts,
	}

	e := kafkaMessageToEvent(msg, "broker1:9092,broker2:9092")

	if e.SpecVersion() != "1.0" {
		t.Errorf("specversion = %q, want 1.0", e.SpecVersion())
	}
	if e.ID() != "partition:2/offset:42" {
		t.Errorf("id = %q, want partition:2/offset:42", e.ID())
	}
	if e.Source() != "kafka://broker1:9092,broker2:9092/my-topic" {
		t.Errorf("source = %q", e.Source())
	}
	if e.Type() != "dev.knative.kafka.event" {
		t.Errorf("type = %q", e.Type())
	}
	if e.Subject() != "my-key" {
		t.Errorf("subject = %q, want my-key", e.Subject())
	}
	if !e.Time().Equal(ts) {
		t.Errorf("time = %v, want %v", e.Time(), ts)
	}
	if string(e.Data()) != `{"hello":"world"}` {
		t.Errorf("data = %q", string(e.Data()))
	}

	exts := e.Extensions()
	if exts["kafkatopic"] != "my-topic" {
		t.Errorf("kafkatopic = %v", exts["kafkatopic"])
	}
	if fmt.Sprintf("%v", exts["kafkapartition"]) != "2" {
		t.Errorf("kafkapartition = %v", exts["kafkapartition"])
	}
	if fmt.Sprintf("%v", exts["kafkaoffset"]) != "42" {
		t.Errorf("kafkaoffset = %v", exts["kafkaoffset"])
	}
	if want := base64.StdEncoding.EncodeToString([]byte("my-key")); exts["kafkakey"] != want {
		t.Errorf("kafkakey = %v, want %v (base64 of my-key)", exts["kafkakey"], want)
	}
}

// TestKafkaMessageToEvent_BinaryKey guards the #2 fix: a key with bytes that are
// not header-safe (here a NUL and invalid UTF-8) must not wedge the partition.
// kafkakey carries the full key base64-encoded, subject is omitted rather than
// set to the raw bytes, and the event must validate (encode client-side).
func TestKafkaMessageToEvent_BinaryKey(t *testing.T) {
	key := []byte{0x00, 0x0a, 0xff, 0xfe, 'k'} // NUL, LF, invalid UTF-8
	msg := Message{
		Key:       key,
		Value:     []byte("data"),
		Topic:     "t",
		Partition: 1,
		Offset:    7,
		Timestamp: time.Now(),
	}

	e := kafkaMessageToEvent(msg, "broker:9092")

	if err := e.Validate(); err != nil {
		t.Fatalf("event with binary key failed Validate(): %v", err)
	}
	if e.Subject() != "" {
		t.Errorf("subject = %q, want empty for a non-header-safe key", e.Subject())
	}
	want := base64.StdEncoding.EncodeToString(key)
	if got := fmt.Sprintf("%v", e.Extensions()["kafkakey"]); got != want {
		t.Errorf("kafkakey = %v, want %v (base64)", got, want)
	}
}

// TestKafkaMessageToEvent_LargeOffset guards against the int32 CloudEvents
// Integer ceiling: a Kafka offset past math.MaxInt32 must survive as a decimal
// string, and the resulting event must validate (i.e. encode client-side).
// Storing the raw int64 would fail Validate() with "cannot convert ... to
// int32: out of range" and wedge the partition under at-least-once redelivery.
func TestKafkaMessageToEvent_LargeOffset(t *testing.T) {
	const bigOffset int64 = 1<<31 + 5 // 2147483653, just past math.MaxInt32
	msg := Message{
		Value:     []byte("data"),
		Topic:     "t",
		Partition: 0,
		Offset:    bigOffset,
		Timestamp: time.Now(),
	}

	e := kafkaMessageToEvent(msg, "broker:9092")

	if err := e.Validate(); err != nil {
		t.Fatalf("event with large offset failed Validate(): %v", err)
	}
	want := strconv.FormatInt(bigOffset, 10)
	if got := fmt.Sprintf("%v", e.Extensions()["kafkaoffset"]); got != want {
		t.Errorf("kafkaoffset = %v, want %v", got, want)
	}
}

func TestKafkaMessageToEvent_NoKey(t *testing.T) {
	msg := Message{
		Value:     []byte("data"),
		Topic:     "t",
		Partition: 0,
		Offset:    0,
		Timestamp: time.Now(),
	}

	e := kafkaMessageToEvent(msg, "b:9092")

	if e.Subject() != "" {
		t.Errorf("subject should be empty when key is nil, got %q", e.Subject())
	}
	if _, ok := e.Extensions()["kafkakey"]; ok {
		t.Error("kafkakey extension should not be set when key is nil")
	}
}

func TestKafkaMessageToEvent_CEPassThrough(t *testing.T) {
	msg := Message{
		Value: []byte(`{"temperature":22}`),
		Topic: "events",
		Headers: []Header{
			{Key: "ce_specversion", Value: []byte("1.0")},
			{Key: "ce_id", Value: []byte("abc-123")},
			{Key: "ce_source", Value: []byte("//my-sensor")},
			{Key: "ce_type", Value: []byte("sensor.reading")},
			{Key: "ce_subject", Value: []byte("temp")},
			{Key: "ce_time", Value: []byte("2025-06-15T12:00:00Z")},
			{Key: "ce_datacontenttype", Value: []byte("application/json")},
			{Key: "ce_dataschema", Value: []byte("https://example.com/schema/sensor.json")},
			{Key: "ce_customext", Value: []byte("custom-value")},
		},
		Partition: 1,
		Offset:    99,
	}

	e := kafkaMessageToEvent(msg, "b:9092")

	if e.SpecVersion() != "1.0" {
		t.Errorf("specversion = %q", e.SpecVersion())
	}
	if e.ID() != "abc-123" {
		t.Errorf("id = %q, want abc-123", e.ID())
	}
	if e.Source() != "//my-sensor" {
		t.Errorf("source = %q", e.Source())
	}
	if e.Type() != "sensor.reading" {
		t.Errorf("type = %q", e.Type())
	}
	if e.Subject() != "temp" {
		t.Errorf("subject = %q", e.Subject())
	}
	expectedTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	if !e.Time().Equal(expectedTime) {
		t.Errorf("time = %v, want %v", e.Time(), expectedTime)
	}
	if e.DataContentType() != "application/json" {
		t.Errorf("datacontenttype = %q", e.DataContentType())
	}
	if string(e.Data()) != `{"temperature":22}` {
		t.Errorf("data = %q", string(e.Data()))
	}
	if e.DataSchema() != "https://example.com/schema/sensor.json" {
		t.Errorf("dataschema = %q, want https://example.com/schema/sensor.json", e.DataSchema())
	}
	if v, ok := e.Extensions()["customext"]; !ok || v != "custom-value" {
		t.Errorf("customext = %v", v)
	}
}

func TestKafkaMessageToEvent_CEPassThrough_DefaultContentType(t *testing.T) {
	msg := Message{
		Value: []byte(`{}`),
		Headers: []Header{
			{Key: "ce_specversion", Value: []byte("1.0")},
			{Key: "ce_id", Value: []byte("x")},
			{Key: "ce_source", Value: []byte("s")},
			{Key: "ce_type", Value: []byte("t")},
		},
	}

	e := kafkaMessageToEvent(msg, "b:9092")

	// No content-type/ce_datacontenttype header was supplied, so the adapter must
	// not invent one: datacontenttype stays absent while the data is preserved.
	if e.DataContentType() != "" {
		t.Errorf("content type = %q, want empty (no header supplied)", e.DataContentType())
	}
	if string(e.Data()) != `{}` {
		t.Errorf("data = %q, want {}", string(e.Data()))
	}
}

func TestKafkaMessageToEvent_CEPassThrough_ContentTypeHeader(t *testing.T) {
	msg := Message{
		Value: []byte(`<xml/>`),
		Headers: []Header{
			{Key: "ce_specversion", Value: []byte("1.0")},
			{Key: "ce_id", Value: []byte("x")},
			{Key: "ce_source", Value: []byte("s")},
			{Key: "ce_type", Value: []byte("t")},
			{Key: "content-type", Value: []byte("application/xml")},
		},
	}

	e := kafkaMessageToEvent(msg, "b:9092")

	if e.DataContentType() != "application/xml" {
		t.Errorf("content type = %q, want application/xml", e.DataContentType())
	}
}

func TestKafkaMessageToEvent_CEPassThrough_CEDataContentTypeOverridesContentType(t *testing.T) {
	msg := Message{
		Value: []byte(`{}`),
		Headers: []Header{
			{Key: "ce_specversion", Value: []byte("1.0")},
			{Key: "ce_id", Value: []byte("x")},
			{Key: "ce_source", Value: []byte("s")},
			{Key: "ce_type", Value: []byte("t")},
			{Key: "content-type", Value: []byte("application/xml")},
			{Key: "ce_datacontenttype", Value: []byte("text/plain")},
		},
	}

	e := kafkaMessageToEvent(msg, "b:9092")

	if e.DataContentType() != "text/plain" {
		t.Errorf("content type = %q, want text/plain (ce_datacontenttype should override content-type)", e.DataContentType())
	}
}

func TestKafkaMessageToEvent_NotCE(t *testing.T) {
	msg := Message{
		Value: []byte("plain data"),
		Headers: []Header{
			{Key: "x-custom", Value: []byte("val")},
		},
		Topic:     "t",
		Partition: 0,
		Offset:    0,
		Timestamp: time.Now(),
	}

	e := kafkaMessageToEvent(msg, "b:9092")

	if e.Type() != "dev.knative.kafka.event" {
		t.Errorf("non-CE message should get type dev.knative.kafka.event, got %q", e.Type())
	}
}

func TestKafkaMessageToEvent_CEPassThrough_MissingAttributes(t *testing.T) {
	// A record carrying ce_specversion but missing a mandatory attribute
	// (here ce_source and ce_type) is a partial/malformed CloudEvent. Passing it
	// through would produce an event that fails outbound validation on every
	// encode and wedge the partition, so it must fall through to the wrap path
	// and come out as a fresh, valid event.
	msg := Message{
		Value: []byte(`{"data":"value"}`),
		Topic: "events",
		Headers: []Header{
			{Key: "ce_specversion", Value: []byte("1.0")},
			{Key: "ce_id", Value: []byte("abc")},
		},
		Partition: 0,
		Offset:    10,
	}

	e := kafkaMessageToEvent(msg, "b:9092")

	// Must be the generated wrapper, not a pass-through: the wrapper sets type,
	// a partition/offset id, and a kafka:// source, and the resulting event must
	// validate.
	if err := e.Validate(); err != nil {
		t.Fatalf("wrapped event failed Validate(): %v", err)
	}
	if e.Type() != "dev.knative.kafka.event" {
		t.Errorf("type = %q, want dev.knative.kafka.event", e.Type())
	}
	if e.ID() != "partition:0/offset:10" {
		t.Errorf("id = %q, want partition:0/offset:10", e.ID())
	}
	if e.Source() != "kafka://b:9092/events" {
		t.Errorf("source = %q, want kafka://b:9092/events", e.Source())
	}
	if string(e.Data()) != `{"data":"value"}` {
		t.Errorf("data = %q", string(e.Data()))
	}
}

// TestKafkaMessageToEvent_CEPassThrough_Complete verifies that a record carrying
// all four mandatory ce_ attributes is passed through verbatim rather than
// wrapped.
func TestKafkaMessageToEvent_CEPassThrough_Complete(t *testing.T) {
	msg := Message{
		Value: []byte(`{"data":"value"}`),
		Topic: "events",
		Headers: []Header{
			{Key: "ce_specversion", Value: []byte("1.0")},
			{Key: "ce_id", Value: []byte("evt-1")},
			{Key: "ce_source", Value: []byte("/my/source")},
			{Key: "ce_type", Value: []byte("com.example.thing")},
		},
		Partition: 0,
		Offset:    10,
	}

	e := kafkaMessageToEvent(msg, "b:9092")

	if e.ID() != "evt-1" {
		t.Errorf("id = %q, want evt-1 (pass-through)", e.ID())
	}
	if e.Source() != "/my/source" {
		t.Errorf("source = %q, want /my/source (pass-through)", e.Source())
	}
	if e.Type() != "com.example.thing" {
		t.Errorf("type = %q, want com.example.thing (pass-through)", e.Type())
	}
}

// TestKafkaMessageToEvent_CEPassThrough_MalformedWrapped verifies that a record
// which has all four mandatory ce_ headers present but malformed (empty ce_id,
// unsupported ce_specversion) is NOT passed through: it would fail outbound
// validation on every encode and wedge the partition. Such records must fall
// through to the wrap path and emerge as fresh, valid events.
func TestKafkaMessageToEvent_CEPassThrough_MalformedWrapped(t *testing.T) {
	cases := map[string][]Header{
		"empty ce_id": {
			{Key: "ce_specversion", Value: []byte("1.0")},
			{Key: "ce_id", Value: []byte("")},
			{Key: "ce_source", Value: []byte("/s")},
			{Key: "ce_type", Value: []byte("t")},
		},
		"unsupported ce_specversion": {
			{Key: "ce_specversion", Value: []byte("0.1")},
			{Key: "ce_id", Value: []byte("x")},
			{Key: "ce_source", Value: []byte("/s")},
			{Key: "ce_type", Value: []byte("t")},
		},
	}

	for name, headers := range cases {
		t.Run(name, func(t *testing.T) {
			msg := Message{
				Value:     []byte("data"),
				Topic:     "events",
				Headers:   headers,
				Partition: 3,
				Offset:    9,
			}

			e := kafkaMessageToEvent(msg, "b:9092")

			if err := e.Validate(); err != nil {
				t.Fatalf("wrapped event failed Validate(): %v", err)
			}
			// The wrap path is identifiable by its generated id/type.
			if e.ID() != "partition:3/offset:9" {
				t.Errorf("id = %q, want partition:3/offset:9 (wrapped, not passed through)", e.ID())
			}
			if e.Type() != "dev.knative.kafka.event" {
				t.Errorf("type = %q, want dev.knative.kafka.event (wrapped)", e.Type())
			}
			if e.SpecVersion() != "1.0" {
				t.Errorf("specversion = %q, want 1.0 (wrapped)", e.SpecVersion())
			}
		})
	}
}

// stubAdapter is a no-op KafkaAdapter used in consume tests.
// consume fails on env-var validation before it ever calls the adapter.
type stubAdapter struct{}

func (stubAdapter) Invoke(context.Context, event.Event) (*event.Event, error) {
	return nil, nil
}

// flakyAdapter fails its first failuresLeft invocations, then succeeds. If
// alwaysFail is set it never succeeds.
type flakyAdapter struct {
	failuresLeft int
	alwaysFail   bool
	calls        int
}

func (f *flakyAdapter) Invoke(context.Context, event.Event) (*event.Event, error) {
	f.calls++
	if f.alwaysFail || f.failuresLeft > 0 {
		f.failuresLeft--
		return nil, errors.New("transient delivery failure")
	}
	return nil, nil
}

// deliver retries a transient failure and only marks the offset once delivery
// succeeds, so a record is never skipped after a blip like a stale keep-alive.
func TestDeliver_RetriesUntilSuccess(t *testing.T) {
	fake := &flakyAdapter{failuresLeft: 2}
	h := &consumerGroupHandler{adapter: fake}
	msg := &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 1}

	if ok := h.deliver(context.Background(), msg, event.New()); !ok {
		t.Fatal("deliver returned false; want true after retries eventually succeed")
	}
	if fake.calls != 3 {
		t.Errorf("Invoke called %d times, want 3 (2 failures + 1 success)", fake.calls)
	}
}

// When the session context is cancelled while delivery keeps failing, deliver
// stops and reports false so the caller does NOT mark the offset (the record is
// redelivered from the last committed offset in the next session).
func TestDeliver_StopsOnContextCancel(t *testing.T) {
	fake := &flakyAdapter{alwaysFail: true}
	h := &consumerGroupHandler{adapter: fake}
	msg := &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 1}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan bool, 1)
	go func() { done <- h.deliver(ctx, msg, event.New()) }()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("deliver returned true on a cancelled context; want false")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deliver did not return after context cancellation")
	}
}

func TestConsume_MissingEnvVars(t *testing.T) {
	tests := []struct {
		name    string
		envVars map[string]string
		wantErr string
	}{
		{
			name:    "no KAFKA_BROKERS",
			envVars: map[string]string{},
			wantErr: "KAFKA_BROKERS",
		},
		{
			name: "no KAFKA_TOPIC",
			envVars: map[string]string{
				"KAFKA_BROKERS": "localhost:9092",
			},
			wantErr: "KAFKA_TOPIC",
		},
		{
			name: "no KAFKA_CONSUMER_GROUP",
			envVars: map[string]string{
				"KAFKA_BROKERS": "localhost:9092",
				"KAFKA_TOPIC":   "my-topic",
			},
			wantErr: "KAFKA_CONSUMER_GROUP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all three env vars, then set only what the subtest provides.
			t.Setenv("KAFKA_BROKERS", "")
			t.Setenv("KAFKA_TOPIC", "")
			t.Setenv("KAFKA_CONSUMER_GROUP", "")
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			var ready atomic.Bool
			err := consume(context.Background(), stubAdapter{}, &ready)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}
