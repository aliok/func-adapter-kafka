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
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/rs/zerolog/log"
)

func kafkaBrokers() []string {
	v := os.Getenv("KAFKA_BROKERS")
	if v == "" {
		return nil
	}
	return splitAndTrim(v)
}

func kafkaTopic() string {
	return strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
}

func kafkaConsumerGroup() string {
	return strings.TrimSpace(os.Getenv("KAFKA_CONSUMER_GROUP"))
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// consume runs the Kafka consumer, delivering each record (converted to a
// CloudEvent) through the given KafkaAdapter. The offset is committed only when
// the adapter returns a nil error.
func consume(ctx context.Context, adapter KafkaAdapter, ready *atomic.Bool) error {
	defer ready.Store(false)

	brokers := kafkaBrokers()
	topic := kafkaTopic()
	group := kafkaConsumerGroup()

	if len(brokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS environment variable is required")
	}
	if topic == "" {
		return fmt.Errorf("KAFKA_TOPIC environment variable is required")
	}
	if group == "" {
		return fmt.Errorf("KAFKA_CONSUMER_GROUP environment variable is required")
	}

	log.Info().
		Strs("brokers", brokers).
		Str("topic", topic).
		Str("group", group).
		Msg("connecting to kafka")

	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	if err := configureSecurity(config); err != nil {
		return fmt.Errorf("configuring kafka security: %w", err)
	}

	client, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		return fmt.Errorf("creating consumer group: %w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Error().Err(err).Msg("error closing kafka consumer group")
		}
	}()

	handler := &consumerGroupHandler{
		adapter: adapter,
		ready:   ready,
		brokers: strings.Join(brokers, ","),
	}

	for {
		if err := client.Consume(ctx, []string{topic}, handler); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("consumer error: %w", err)
		}
		if ctx.Err() != nil {
			return nil
		}
		ready.Store(false)
	}
}

type consumerGroupHandler struct {
	adapter KafkaAdapter
	ready   *atomic.Bool
	brokers string
}

func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	h.ready.Store(true)
	log.Info().Msg("kafka consumer ready (partitions assigned)")
	return nil
}

func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	h.ready.Store(false)
	log.Info().Msg("kafka consumer partitions revoked")
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			m := Message{
				Key:       msg.Key,
				Value:     msg.Value,
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Timestamp: msg.Timestamp,
			}
			for _, rh := range msg.Headers {
				if rh != nil {
					m.Headers = append(m.Headers, Header{
						Key:   string(rh.Key),
						Value: rh.Value,
					})
				}
			}

			e := kafkaMessageToEvent(m, h.brokers)

			// Deliver with retry. The offset is only marked once delivery
			// succeeds, so a transient failure (a stale keep-alive connection,
			// a restarting function, a timeout) is redelivered rather than
			// skipped — preserving at-least-once. If delivery keeps failing we
			// keep retrying the same record with backoff, which blocks this
			// partition until the function recovers; we never advance past an
			// undelivered record. Redelivery of a genuine poison-pill record is
			// a known gap (a dead-letter path is future work).
			if !h.deliver(session.Context(), msg, e) {
				// Context cancelled (rebalance or shutdown) before delivery
				// succeeded: stop without marking so the record is redelivered
				// from the last committed offset in the next session.
				return nil
			}
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

// Delivery retry backoff bounds. Backoff grows exponentially from base to max;
// attempts are unbounded but interrupted as soon as the session context is
// cancelled (rebalance/shutdown).
const (
	deliverBaseBackoff = 100 * time.Millisecond
	deliverMaxBackoff  = 30 * time.Second
)

// deliver invokes the function for a single record, retrying transient failures
// with exponential backoff until it succeeds. It returns false only when ctx is
// cancelled (rebalance/shutdown) before a successful delivery.
func (h *consumerGroupHandler) deliver(ctx context.Context, msg *sarama.ConsumerMessage, e event.Event) bool {
	backoff := deliverBaseBackoff
	for attempt := 1; ; attempt++ {
		if _, err := h.adapter.Invoke(ctx, e); err == nil {
			return true
		} else {
			log.Warn().Err(err).
				Str("topic", msg.Topic).
				Int32("partition", msg.Partition).
				Int64("offset", msg.Offset).
				Int("attempt", attempt).
				Dur("retryIn", backoff).
				Msg("delivery to function failed; will retry")
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}

		if backoff < deliverMaxBackoff {
			backoff *= 2
			if backoff > deliverMaxBackoff {
				backoff = deliverMaxBackoff
			}
		}
	}
}

// kafkaMessageToEvent converts a Kafka message to a CloudEvent.
// If the message already contains CloudEvent headers (ce_ prefix per the
// CloudEvents Kafka Protocol Binding), it is parsed as an existing CloudEvent
// rather than re-wrapped.
func kafkaMessageToEvent(msg Message, brokers string) event.Event {
	if e, ok := parseCEFromHeaders(msg); ok {
		return e
	}

	e := event.New()
	e.SetSpecVersion("1.0")
	e.SetID(fmt.Sprintf("partition:%d/offset:%d", msg.Partition, msg.Offset))
	e.SetSource(fmt.Sprintf("kafka://%s/%s", brokers, msg.Topic))
	e.SetType("dev.knative.kafka.event")
	e.SetTime(msg.Timestamp)
	if len(msg.Key) > 0 {
		e.SetSubject(string(msg.Key))
	}
	_ = e.SetData("application/octet-stream", msg.Value)
	e.SetExtension("kafkatopic", msg.Topic)
	e.SetExtension("kafkapartition", msg.Partition)
	e.SetExtension("kafkaoffset", msg.Offset)
	if len(msg.Key) > 0 {
		e.SetExtension("kafkakey", string(msg.Key))
	}
	return e
}

// parseCEFromHeaders checks if the Kafka message is already a CloudEvent
// (binary content mode with ce_ prefixed headers) and parses it.
func parseCEFromHeaders(msg Message) (event.Event, bool) {
	headers := make(map[string]string)
	for _, h := range msg.Headers {
		headers[strings.ToLower(h.Key)] = string(h.Value)
	}

	if _, ok := headers["ce_specversion"]; !ok {
		return event.Event{}, false
	}

	e := event.New()
	e.SetSpecVersion(headers["ce_specversion"])
	if v, ok := headers["ce_id"]; ok {
		e.SetID(v)
	}
	if v, ok := headers["ce_source"]; ok {
		e.SetSource(v)
	}
	if v, ok := headers["ce_type"]; ok {
		e.SetType(v)
	}
	if v, ok := headers["ce_subject"]; ok {
		e.SetSubject(v)
	}
	if v, ok := headers["ce_time"]; ok {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err == nil {
			e.SetTime(t)
		}
	}
	if v, ok := headers["ce_dataschema"]; ok {
		e.SetDataSchema(v)
	}

	contentType := "application/json"
	if v, ok := headers["content-type"]; ok {
		contentType = v
	}
	if v, ok := headers["ce_datacontenttype"]; ok {
		contentType = v
	}
	_ = e.SetData(contentType, msg.Value)

	// Set non-standard ce_ headers as extensions
	for k, v := range headers {
		if strings.HasPrefix(k, "ce_") {
			attr := strings.TrimPrefix(k, "ce_")
			switch attr {
			case "specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema":
				continue
			default:
				e.SetExtension(attr, v)
			}
		}
	}

	return e, true
}
