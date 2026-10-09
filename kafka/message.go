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

import "time"

// Message represents a Kafka message delivered to the function's handler.
type Message struct {
	Key       []byte
	Value     []byte
	Headers   []Header
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
}

// Header is a key-value pair attached to a Kafka message.
type Header struct {
	Key   string
	Value []byte
}
