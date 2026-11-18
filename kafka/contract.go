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

// This file is the adapter half of the func<->func-adapter-kafka environment
// variable contract. knative/func emits a fixed set of env var names onto the
// Kafka runtime sidecar; this adapter reads exactly those names. The production
// read sites look them up through the env* constants below (consumer.go,
// security.go, delivery.go, http_adapter.go, service.go, and cmd/fkafka-runtime,
// which reads the exported EnvFunctionTarget). A rename or drop on either side
// turns the corresponding setting into a runtime no-op. How visibly that fails
// depends on the name: a required connection/security name (brokers, topic, group,
// or a TLS/SASL setting the broker actually needs) makes the adapter fail to start
// or fail its handshake, while the optional tuning names (FUNCTION_DELIVERY_*,
// FUNCTION_REQUEST_TIMEOUT) and any security setting the broker does not need fall
// back to defaults silently.
//
// This is an adapter-side tripwire, not a cross-repo check. contractEnvVars is a
// hand-maintained list of the contract names, built from the env* constants.
// TestContractEnvVars pins the VALUES of those constants against a literal copy of
// func's names; it does NOT run the read sites, and it does NOT fetch or compare
// against knative/func. Most of these names are also pinned individually by the
// per-value tests in this package, so a changed constant already fails some unit
// test; what this list adds is one place that enumerates the whole contract and a
// failure message that points at func. The two lists are kept in lockstep by
// humans, mirroring kafkaRuntimeContractEnvNames /
// TestKafkaSidecarContainer_EnvNameContract in knative/func
// (pkg/k8s/deployer_test.go); change them together.
//
// Note: the error and log messages in this package repeat these names as plain
// string literals for readability, not generated from the constants.
const (
	// EnvFunctionTarget is the function URL the adapter POSTs decoded records to.
	// func sets it to a fixed value; read in cmd/fkafka-runtime.
	EnvFunctionTarget = "FUNCTION_TARGET"

	// Connection + security. Consumed in consumer.go and security.go.
	envKafkaBrokers          = "KAFKA_BROKERS"
	envKafkaTopic            = "KAFKA_TOPIC"
	envKafkaConsumerGroup    = "KAFKA_CONSUMER_GROUP"
	envKafkaSecurityProtocol = "KAFKA_SECURITY_PROTOCOL"
	envKafkaTLSCACert        = "KAFKA_TLS_CA_CERT"
	envKafkaTLSClientCert    = "KAFKA_TLS_CLIENT_CERT"
	envKafkaTLSClientKey     = "KAFKA_TLS_CLIENT_KEY"
	envKafkaTLSSkipVerify    = "KAFKA_TLS_SKIP_VERIFY"
	envKafkaSASLMechanism    = "KAFKA_SASL_MECHANISM"
	envKafkaSASLUser         = "KAFKA_SASL_USER"
	envKafkaSASLPassword     = "KAFKA_SASL_PASSWORD"

	// Delivery tuning. Consumed in delivery.go and http_adapter.go.
	envFunctionDeliveryRetry           = "FUNCTION_DELIVERY_RETRY"
	envFunctionDeliveryBackoffPolicy   = "FUNCTION_DELIVERY_BACKOFF_POLICY"
	envFunctionDeliveryBackoffDelay    = "FUNCTION_DELIVERY_BACKOFF_DELAY"
	envFunctionDeliveryBackoffMaxDelay = "FUNCTION_DELIVERY_BACKOFF_MAX_DELAY"
	envFunctionRequestTimeout          = "FUNCTION_REQUEST_TIMEOUT"
)

// Adapter-only env vars: read by the adapter but NOT part of the func contract,
// because func does not emit them. Kept out of contractEnvVars on purpose.
const (
	envFunctionMaxConcurrency = "FUNCTION_MAX_CONCURRENCY"
	EnvListenAddress          = "LISTEN_ADDRESS"
)

// contractEnvVars is the complete set of env var names this adapter reads that
// are part of the func<->adapter contract -- i.e. exactly the names func emits
// onto the sidecar. It is a hand-maintained mirror, built from the env*
// constants, of kafkaRuntimeContractEnvNames in knative/func. The adapter-only
// variables above (FUNCTION_MAX_CONCURRENCY, LISTEN_ADDRESS) are deliberately
// excluded because func does not emit them.
var contractEnvVars = []string{
	EnvFunctionTarget,
	envKafkaBrokers,
	envKafkaTopic,
	envKafkaConsumerGroup,
	envKafkaSecurityProtocol,
	envKafkaTLSCACert,
	envKafkaTLSClientCert,
	envKafkaTLSClientKey,
	envKafkaTLSSkipVerify,
	envKafkaSASLMechanism,
	envKafkaSASLUser,
	envKafkaSASLPassword,
	envFunctionDeliveryRetry,
	envFunctionDeliveryBackoffPolicy,
	envFunctionDeliveryBackoffDelay,
	envFunctionDeliveryBackoffMaxDelay,
	envFunctionRequestTimeout,
}
