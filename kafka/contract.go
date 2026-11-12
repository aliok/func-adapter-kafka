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
// read sites look them up through the Env* constants below (consumer.go,
// security.go, delivery.go, http_adapter.go, service.go, and
// cmd/fkafka-runtime). A rename or drop on either side silently turns the
// corresponding setting into a runtime no-op -- a break neither a mock-based
// adapter test nor a func cluster test would surface.
//
// This is an adapter-side tripwire, not a cross-repo check. contractEnvVars is
// a hand-maintained list of the contract names, built from the Env* constants;
// it is not derived from the read sites, so keeping it complete is on the
// author. TestContractEnvVars pins that list against a literal copy of func's
// names -- it does NOT fetch or compare against knative/func. The two lists are
// kept in lockstep by humans, mirroring kafkaRuntimeContractEnvNames /
// TestKafkaSidecarContainer_EnvNameContract in knative/func
// (pkg/k8s/deployer_test.go). Change the two together; the test failure messages
// name func so a rename on one side is a prompt to update the other.
//
// Note: the error and log messages in this package repeat these names as plain
// string literals for readability. They are not generated from the Env*
// constants, so they are not a source of truth for the names -- the constants
// and contractEnvVars are.
const (
	// EnvFunctionTarget is the function URL the adapter POSTs decoded records to.
	// func sets it to a fixed value; read in cmd/fkafka-runtime.
	EnvFunctionTarget = "FUNCTION_TARGET"

	// Connection + security. Consumed in consumer.go and security.go.
	EnvKafkaBrokers          = "KAFKA_BROKERS"
	EnvKafkaTopic            = "KAFKA_TOPIC"
	EnvKafkaConsumerGroup    = "KAFKA_CONSUMER_GROUP"
	EnvKafkaSecurityProtocol = "KAFKA_SECURITY_PROTOCOL"
	EnvKafkaTLSCACert        = "KAFKA_TLS_CA_CERT"
	EnvKafkaTLSClientCert    = "KAFKA_TLS_CLIENT_CERT"
	EnvKafkaTLSClientKey     = "KAFKA_TLS_CLIENT_KEY"
	EnvKafkaTLSSkipVerify    = "KAFKA_TLS_SKIP_VERIFY"
	EnvKafkaSASLMechanism    = "KAFKA_SASL_MECHANISM"
	EnvKafkaSASLUser         = "KAFKA_SASL_USER"
	EnvKafkaSASLPassword     = "KAFKA_SASL_PASSWORD"

	// Delivery tuning. Consumed in delivery.go and http_adapter.go.
	EnvFunctionDeliveryRetry           = "FUNCTION_DELIVERY_RETRY"
	EnvFunctionDeliveryBackoffPolicy   = "FUNCTION_DELIVERY_BACKOFF_POLICY"
	EnvFunctionDeliveryBackoffDelay    = "FUNCTION_DELIVERY_BACKOFF_DELAY"
	EnvFunctionDeliveryBackoffMaxDelay = "FUNCTION_DELIVERY_BACKOFF_MAX_DELAY"
	EnvFunctionRequestTimeout          = "FUNCTION_REQUEST_TIMEOUT"
)

// Adapter-only env vars: read by the adapter but NOT part of the func contract,
// because func does not emit them. Kept out of contractEnvVars on purpose.
const (
	EnvFunctionMaxConcurrency = "FUNCTION_MAX_CONCURRENCY"
	EnvListenAddress          = "LISTEN_ADDRESS"
)

// contractEnvVars is the complete set of env var names this adapter reads that
// are part of the func<->adapter contract -- i.e. exactly the names func emits
// onto the sidecar. It is a hand-maintained literal mirror of
// kafkaRuntimeContractEnvNames in knative/func. The adapter-only variables above
// (FUNCTION_MAX_CONCURRENCY, LISTEN_ADDRESS) are deliberately excluded because
// func does not emit them.
var contractEnvVars = []string{
	EnvFunctionTarget,
	EnvKafkaBrokers,
	EnvKafkaTopic,
	EnvKafkaConsumerGroup,
	EnvKafkaSecurityProtocol,
	EnvKafkaTLSCACert,
	EnvKafkaTLSClientCert,
	EnvKafkaTLSClientKey,
	EnvKafkaTLSSkipVerify,
	EnvKafkaSASLMechanism,
	EnvKafkaSASLUser,
	EnvKafkaSASLPassword,
	EnvFunctionDeliveryRetry,
	EnvFunctionDeliveryBackoffPolicy,
	EnvFunctionDeliveryBackoffDelay,
	EnvFunctionDeliveryBackoffMaxDelay,
	EnvFunctionRequestTimeout,
}
