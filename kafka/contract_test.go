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

import "testing"

// goldenContractEnvVars is the exact set of env var names knative/func emits onto
// the Kafka runtime sidecar, copied verbatim from kafkaRuntimeContractEnvNames in
// knative/func (pkg/k8s/deployer_test.go). It is intentionally duplicated here as
// literal strings so the test does not lean on the adapter's own constants to
// decide what func emits: contractEnvVars is built from this package's Env*
// constants -- the same constants the production read sites use -- so comparing
// it against these literals catches a renamed or dropped constant on the adapter
// side. This is not a cross-repo check -- nothing fetches func's list; these
// literals are kept aligned with it by hand. If a variable is genuinely renamed
// or dropped, change both lists (and the func side) together.
var goldenContractEnvVars = []string{
	"FUNCTION_TARGET",
	"KAFKA_BROKERS",
	"KAFKA_TOPIC",
	"KAFKA_CONSUMER_GROUP",
	"KAFKA_SECURITY_PROTOCOL",
	"KAFKA_TLS_CA_CERT",
	"KAFKA_TLS_CLIENT_CERT",
	"KAFKA_TLS_CLIENT_KEY",
	"KAFKA_TLS_SKIP_VERIFY",
	"KAFKA_SASL_MECHANISM",
	"KAFKA_SASL_USER",
	"KAFKA_SASL_PASSWORD",
	"FUNCTION_DELIVERY_RETRY",
	"FUNCTION_DELIVERY_BACKOFF_POLICY",
	"FUNCTION_DELIVERY_BACKOFF_DELAY",
	"FUNCTION_DELIVERY_BACKOFF_MAX_DELAY",
	"FUNCTION_REQUEST_TIMEOUT",
}

// TestContractEnvVars pins the adapter's half of the func<->adapter env-var name
// contract. It is the hand-kept mirror of TestKafkaSidecarContainer_EnvNameContract
// in knative/func: that test asserts the names func emits; this one asserts the
// names the adapter lists as the ones it reads. Neither test compares against the
// other repo -- each pins its own side against a local copy, and the two copies
// are kept in lockstep by humans. A rename or drop applied to only one side (which
// would silently no-op a setting at runtime) fails a cheap unit test here instead
// of surfacing only as a flaky cluster run or not at all.
func TestContractEnvVars(t *testing.T) {
	// A duplicate would be masked by the set comparison below, so catch it first.
	seen := map[string]bool{}
	for _, n := range contractEnvVars {
		if seen[n] {
			t.Errorf("contractEnvVars contains duplicate %q", n)
		}
		seen[n] = true
	}

	want := envNameSet(goldenContractEnvVars)
	got := envNameSet(contractEnvVars)

	for n := range want {
		if !got[n] {
			t.Errorf("contractEnvVars is missing %q that func emits; if the name was intentionally renamed or dropped, update contractEnvVars AND knative/func's kafkaRuntimeContractEnvNames together", n)
		}
	}
	for n := range got {
		if !want[n] {
			t.Errorf("contractEnvVars reads %q that func does not emit; add it to knative/func's kafkaRuntimeContractEnvNames, or move it to the adapter-only env vars in contract.go", n)
		}
	}
}

func envNameSet(names []string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}
