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

// Command fkafka-runtime is the standalone, language-agnostic Kafka runtime.
//
// It consumes records from Kafka and delivers each one as a binary-mode
// CloudEvent over localhost HTTP to a function that runs as its own
// CloudEvents-over-HTTP server. A 2xx response commits the offset; anything
// else leaves the record for redelivery. Because the function is reached over
// HTTP, it can be written in any language.
//
// This is the binary that runs in the Kafka sidecar container (and, later, as a
// sibling sub-process for local `func run`).
//
// Configuration (environment variables):
//
//	KAFKA_BROKERS        — comma-separated broker addresses (required)
//	KAFKA_TOPIC          — topic to consume from (required)
//	KAFKA_CONSUMER_GROUP — consumer group ID (required)
//	FUNCTION_TARGET      — function URL (default http://127.0.0.1:8080/)
//	LISTEN_ADDRESS       — health server address (default [::]:8081, chosen to
//	                       avoid clashing with the function's own 8080)
//	KAFKA_SECURITY_PROTOCOL, KAFKA_TLS_*, KAFKA_SASL_* — auth (see kafka pkg)
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"knative.dev/func-adapter-kafka/kafka"
)

const (
	defaultFunctionTarget = "http://127.0.0.1:8080/"
	defaultListenAddress  = "[::]:8081"
)

func main() {
	// The runtime's own health server must not collide with the function's
	// 8080 in the shared network namespace of a sidecar Pod.
	if os.Getenv(kafka.EnvListenAddress) == "" {
		os.Setenv(kafka.EnvListenAddress, defaultListenAddress)
	}

	// Start serves health immediately and waits for the function target before
	// consuming, so a slow-starting function is not restart-looped by its
	// liveness probe.
	svc, err := kafka.New(functionTarget())
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	if err := svc.Start(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func functionTarget() string {
	if v := strings.TrimSpace(os.Getenv(kafka.EnvFunctionTarget)); v != "" {
		return v
	}
	return defaultFunctionTarget
}
