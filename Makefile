# func-kafka-adapter — build and test the standalone Kafka runtime.

BINDIR := bin
BINARY := $(BINDIR)/fkafka-runtime

# Container images are built with ko (see .ko.yaml), which cross-compiles the
# static Go binary and pins a non-root base. KO_DOCKER_REPO selects the
# destination; it defaults to ko.local for a local-only build.
KO_DOCKER_REPO ?= ko.local

.PHONY: all build test test-e2e vet fmt check image clean

all: check build

## build: compile the runtime binary into ./bin
build:
	CGO_ENABLED=0 go build -trimpath -o $(BINARY) ./cmd/fkafka-runtime

## test: run unit tests
test:
	go test ./...

## test-e2e: run the real-broker delivery/retry E2E suite (build tag: e2e).
## Requires a reachable Kafka: set KAFKA_E2E_BOOTSTRAP to its bootstrap address
## (e.g. localhost:9092). Run with the race detector: this is the only suite that
## exercises the real shutdown/consumeDone and sarama consumer-group concurrency.
## Unlike a bare `go test -tags e2e ./...` (where an unset variable correctly skips),
## this entry point fails if the variable is unset -- its whole job is to run the
## suite, so a skip-and-pass here would hide that zero scenarios ran.
## Note: the suite runs the production runtime in-process, and its signal handler keeps
## SIGINT/SIGTERM for the process, so Ctrl-C does NOT interrupt a running suite -- use
## Ctrl-\ (SIGQUIT) or kill the process to abort.
test-e2e:
	@test -n "$$KAFKA_E2E_BOOTSTRAP" || { echo "KAFKA_E2E_BOOTSTRAP must be set (e.g. localhost:9092) to run the E2E suite" >&2; exit 1; }
	go test -tags e2e -race -count=1 -timeout 15m ./kafka/ -run '^TestE2E'

## vet: run go vet, including the e2e-tagged files (the default build excludes them).
vet:
	go vet ./...
	go vet -tags e2e ./...

## fmt: check formatting (fails if any file needs gofmt)
fmt:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

## check: fmt + vet + test
check: fmt vet test

## image: build the container image with ko into $(KO_DOCKER_REPO)
image:
	KO_DOCKER_REPO=$(KO_DOCKER_REPO) ko build --bare ./cmd/fkafka-runtime

## clean: remove build artifacts
clean:
	rm -rf $(BINDIR)
