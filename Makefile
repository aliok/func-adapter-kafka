# func-kafka-adapter — build and test the standalone Kafka runtime.

BINDIR := bin
BINARY := $(BINDIR)/fkafka-runtime

# Container images are built with ko (see .ko.yaml), which cross-compiles the
# static Go binary and pins a non-root base. KO_DOCKER_REPO selects the
# destination; it defaults to ko.local for a local-only build.
KO_DOCKER_REPO ?= ko.local

.PHONY: all build test vet fmt check image clean

all: check build

## build: compile the runtime binary into ./bin
build:
	CGO_ENABLED=0 go build -trimpath -o $(BINARY) ./cmd/fkafka-runtime

## test: run unit tests
test:
	go test ./...

## vet: run go vet
vet:
	go vet ./...

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
