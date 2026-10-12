# func-kafka-adapter — build and test the standalone Kafka runtime.

IMAGE ?= func-kafka-adapter:dev
BINDIR := bin
BINARY := $(BINDIR)/fkafka-runtime

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

## image: build the container image
image:
	docker build -t $(IMAGE) .

## clean: remove build artifacts
clean:
	rm -rf $(BINDIR)
