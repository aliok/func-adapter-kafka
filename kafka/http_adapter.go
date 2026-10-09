package kafka

import (
	"context"
	"fmt"
	nethttp "net/http"
	"os"
	"strconv"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/rs/zerolog/log"
)

// DefaultRequestTimeout bounds a single delivery attempt to the function. It is
// overridable with the FUNCTION_REQUEST_TIMEOUT environment variable (a Go
// duration, e.g. "45s"; "0" disables the timeout). Without it, a function that
// accepts a connection but never responds would block a partition forever.
const DefaultRequestTimeout = 30 * time.Second

// DefaultMaxConcurrency caps the number of deliveries in flight to the function
// at once, across all partitions. It is overridable with the
// FUNCTION_MAX_CONCURRENCY environment variable (a non-negative integer; "0"
// disables the cap).
//
// This is defense-in-depth. Delivery concurrency is naturally bounded by the
// number of partitions assigned to this consumer, and a correct CloudEvents
// receiver handles concurrent requests fine. But a receiver that mishandles
// mid-flight cancellation can stall a delivery per concurrent request; bounding
// concurrency bounds how many deliveries (and connections and goroutines) can be
// stuck at once, so a misbehaving function degrades gracefully instead of
// pinning one delivery per partition without limit.
const DefaultMaxConcurrency = 16

// httpAdapter delivers each event to the user function over localhost HTTP as a
// binary-mode CloudEvent. It is the go-forward adapter used by the standalone
// runtime: the function runs as its own CloudEvents-over-HTTP server (in a
// sidecar container or, later, a sibling sub-process) and the runtime POSTs to
// it. A 2xx response is the offset-commit ack; any other status, a transport
// error, or a timeout is a failure that triggers redelivery.
//
// Invoke performs a single attempt; retrying transient failures is the caller's
// responsibility (see the consumer's delivery loop).
type httpAdapter struct {
	client  cloudevents.Client
	target  string
	timeout time.Duration
	// sem bounds concurrent in-flight deliveries. nil means unbounded. A slot is
	// held only for the duration of a single Invoke attempt, so a record waiting
	// out its retry backoff does not occupy one.
	sem chan struct{}
}

// newHTTPAdapter builds an httpAdapter that POSTs to target, e.g.
// "http://127.0.0.1:8080/".
func newHTTPAdapter(target string) (*httpAdapter, error) {
	// Deliver over a transport that does NOT pool/reuse connections. Delivery is
	// to a localhost sidecar (or sibling sub-process), so connection setup is
	// cheap, while pooled HTTP/1.1 keep-alive connections have proven to wedge:
	// once a delivery is cancelled mid-flight (a per-attempt timeout, or a
	// function restart producing an EOF), the pooled connection's request/
	// response framing desyncs. Every subsequent reuse then blocks until the
	// deadline — the function receives and answers the request, but the client
	// never reads the response, so at-least-once redelivery loops forever and the
	// partition never drains. A fresh connection per delivery eliminates this
	// entire class of failure. It also keeps the per-attempt context timeout as
	// the sole bound on a genuinely hung function.
	transport := &nethttp.Transport{DisableKeepAlives: true}
	p, err := cehttp.New(cehttp.WithTarget(target), cehttp.WithRoundTripper(transport))
	if err != nil {
		return nil, fmt.Errorf("creating cloudevents http protocol: %w", err)
	}
	c, err := cloudevents.NewClient(p)
	if err != nil {
		return nil, fmt.Errorf("creating cloudevents client: %w", err)
	}
	timeout, err := requestTimeout()
	if err != nil {
		return nil, err
	}
	maxConcurrency, err := maxConcurrency()
	if err != nil {
		return nil, err
	}
	var sem chan struct{}
	if maxConcurrency > 0 {
		sem = make(chan struct{}, maxConcurrency)
	}
	log.Info().
		Str("target", target).
		Dur("requestTimeout", timeout).
		Int("maxConcurrency", maxConcurrency).
		Msg("delivery configured")
	return &httpAdapter{client: c, target: target, timeout: timeout, sem: sem}, nil
}

// requestTimeout returns the per-attempt delivery timeout, honoring the
// FUNCTION_REQUEST_TIMEOUT override.
func requestTimeout() (time.Duration, error) {
	v, ok := os.LookupEnv("FUNCTION_REQUEST_TIMEOUT")
	if !ok || v == "" {
		return DefaultRequestTimeout, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid FUNCTION_REQUEST_TIMEOUT %q: %w", v, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("invalid FUNCTION_REQUEST_TIMEOUT %q: must not be negative", v)
	}
	return d, nil
}

// maxConcurrency returns the maximum number of concurrent in-flight deliveries,
// honoring the FUNCTION_MAX_CONCURRENCY override. 0 means unbounded.
func maxConcurrency() (int, error) {
	v, ok := os.LookupEnv("FUNCTION_MAX_CONCURRENCY")
	if !ok || v == "" {
		return DefaultMaxConcurrency, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid FUNCTION_MAX_CONCURRENCY %q: %w", v, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid FUNCTION_MAX_CONCURRENCY %q: must not be negative", v)
	}
	return n, nil
}

func (a *httpAdapter) Invoke(ctx context.Context, e event.Event) (*event.Event, error) {
	// Bound concurrent deliveries (defense-in-depth). Respect cancellation while
	// waiting for a slot so a rebalance/shutdown does not block here.
	if a.sem != nil {
		select {
		case a.sem <- struct{}{}:
			defer func() { <-a.sem }()
		case <-ctx.Done():
			return nil, fmt.Errorf("delivering event to %s aborted while awaiting concurrency slot: %w", a.target, ctx.Err())
		}
	}

	if a.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.timeout)
		defer cancel()
	}
	ctx = cloudevents.ContextWithTarget(ctx, a.target)
	ctx = cloudevents.WithEncodingBinary(ctx)

	resp, result := a.client.Request(ctx, e)

	// The commit ack is a 2xx HTTP status. Check the status explicitly rather
	// than relying on IsACK, which treats a delivered-but-rejected response
	// (e.g. 500) as an ack.
	var httpResult *cehttp.Result
	if cloudevents.ResultAs(result, &httpResult) {
		if httpResult.StatusCode >= 200 && httpResult.StatusCode < 300 {
			return resp, nil
		}
		return nil, fmt.Errorf("function returned non-2xx status %d: %w", httpResult.StatusCode, result)
	}

	// No HTTP status available means the request never completed (transport
	// error, timeout): treat as a failure so the record is redelivered.
	return nil, fmt.Errorf("delivering event to %s failed: %w", a.target, result)
}
