package kafka

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
)

func newTestEvent(t *testing.T) event.Event {
	t.Helper()
	e := event.New()
	e.SetID("test-id")
	e.SetSource("kafka://b:9092/t")
	e.SetType("dev.knative.kafka.event")
	if err := e.SetData("application/json", map[string]string{"hello": "world"}); err != nil {
		t.Fatalf("set data: %v", err)
	}
	return e
}

// A 2xx response is the ack: Invoke returns nil so the offset is committed. The
// server also confirms it received a binary-mode CloudEvent (Ce-* headers).
func TestHTTPAdapter_2xxCommits(t *testing.T) {
	var gotCEID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCEID = r.Header.Get("Ce-Id")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a, err := newHTTPAdapter(srv.URL)
	if err != nil {
		t.Fatalf("newHTTPAdapter: %v", err)
	}
	if _, err := a.Invoke(context.Background(), newTestEvent(t)); err != nil {
		t.Fatalf("Invoke returned error on 2xx: %v", err)
	}
	if gotCEID != "test-id" {
		t.Errorf("server did not receive a binary-mode CloudEvent (Ce-Id=%q); want test-id", gotCEID)
	}
}

// A non-2xx response is a failure: Invoke returns an error so the offset is NOT
// committed and the record is redelivered.
func TestHTTPAdapter_Non2xxRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	a, err := newHTTPAdapter(srv.URL)
	if err != nil {
		t.Fatalf("newHTTPAdapter: %v", err)
	}
	if _, err := a.Invoke(context.Background(), newTestEvent(t)); err == nil {
		t.Fatal("Invoke returned nil on 500; want error so the offset is not committed")
	}
}

// A function that accepts the connection but does not respond within the
// per-delivery timeout is a failure, so the record is retried instead of
// blocking the partition forever.
func TestHTTPAdapter_TimeoutFails(t *testing.T) {
	t.Setenv("FUNCTION_REQUEST_TIMEOUT", "100ms")

	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // hang until the test is done
	}))
	defer srv.Close()
	defer close(release)

	a, err := newHTTPAdapter(srv.URL)
	if err != nil {
		t.Fatalf("newHTTPAdapter: %v", err)
	}

	start := time.Now()
	if _, err := a.Invoke(context.Background(), newTestEvent(t)); err == nil {
		t.Fatal("Invoke returned nil for a non-responding function; want a timeout error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Invoke took %v; the per-delivery timeout did not fire", elapsed)
	}
}

func TestRequestTimeout(t *testing.T) {
	tests := []struct {
		name    string
		set     bool
		value   string
		want    time.Duration
		wantErr bool
	}{
		{name: "unset uses default", set: false, want: DefaultRequestTimeout},
		{name: "empty uses default", set: true, value: "", want: DefaultRequestTimeout},
		{name: "custom duration", set: true, value: "45s", want: 45 * time.Second},
		{name: "zero disables", set: true, value: "0", want: 0},
		{name: "invalid", set: true, value: "banana", wantErr: true},
		{name: "negative", set: true, value: "-5s", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				t.Setenv("FUNCTION_REQUEST_TIMEOUT", tt.value)
			} else {
				t.Setenv("FUNCTION_REQUEST_TIMEOUT", "")
				os.Unsetenv("FUNCTION_REQUEST_TIMEOUT")
			}
			got, err := requestTimeout()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("requestTimeout() error = nil; want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("requestTimeout() unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("requestTimeout() = %v, want %v", got, tt.want)
			}
		})
	}
}

// A transport error (nothing listening) is also a failure.
func TestHTTPAdapter_ConnectionErrorRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	target := srv.URL
	srv.Close() // nothing is listening on target now

	a, err := newHTTPAdapter(target)
	if err != nil {
		t.Fatalf("newHTTPAdapter: %v", err)
	}
	if _, err := a.Invoke(context.Background(), newTestEvent(t)); err == nil {
		t.Fatal("Invoke returned nil on connection failure; want error")
	}
}

// A CloudEvent in the HTTP response is returned as the response event (the
// future produce-back hook); today the consumer ignores it.
func TestHTTPAdapter_ResponseEventReturned(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Ce-Specversion", "1.0")
		w.Header().Set("Ce-Id", "resp-id")
		w.Header().Set("Ce-Source", "/fn")
		w.Header().Set("Ce-Type", "response")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a, err := newHTTPAdapter(srv.URL)
	if err != nil {
		t.Fatalf("newHTTPAdapter: %v", err)
	}
	resp, err := a.Invoke(context.Background(), newTestEvent(t))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if resp == nil {
		t.Fatal("expected a response event to be returned")
	}
	if resp.ID() != "resp-id" {
		t.Errorf("resp id = %q, want resp-id", resp.ID())
	}
}

// The concurrency cap bounds how many deliveries are in flight at once. With
// FUNCTION_MAX_CONCURRENCY=2, a server that blocks until released must never see
// more than 2 concurrent requests even when many Invokes run at once.
func TestHTTPAdapter_MaxConcurrency(t *testing.T) {
	t.Setenv("FUNCTION_MAX_CONCURRENCY", "2")

	var inFlight, maxSeen int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			old := atomic.LoadInt32(&maxSeen)
			if n <= old || atomic.CompareAndSwapInt32(&maxSeen, old, n) {
				break
			}
		}
		<-release // hold the request open until the test releases it
		atomic.AddInt32(&inFlight, -1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a, err := newHTTPAdapter(srv.URL)
	if err != nil {
		t.Fatalf("newHTTPAdapter: %v", err)
	}

	const callers = 8
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = a.Invoke(context.Background(), newTestEvent(t))
		}()
	}

	// Give the goroutines time to pile up against the cap, then release them.
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt32(&maxSeen); got > 2 {
		close(release)
		wg.Wait()
		t.Fatalf("observed %d concurrent requests; cap of 2 was not enforced", got)
	}
	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&maxSeen); got == 0 {
		t.Fatal("no requests reached the server")
	}
}

// With the cap disabled (0), all callers reach the server concurrently.
func TestHTTPAdapter_MaxConcurrencyDisabled(t *testing.T) {
	t.Setenv("FUNCTION_MAX_CONCURRENCY", "0")

	const callers = 6
	var inFlight, maxSeen int32
	gate := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			old := atomic.LoadInt32(&maxSeen)
			if n <= old || atomic.CompareAndSwapInt32(&maxSeen, old, n) {
				break
			}
		}
		<-gate
		atomic.AddInt32(&inFlight, -1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a, err := newHTTPAdapter(srv.URL)
	if err != nil {
		t.Fatalf("newHTTPAdapter: %v", err)
	}
	if a.sem != nil {
		t.Fatal("expected sem to be nil when cap is disabled")
	}

	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = a.Invoke(context.Background(), newTestEvent(t))
		}()
	}
	time.Sleep(200 * time.Millisecond)
	got := atomic.LoadInt32(&maxSeen)
	close(gate)
	wg.Wait()
	if got != callers {
		t.Fatalf("concurrent requests = %d, want %d (cap disabled)", got, callers)
	}
}
