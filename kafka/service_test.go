package kafka

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	svc, err := New("http://127.0.0.1:8080/")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return svc
}

func TestReady_NotReady(t *testing.T) {
	svc := newTestService(t)
	// ready defaults to false (no partitions assigned yet)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/health/readiness", nil)
	svc.Ready(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}
}

func TestReady_Ready(t *testing.T) {
	svc := newTestService(t)
	svc.ready.Store(true)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/health/readiness", nil)
	svc.Ready(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}
	if w.Body.String() != "READY" {
		t.Errorf("expected body %q, got %q", "READY", w.Body.String())
	}
}

func TestAlive_Default(t *testing.T) {
	svc := newTestService(t)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/health/liveness", nil)
	svc.Alive(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}
	if w.Body.String() != "ALIVE" {
		t.Errorf("expected body %q, got %q", "ALIVE", w.Body.String())
	}
}
