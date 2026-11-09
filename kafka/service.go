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

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	DefaultListenAddress  = "[::]:8080"
	ServerShutdownTimeout = 30 * time.Second

	// defaultTargetWaitTimeout bounds how long Start waits for the function target
	// to accept connections before giving up (and letting the process exit so the
	// kubelet restarts it). Sized for a slow cold start such as a JVM function.
	defaultTargetWaitTimeout = 60 * time.Second
)

// Service consumes Kafka records and delivers each one as a binary-mode
// CloudEvent to the function through a KafkaAdapter. A small HTTP server runs
// alongside for health probes only (it does not receive events).
type Service struct {
	http.Server
	// listenerMu guards listener: Start assigns it from its own goroutine while
	// Addr may be read concurrently (e.g. a caller discovering the bound address
	// after Start begins).
	listenerMu    sync.Mutex
	listener      net.Listener
	adapter       KafkaAdapter
	stop          chan error
	ready         atomic.Bool
	cancelConsume context.CancelFunc
	// target is the function URL the runtime delivers to; Start waits for it to
	// accept connections before it begins consuming.
	target string
	// targetWaitTimeout bounds the wait for target. Defaults to
	// defaultTargetWaitTimeout; overridable in tests.
	targetWaitTimeout time.Duration
	// consumeDone is closed when the consumer goroutine has fully returned (and
	// therefore its deferred client.Close() has committed final offsets and left
	// the consumer group). shutdown waits on it so the process does not exit
	// mid-delivery or before a clean group close.
	consumeDone chan struct{}
	// shutdownTimeout bounds how long shutdown waits for the consumer to stop and
	// the health server to close. Defaults to ServerShutdownTimeout; overridable
	// in tests.
	shutdownTimeout time.Duration
}

// New creates a Service that delivers records to a function running as its own
// CloudEvents-over-HTTP server at target (e.g. "http://127.0.0.1:8080/").
func New(target string) (*Service, error) {
	adapter, err := newHTTPAdapter(target)
	if err != nil {
		return nil, err
	}
	svc := newService(adapter)
	svc.target = target
	return svc, nil
}

func newService(adapter KafkaAdapter) *Service {
	svc := &Service{
		adapter:           adapter,
		stop:              make(chan error, 1),
		shutdownTimeout:   ServerShutdownTimeout,
		targetWaitTimeout: defaultTargetWaitTimeout,
		Server: http.Server{
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       30 * time.Second,
			MaxHeaderBytes:    1 << 20,
			ReadHeaderTimeout: 2 * time.Second,
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health/readiness", svc.Ready)
	mux.HandleFunc("/health/liveness", svc.Alive)
	svc.Handler = mux
	return svc
}

// Start the Kafka consumer and the health HTTP server, blocking until a signal
// or an unrecoverable error.
func (s *Service) Start(ctx context.Context) (err error) {
	addr := listenAddress()
	log.Debug().Str("address", addr).Msg("kafka runtime starting")

	// Parse and validate delivery configuration before the health server starts or
	// the function target is awaited, so a malformed FUNCTION_DELIVERY_* value
	// fails immediately at process startup. Validating it inside consume (after the
	// target wait) meant a bad value was masked by -- and reported only after -- a
	// target-wait timeout when the function was slow to start or unavailable.
	deliveryCfg, err := loadDeliveryConfig()
	if err != nil {
		return fmt.Errorf("configuring delivery: %w", err)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.setListener(listener)

	s.handleSignals()

	// Serve health immediately -- before waiting for the function target -- so the
	// runtime reports liveness (ALIVE) throughout a slow function cold start.
	// Readiness stays 503 until the consumer has partitions assigned. Previously
	// the health server came up only after the target wait, so an injected
	// liveness probe saw connection-refused during the wait and the kubelet could
	// restart-loop a slow-starting (e.g. JVM) function before it was ever up.
	go func() {
		if err := s.Serve(listener); err != http.ErrServerClosed {
			log.Error().Err(err).Msg("health server exited with unexpected error")
			s.sendStop(err)
		}
	}()

	consumerCtx, cancelConsume := context.WithCancel(ctx)
	s.cancelConsume = cancelConsume
	s.consumeDone = make(chan struct{})
	go func() {
		// Close consumeDone last (after any sendStop) so a waiter in shutdown only
		// unblocks once consume has fully returned and run its deferred cleanup.
		defer close(s.consumeDone)

		// Wait for the function to accept connections before consuming, so the first
		// records aren't needlessly redelivered while it starts up. A timeout or an
		// invalid target is a runtime failure -- stop so the process exits and the
		// kubelet restarts it; a cancelled context is a clean shutdown, not an error.
		if err := waitForTarget(consumerCtx, s.target, s.targetWaitTimeout); err != nil {
			if consumerCtx.Err() == nil {
				s.sendStop(err)
			}
			return
		}

		if err := consume(consumerCtx, s.adapter, &s.ready, deliveryCfg); err != nil {
			log.Error().Err(err).Msg("kafka consumer exited with error")
			s.sendStop(err)
		}
	}()

	log.Debug().Msg("waiting for stop signals or errors")
	select {
	case err = <-s.stop:
		if err != nil {
			log.Error().Err(err).Msg("runtime error")
		}
	case <-ctx.Done():
		log.Debug().Msg("runtime canceled")
	}
	return s.shutdown(err)
}

func (s *Service) setListener(l net.Listener) {
	s.listenerMu.Lock()
	s.listener = l
	s.listenerMu.Unlock()
}

// Addr returns the address upon which the health server is listening, or nil if
// it has not started listening yet.
func (s *Service) Addr() net.Addr {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Ready reports readiness: the consumer must have partitions assigned.
func (s *Service) Ready(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, "kafka consumer not yet ready")
		return
	}
	fmt.Fprintf(w, "READY")
}

// Alive reports liveness. The process is alive as long as it is serving.
func (s *Service) Alive(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ALIVE")
}

func (s *Service) handleSignals() {
	// Only intercept the termination signals we act on. Passing no signals to
	// signal.Notify would capture every catchable signal and suppress the default
	// behavior of ones we do not handle (e.g. SIGHUP, SIGQUIT).
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for sig := range sigs {
			log.Debug().Any("signal", sig).Msg("signal received")
			s.sendStop(nil)
		}
	}()
}

func (s *Service) sendStop(err error) {
	select {
	case s.stop <- err:
	default:
	}
}

func (s *Service) shutdown(sourceErr error) error {
	log.Debug().Msg("kafka runtime stopping")
	if s.cancelConsume != nil {
		s.cancelConsume()
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()

	// Wait for the consumer goroutine to return before exiting. Cancelling its
	// context (above) only signals it to stop; its deferred client.Close() — which
	// commits the final offsets and leaves the consumer group cleanly — runs as it
	// unwinds. Without this join the process could exit first, dropping an
	// in-flight delivery or skipping the clean group close. The wait shares the
	// shutdown deadline so a wedged in-flight delivery cannot block exit beyond
	// the shutdown timeout.
	var consumeErr error
	if s.consumeDone != nil {
		select {
		case <-s.consumeDone:
		case <-ctx.Done():
			consumeErr = fmt.Errorf("kafka consumer did not stop within %s: %w", s.shutdownTimeout, ctx.Err())
			log.Warn().Dur("timeout", s.shutdownTimeout).Msg("kafka consumer did not stop in time; forcing shutdown")
		}
	}

	runtimeErr := s.Shutdown(ctx)

	return collapseErrors("shutdown error", sourceErr, consumeErr, runtimeErr)
}

func listenAddress() string {
	if v := os.Getenv("LISTEN_ADDRESS"); v != "" {
		return v
	}
	return DefaultListenAddress
}

// waitForTarget blocks until a TCP connection to the function target succeeds,
// the timeout elapses, or ctx is cancelled.
func waitForTarget(ctx context.Context, target string, timeout time.Duration) error {
	u, err := url.Parse(target)
	if err != nil {
		return fmt.Errorf("invalid FUNCTION_TARGET %q: %w", target, err)
	}
	// Require an absolute http(s) URL with a host. url.Parse accepts a bare word
	// ("myfunc") as a relative URL with an empty host, which would otherwise
	// silently resolve to probing ":80" on localhost instead of failing fast on a
	// misconfiguration.
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid FUNCTION_TARGET %q: must be an absolute http or https URL", target)
	}
	if u.Hostname() == "" {
		return fmt.Errorf("invalid FUNCTION_TARGET %q: missing host", target)
	}

	host := u.Host
	if u.Port() == "" {
		port := "80"
		if u.Scheme == "https" {
			port = "443"
		}
		host = net.JoinHostPort(u.Hostname(), port)
	}

	// Bound the whole wait with a context so the configured timeout and any
	// cancellation are honored promptly -- including mid-dial. A context-aware
	// dialer (rather than net.DialTimeout with a fixed per-dial timeout) means a
	// short timeout or a shutdown does not have to wait out a dial against a
	// black-holed address.
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var dialer net.Dialer
	for {
		conn, err := dialer.DialContext(dialCtx, "tcp", host)
		if err == nil {
			_ = conn.Close()
			log.Info().Str("target", target).Msg("function target is reachable; starting consumer")
			return nil
		}
		// Check the parent first: if it was cancelled, the child inherited the
		// cancellation, but we want to report context.Canceled (a clean shutdown),
		// not a timeout.
		if err := ctx.Err(); err != nil {
			return err
		}
		if dialCtx.Err() != nil {
			return fmt.Errorf("timed out after %s waiting for function target %s", timeout, target)
		}
		log.Info().Str("target", target).Msg("waiting for function target to become reachable")
		select {
		case <-dialCtx.Done():
		case <-time.After(time.Second):
		}
	}
}

func collapseErrors(msg string, ee ...error) (err error) {
	for _, e := range ee {
		if e != nil {
			if err == nil {
				err = e
			} else {
				log.Error().Err(e).Msg(msg)
			}
		}
	}
	return
}
