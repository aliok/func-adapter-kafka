package kafka

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	DefaultListenAddress  = "[::]:8080"
	ServerShutdownTimeout = 30 * time.Second
)

// Service consumes Kafka records and delivers each one as a binary-mode
// CloudEvent to the function through a KafkaAdapter. A small HTTP server runs
// alongside for health probes only (it does not receive events).
type Service struct {
	http.Server
	listener      net.Listener
	adapter       KafkaAdapter
	stop          chan error
	ready         atomic.Bool
	cancelConsume context.CancelFunc
}

// New creates a Service that delivers records to a function running as its own
// CloudEvents-over-HTTP server at target (e.g. "http://127.0.0.1:8080/").
func New(target string) (*Service, error) {
	adapter, err := newHTTPAdapter(target)
	if err != nil {
		return nil, err
	}
	return newService(adapter), nil
}

func newService(adapter KafkaAdapter) *Service {
	svc := &Service{
		adapter: adapter,
		stop:    make(chan error, 1),
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

	if s.listener, err = net.Listen("tcp", addr); err != nil {
		return
	}

	s.handleSignals()

	go func() {
		if err := s.Serve(s.listener); err != http.ErrServerClosed {
			log.Error().Err(err).Msg("health server exited with unexpected error")
			s.sendStop(err)
		}
	}()

	consumerCtx, cancelConsume := context.WithCancel(ctx)
	s.cancelConsume = cancelConsume
	go func() {
		if err := consume(consumerCtx, s.adapter, &s.ready); err != nil {
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

// Addr returns the address upon which the health server is listening.
func (s *Service) Addr() net.Addr {
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
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs)
	go func() {
		for {
			sig := <-sigs
			if sig == syscall.SIGINT || sig == syscall.SIGTERM {
				log.Debug().Any("signal", sig).Msg("signal received")
				s.sendStop(nil)
			} else if runtime.GOOS == "linux" && sig == syscall.Signal(0x17) {
				// Ignore SIGURG
			}
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

	ctx, cancel := context.WithTimeout(context.Background(), ServerShutdownTimeout)
	defer cancel()
	runtimeErr := s.Shutdown(ctx)

	return collapseErrors("shutdown error", sourceErr, runtimeErr)
}

func listenAddress() string {
	if v := os.Getenv("LISTEN_ADDRESS"); v != "" {
		return v
	}
	return DefaultListenAddress
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
