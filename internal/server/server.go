package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/realdatadriven/duck-flight/internal/config"
	"github.com/realdatadriven/duck-flight/internal/duckmanager"
	"github.com/realdatadriven/duck-flight/internal/flight"
)

// Server holds runtime state for the Duck-Flight server.
type Server struct {
	addr    string
	cfg     *config.ServerConfig
	manager *duckmanager.DuckManager

	httpSrv *http.Server
	flight  flight.FlightManager
}

// NewServer constructs the server wrapper.
func NewServer(addr string, cfg *config.ServerConfig, manager *duckmanager.DuckManager, fm flight.FlightManager) *Server {
	return &Server{
		addr:    addr,
		cfg:     cfg,
		manager: manager,
		flight:  fm,
	}
}

// Start boots the HTTP health endpoint and starts the FlightManager if provided.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.healthHandler)

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}

	s.httpSrv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("[server] HTTP health listening on %s", s.addr)
		if err := s.httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[server] HTTP server error: %v", err)
		}
	}()

	if s.flight != nil {
		log.Printf("[server] starting Flight manager (airport-go adapter)")
		if err := s.flight.Start(s.addr); err != nil {
			return fmt.Errorf("start flight manager: %w", err)
		}
	}
	return nil
}

func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if s.flight != nil {
		_ = s.flight.Stop(ctx)
	}

	if s.httpSrv != nil {
		if err := s.httpSrv.Shutdown(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	// Basic health check; in future include catalog/engine checks.
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}
