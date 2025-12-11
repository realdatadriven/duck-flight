package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"flag"
	"context"

	"github.com/realdatadriven/duck-flight/internal/config"
	"github.com/realdatadriven/duck-flight/internal/duckmanager"
	"github.com/realdatadriven/duck-flight/internal/flight"
	"github.com/realdatadriven/duck-flight/internal/server"
)

func main() {
	var cfgPath string
	var addr string

	flag.StringVar(&cfgPath, "config", "configs/config.yaml", "Path to YAML configuration")
	flag.StringVar(&addr, "addr", "127.0.0.1:50051", "Address to bind the server (gRPC)")
	flag.Parse()

	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	log.Printf("loaded config: %s", cfg.Description)

	// Create DuckManager which manages the DuckDB instance and lifecycle SQL
	manager, err := duckmanager.NewDuckManager(cfg)
	if err != nil {
		log.Fatalf("failed to initialize DuckManager: %v", err)
	}

	// Run startup lifecycle (before_sql + main_sql)
	if err := manager.Startup(); err != nil {
		log.Fatalf("duckmanager startup failed: %v", err)
	}
	// Ensure teardown on exit
	defer func() {
		if err := manager.Shutdown(); err != nil {
			log.Printf("duckmanager shutdown error: %v", err)
		}
	}()

	// Create Flight adapter (airport-go) backed by our manager.
	flightMgr := flight.NewAirportAdapter(manager)

	// Create and start server
	srv := server.NewServer(addr, cfg, manager, flightMgr)

	// Start the server (includes starting airport-go Flight server)
	if err := srv.Start(); err != nil {
		log.Fatalf("server failed to start: %v", err)
	}
	log.Printf("server started at %s", addr)

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Print("shutting down...")
	// give Flight manager a chance to stop
	if err := srv.Stop(); err != nil {
		log.Printf("error during shutdown: %v", err)
	}
	// ensure manager shutdown (deferred previously) -- allow small wait
	_ = context.Background()
	log.Println("goodbye")
}
