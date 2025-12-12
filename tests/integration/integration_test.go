package integration

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestDuckFlightWithDuckDBCLI(t *testing.T) {
	// Skip if dependencies are not available
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go binary not found in PATH")
	}

	// Install DuckDB if it doesn't exist
	if _, err := exec.LookPath("duckdb"); err != nil {
		t.Logf("duckdb binary not found, attempting to install")
		installCmd := exec.Command("bash", "-c", "curl https://install.duckdb.org | sh")
		var out bytes.Buffer
		installCmd.Stdout = &out
		installCmd.Stderr = &out

		if err := installCmd.Run(); err != nil {
			t.Fatalf("failed to install duckdb: %v\noutput: %s", err, out.String())
		}
		t.Logf("DuckDB installed successfully")
	}

	// Create the TPC-H test database if it doesn't exist
	dbPath := "database/test.db"
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Logf("Database %s not found, creating with TPC-H data", dbPath)

		// Ensure the directory exists
		dbDir := filepath.Dir(dbPath)
		if err := os.MkdirAll(dbDir, 0755); err != nil {
			t.Fatalf("failed to create database directory: %v", err)
		}

		// Run DuckDB to generate TPC-H database
		setupSQL := `INSTALL tpch; LOAD tpch; CALL dbgen(sf=0.1); ATTACH 'database/test.db' (TYPE SQLITE); COPY FROM DATABASE memory TO test; DETACH test`
		setupCmd := exec.Command("duckdb", "-c", setupSQL)
		var out bytes.Buffer
		setupCmd.Stdout = &out
		setupCmd.Stderr = &out

		if err := setupCmd.Run(); err != nil {
			t.Fatalf("failed to create TPC-H database: %v\noutput: %s", err, out.String())
		}
		t.Logf("TPC-H database created successfully")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the server with the requested build tag and config
	cmd := exec.CommandContext(ctx, "go", "run", "-tags=duckdb_arrow", "./cmd/duckflight", "--config", "examples/config.yaml")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	// Ensure the server process is cleaned up
	defer func() {
		cancel()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	// Wait for the server to accept connections on 127.0.0.1:8088
	addr := "grpc://127.0.0.1:50051"
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			goto ready
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("server did not start listening on %s within timeout", addr)

ready:
	// Run the DuckDB CLI to exercise INSTALL/LOAD/ATTACH and a simple query
	sql := `INSTALL airport FROM community; LOAD airport; ATTACH '' AS my_server (TYPE AIRPORT, LOCATION 'grpc://127.0.0.1:50051'); SELECT * FROM my_server.my_schema.orders LIMIT 10;`

	var out bytes.Buffer
	duck := exec.Command("duckdb", "-c", sql)
	duck.Stdout = &out
	duck.Stderr = &out

	// Run with a timeout to avoid hanging the test
	done := make(chan error, 1)
	go func() { done <- duck.Run() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("duckdb CLI failed: %v\noutput:\n%s", err, out.String())
		}
		t.Logf("duckdb output:\n%s", out.String())
	case <-time.After(30 * time.Second):
		_ = duck.Process.Kill()
		t.Fatalf("duckdb CLI timed out\npartial output:\n%s", out.String())
	}
}
