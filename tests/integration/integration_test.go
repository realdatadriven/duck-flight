package integration

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestDuckFlightWithDuckDBCLI(t *testing.T) {
	// Skip if dependencies are not available
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go binary not found in PATH")
	}
	if _, err := exec.LookPath("duckdb"); err != nil {
		t.Skip("duckdb binary not found in PATH; skipping integration test")
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
	addr := "127.0.0.1:8088"
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
	sql := `INSTALL airport; LOAD airport; ATTACH 'test' (TYPE airport, LOCATION '127.0.0.1:8088'); SELECT * FROM test.orders LIMIT 10;`

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
