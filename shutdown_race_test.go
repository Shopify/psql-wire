package wire

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/neilotoole/slogt"
)

// TestRaceServeAndClose tests the race condition between Serve and Close
func TestRaceServeAndClose(t *testing.T) {
	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	// Run this test multiple times to increase chance of hitting race condition
	for i := 0; i < 100; i++ {
		server, err := NewServer(handler, Logger(slogt.New(t)))
		if err != nil {
			t.Fatal(err)
		}

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		var serveErr error

		// Start Serve and Close concurrently to create race condition
		wg.Add(2)

		// Goroutine 1: Start serving
		go func() {
			defer wg.Done()
			serveErr = server.Serve(listener)
		}()

		// Goroutine 2: Close immediately (before Serve has time to fully start)
		go func() {
			defer wg.Done()
			// Small delay to let Serve start but not complete setup
			time.Sleep(time.Microsecond * 10)
			_ = server.Close()
		}()

		wg.Wait()

		// The test passes if no deadlock occurs and Close() returns
		if serveErr != nil && serveErr.Error() != "use of closed network connection" {
			t.Logf("Serve returned error (expected): %v", serveErr)
		}
	}
}

// TestRaceCloseBeforeServe tests calling Close before Serve has started
func TestRaceCloseBeforeServe(t *testing.T) {
	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	// Close before Serve is called
	err = server.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Now try to serve - should return quickly without hanging
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	done := make(chan error, 1)
	go func() {
		done <- server.Serve(listener)
	}()

	select {
	case err := <-done:
		// Should complete without hanging
		if err != nil {
			t.Logf("Serve returned error (expected): %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Serve did not return within 1 second - possible deadlock")
	}
}

// TestRaceMultipleConcurrentCloses tests multiple concurrent Close() calls
func TestRaceMultipleConcurrentCloses(t *testing.T) {
	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	var serverWg sync.WaitGroup
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		_ = server.Serve(listener)
	}()

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	// Call Close() from multiple goroutines concurrently
	var closeWg sync.WaitGroup
	const numClosers = 10
	closeWg.Add(numClosers)

	for i := 0; i < numClosers; i++ {
		go func() {
			defer closeWg.Done()
			err := server.Close()
			if err != nil {
				t.Errorf("Close() returned error: %v", err)
			}
		}()
	}

	closeWg.Wait()
	serverWg.Wait()

	// All Close() calls should have completed without error
}

// TestRaceNewConnectionDuringShutdown tests the race condition where
// new connections arrive just as shutdown begins
func TestRaceNewConnectionDuringShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race test in short mode")
	}

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			// Add small delay to simulate work
			time.Sleep(10 * time.Millisecond)
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	// Run multiple iterations to increase chances of hitting the race
	for i := 0; i < 50; i++ {
		server, err := NewServer(handler, Logger(slogt.New(t)))
		if err != nil {
			t.Fatal(err)
		}

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		var serverWg sync.WaitGroup
		serverWg.Add(1)
		go func() {
			defer serverWg.Done()
			_ = server.Serve(listener)
		}()

		// Give server time to start
		time.Sleep(10 * time.Millisecond)

		var raceWg sync.WaitGroup
		raceWg.Add(2)

		// Goroutine 1: Try to connect just as shutdown begins
		go func() {
			defer raceWg.Done()
			conn, err := net.Dial("tcp", listener.Addr().String())
			if err == nil {
				// Connection succeeded, close it
				_ = conn.Close()
			}
			// If connection failed, that's also fine - shutdown happened first
		}()

		// Goroutine 2: Start shutdown at the same time
		go func() {
			defer raceWg.Done()
			_ = server.Close()
		}()

		raceWg.Wait()
		
		// Wait for server to finish with timeout
		done := make(chan struct{})
		go func() {
			serverWg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Good, server finished
		case <-time.After(2 * time.Second):
			t.Errorf("Server did not shutdown within timeout on iteration %d", i)
			return
		}
	}
}

// TestRaceShutdownTimeout tests that shutdown respects the timeout
func TestRaceShutdownTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping timeout test in short mode")
	}

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			// Simulate a long-running query that exceeds shutdown timeout
			select {
			case <-time.After(2 * time.Second):
				return writer.Complete("OK")
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		return Prepared(statement), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	var serverWg sync.WaitGroup
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		_ = server.Serve(listener)
	}()

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	// Simulate a long-running connection by adding to WaitGroup manually
	// This simulates the scenario where a connection is active during shutdown
	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		// Simulate long-running work that exceeds shutdown timeout
		time.Sleep(2 * time.Second)
	}()

	// Give the simulated connection time to start
	time.Sleep(50 * time.Millisecond)

	// Now test shutdown timing
	start := time.Now()
	err = server.Close()
	duration := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}

	// Should respect the 1-second timeout
	if duration < 900*time.Millisecond || duration > 1200*time.Millisecond {
		t.Errorf("Shutdown took %v, expected ~1 second", duration)
	}

	serverWg.Wait()
	t.Logf("Shutdown completed in %v", duration)
}

// TestRaceWaitGroupOperations tests for race conditions in WaitGroup usage
func TestRaceWaitGroupOperations(t *testing.T) {
	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	// Test rapid start/stop cycles
	for i := 0; i < 100; i++ {
		server, err := NewServer(handler, Logger(slogt.New(t)))
		if err != nil {
			t.Fatal(err)
		}

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		// Start and immediately stop server
		done := make(chan error, 1)
		go func() {
			done <- server.Serve(listener)
		}()

		// Close almost immediately
		time.Sleep(time.Microsecond)
		err = server.Close()
		if err != nil {
			t.Fatal(err)
		}

		// Wait for serve to complete
		select {
		case <-done:
			// Good
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Server did not stop quickly on iteration %d", i)
			return
		}
	}
}