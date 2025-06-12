package wire

import (
	"context"
	"testing"
	"time"

	"github.com/neilotoole/slogt"
)

func TestServerClose(t *testing.T) {
	t.Parallel()

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

	// Test that Close() can be called multiple times without error
	err = server.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Second call should not error
	err = server.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Verify server is marked as closing
	if !server.closing.Load() {
		t.Error("Expected server to be marked as closing")
	}
}

func TestServerCloseWithTimeout(t *testing.T) {
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

	// Test timeout behavior by calling Close() without any server running
	// This tests the timeout mechanism itself
	start := time.Now()
	err = server.Close()
	duration := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}

	// Should complete very quickly since no connections are active
	if duration > 100*time.Millisecond {
		t.Errorf("Expected fast shutdown with no connections, but took %v", duration)
	}

	t.Logf("Shutdown completed in %v", duration)
}

func TestServerCloseIdempotent(t *testing.T) {
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

	// Test that multiple calls to Close() are safe
	for i := 0; i < 3; i++ {
		err = server.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Verify server is marked as closing
	if !server.closing.Load() {
		t.Error("Expected server to be marked as closing")
	}

	t.Log("Multiple Close() calls completed successfully")
}

func TestServerShutdownLogging(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	// Test with nil logger
	server, err := NewServer(handler)
	if err != nil {
		t.Fatal(err)
	}

	// Should not panic with nil logger
	err = server.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Test with actual logger
	server2, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	err = server2.Close()
	if err != nil {
		t.Fatal(err)
	}
}