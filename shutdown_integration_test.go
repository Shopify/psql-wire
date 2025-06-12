package wire

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lib/pq/oid"
	"github.com/neilotoole/slogt"
)

// TestShutdownIntegration tests the shutdown functionality with a real connection
func TestShutdownIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		columns := Columns{
			{
				Table: 0,
				Name:  "number",
				Oid:   oid.T_int4,
				Width: 4,
			},
		}
		
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			err := writer.Row([]any{1})
			if err != nil {
				return err
			}
			return writer.Complete("SELECT 1")
		}, WithColumns(columns))
		return Prepared(statement), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	// Start server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = server.Serve(listener)
	}()

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	// Connect and execute a quick query
	connStr := fmt.Sprintf("postgres://127.0.0.1:%d?sslmode=disable", listener.Addr().(*net.TCPAddr).Port)
	
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		t.Fatal(err)
	}

	var result int
	err = conn.QueryRow(context.Background(), "SELECT 1").Scan(&result)
	if err != nil {
		t.Fatal(err)
	}

	if result != 1 {
		t.Errorf("Expected 1, got %d", result)
	}

	_ = conn.Close(context.Background())

	// Give connection time to clean up
	time.Sleep(50 * time.Millisecond)

	// Test shutdown
	start := time.Now()
	err = server.Close()
	duration := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}

	// Wait for server to finish
	wg.Wait()

	// Should be fast since connection is closed
	if duration > 200*time.Millisecond {
		t.Errorf("Expected fast shutdown, but took %v", duration)
	}

	t.Logf("Shutdown completed in %v", duration)
}

