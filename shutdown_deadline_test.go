package wire

import (
	"context"
	"testing"
	"time"

	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

func TestRepeatedShutdownHonorsEachCallersDeadline(t *testing.T) {
	server, err := NewServer(func(context.Context, string) (PreparedStatements, error) { return nil, nil }, Logger(slogt.New(t)))
	require.NoError(t, err)
	// An active command whose handler ignores cancellation must not make a
	// second caller wait forever after the first caller's budget expires.
	server.wg.Add(1)
	t.Cleanup(server.wg.Done)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 2; i++ {
		done := make(chan error, 1)
		go func() { done <- server.Shutdown(ctx) }()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("Shutdown ignored caller cancellation")
		}
	}
}
