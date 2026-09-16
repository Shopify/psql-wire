package wire

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

func TestLegacyExecuteLimitUnwindsHandler(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprint(parallel), func(t *testing.T) {
			returned := make(chan struct{})
			conn := compatibilityClient(t, func(context.Context, string) (PreparedStatements, error) {
				return Prepared(NewStatement(func(_ context.Context, w DataWriter, _ []Parameter) error {
					defer close(returned)
					for i := 0; i < 2; i++ {
						if err := w.Row([]any{"row"}); err != nil {
							return err
						}
					}
					return w.Complete("SELECT 2")
				}, WithColumns(Columns{{Name: "value", Oid: 25}}))), nil
			}, PortalSuspension(false), ParallelPipeline(ParallelPipelineConfig{Enabled: parallel}))
			f := conn.Frontend()
			f.Send(&pgproto3.Parse{Name: "s", Query: "limited"})
			f.Send(&pgproto3.Bind{DestinationPortal: "p", PreparedStatement: "s"})
			f.Send(&pgproto3.Execute{Portal: "p", MaxRows: 1})
			f.Send(&pgproto3.Sync{})
			require.NoError(t, f.Flush())
			result := readCompatibilityBatch(t, f)
			require.Equal(t, []string{"54000"}, result.errors)
			require.Empty(t, result.tags)
			if parallel {
				require.Empty(t, result.rows)
			} else {
				require.Equal(t, []string{"row"}, result.rows)
			}
			select {
			case <-returned:
			default:
				t.Fatal("row-limited handler still retains resources")
			}
		})
	}
}

func TestCompatibilityBindDescribeErrorRecovery(t *testing.T) {
	for _, message := range []pgproto3.FrontendMessage{
		&pgproto3.Bind{DestinationPortal: "p", PreparedStatement: "missing"},
		&pgproto3.Bind{DestinationPortal: "p", PreparedStatement: "cache-error"},
		&pgproto3.Describe{ObjectType: 'S', Name: "cache-error"},
		&pgproto3.Bind{DestinationPortal: "bind-error", PreparedStatement: "existing"},
		&pgproto3.Describe{ObjectType: 'P', Name: "cache-error"},
	} {
		t.Run(fmt.Sprintf("%T/%v", message, message), func(t *testing.T) {
			conn := compatibilityClient(t, func(context.Context, string) (PreparedStatements, error) {
				return Prepared(NewStatement(func(context.Context, DataWriter, []Parameter) error { return nil })), nil
			}, Statements(func() StatementCache { return &failingLookupCache{DefaultStatementCacheFn()} }),
				Portals(func() PortalCache { return &failingPortalCache{DefaultPortalCacheFn()} }))
			f := conn.Frontend()
			f.Send(&pgproto3.Parse{Name: "existing", Query: "setup"})
			f.Send(&pgproto3.Sync{})
			require.NoError(t, f.Flush())
			setup := readCompatibilityBatch(t, f)
			require.Empty(t, setup.errors)
			require.Equal(t, 1, setup.parses)
			f.Send(message)
			f.Send(&pgproto3.Parse{Name: "discarded", Query: "ignored after error"})
			f.Send(&pgproto3.Sync{})
			require.NoError(t, f.Flush())
			failed := readCompatibilityBatch(t, f)
			require.Len(t, failed.errors, 1)
			require.Zero(t, failed.parses)
			f.Send(&pgproto3.Parse{Name: "kept", Query: "works after Sync"})
			f.Send(&pgproto3.Sync{})
			require.NoError(t, f.Flush())
			reused := readCompatibilityBatch(t, f)
			require.Empty(t, reused.errors)
			require.Equal(t, 1, reused.parses, "also rejects an extra ReadyForQuery left by the failed batch")
		})
	}
}

func TestParallelResponseOrderUsesPortalIdentity(t *testing.T) {
	secondFinished := make(chan struct{})
	abort := make(chan struct{})
	defer close(abort) // release A before server cleanup if B never starts
	conn := compatibilityClient(t, func(_ context.Context, query string) (PreparedStatements, error) {
		return Prepared(NewStatement(func(_ context.Context, w DataWriter, _ []Parameter) error {
			if query == "A" {
				select {
				case <-secondFinished:
				case <-abort:
					return context.Canceled
				}
			} else {
				defer close(secondFinished)
			}
			if err := w.Row([]any{query}); err != nil {
				return err
			}
			return w.Complete("SELECT 1")
		}, WithColumns(Columns{{Name: "identity", Oid: 25}}))), nil
	}, ParallelPipeline(ParallelPipelineConfig{Enabled: true}))
	f := conn.Frontend()
	for _, name := range []string{"A", "B"} {
		f.Send(&pgproto3.Parse{Name: name, Query: name})
		f.Send(&pgproto3.Bind{DestinationPortal: name, PreparedStatement: name})
		f.Send(&pgproto3.Execute{Portal: name})
	}
	f.Send(&pgproto3.Sync{})
	require.NoError(t, f.Flush())
	result := readCompatibilityBatch(t, f)
	require.Empty(t, result.errors)
	require.Equal(t, []string{"A", "B"}, result.rows, "B finished first but must be replayed second")
	require.Equal(t, []string{"SELECT 1", "SELECT 1"}, result.tags)
}

type failingLookupCache struct{ StatementCache }

func (c *failingLookupCache) Get(ctx context.Context, name string) (*Statement, error) {
	if name == "cache-error" {
		return nil, errors.New("cache lookup failed")
	}
	return c.StatementCache.Get(ctx, name)
}

type failingPortalCache struct{ PortalCache }

func (c *failingPortalCache) Bind(ctx context.Context, name string, stmt *Statement, params []Parameter, formats []FormatCode) error {
	if name == "bind-error" {
		return errors.New("portal bind failed")
	}
	return c.PortalCache.Bind(ctx, name, stmt, params, formats)
}

func (c *failingPortalCache) Get(ctx context.Context, name string) (*Portal, error) {
	if name == "cache-error" {
		return nil, errors.New("portal lookup failed")
	}
	return c.PortalCache.Get(ctx, name)
}

func compatibilityClient(t *testing.T, handler ParseFn, options ...OptionFn) *pgconn.PgConn {
	t.Helper()
	server, err := NewServer(handler, append(options, Logger(slogt.New(t)))...)
	require.NoError(t, err)
	addr := TListenAndServe(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://%s/test?sslmode=disable", addr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	require.NoError(t, conn.Conn().SetDeadline(time.Now().Add(5*time.Second)))
	return conn
}

type compatibilityBatch struct {
	rows, tags, errors []string
	parses             int
}

func readCompatibilityBatch(t *testing.T, f *pgproto3.Frontend) compatibilityBatch {
	t.Helper()
	var result compatibilityBatch
	for {
		msg, err := f.Receive()
		require.NoError(t, err)
		switch m := msg.(type) {
		case *pgproto3.DataRow:
			require.Len(t, m.Values, 1)
			result.rows = append(result.rows, string(m.Values[0]))
		case *pgproto3.CommandComplete:
			result.tags = append(result.tags, string(m.CommandTag))
		case *pgproto3.ErrorResponse:
			result.errors = append(result.errors, m.Code)
		case *pgproto3.ParseComplete:
			result.parses++
		case *pgproto3.BindComplete:
		case *pgproto3.ReadyForQuery:
			return result
		default:
			t.Fatalf("unexpected message %T", msg)
		}
	}
}
