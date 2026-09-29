package wire

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/stretchr/testify/require"
)

// clientTimeout bounds each client operation, so a missing flush fails the test
// instead of hanging it.
const clientTimeout = 5 * time.Second

type countingListener struct {
	net.Listener
	writes atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: conn, writes: &l.writes}, nil
}

type countingConn struct {
	net.Conn
	writes *atomic.Int64
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(p)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func serveCounting(tb testing.TB, parse ParseFn, options ...OptionFn) (*net.TCPAddr, *countingListener) {
	tb.Helper()
	// pgx's simple protocol requires standard_conforming_strings=on.
	defaults := []OptionFn{Logger(discardLogger()), GlobalParameters(Parameters{"standard_conforming_strings": "on"})}
	server, err := NewServer(parse, append(defaults, options...)...)
	require.NoError(tb, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	counting := &countingListener{Listener: listener}
	tb.Cleanup(func() { _ = server.Close() })
	go server.Serve(counting) //nolint:errcheck
	return listener.Addr().(*net.TCPAddr), counting
}

// rowsParser returns n rows for every query. If gate is not nil, the handler
// waits on it after writing the first row.
func rowsParser(n int, gate <-chan struct{}) ParseFn {
	return func(ctx context.Context, query Query) (PreparedStatements, error) {
		handle := func(ctx context.Context, writer DataWriter, _ []Parameter) error {
			for i := range n {
				if err := writer.Row([]any{int64(i)}); err != nil {
					return err
				}
				if i == 0 && gate != nil {
					select {
					case <-gate:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			return writer.Complete("SELECT " + strconv.Itoa(n))
		}
		return Prepared(NewStatement(handle, WithColumns(Columns{{Name: "n", Oid: pgtype.Int8OID}}))), nil
	}
}

// stallGate returns a channel for a handler to wait on and an idempotent
// release. Register the release with t.Cleanup after starting the server, so it
// runs before server shutdown even when the test fails.
func stallGate() (<-chan struct{}, func()) {
	gate := make(chan struct{})
	var once sync.Once
	return gate, func() { once.Do(func() { close(gate) }) }
}

func connect(tb testing.TB, addr *net.TCPAddr) *pgx.Conn {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), clientTimeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://127.0.0.1:%d/test?sslmode=disable", addr.Port))
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func queryAll(tb testing.TB, conn *pgx.Conn) []int64 {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), clientTimeout)
	defer cancel()
	rows, err := conn.Query(ctx, "SELECT n", pgx.QueryExecModeSimpleProtocol)
	require.NoError(tb, err)
	values, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	require.NoError(tb, err)
	return values
}

func TestWriteBufferCoalescesRows(t *testing.T) {
	const n = 500
	want := make([]int64, n)
	for i := range want {
		want[i] = int64(i)
	}
	for _, tc := range []struct {
		name    string
		options []OptionFn
		ok      func(writes int64) bool
	}{
		{"disabled", nil, func(writes int64) bool { return writes >= n }},
		{"enabled", []OptionFn{WriteBufferSize(8192)}, func(writes int64) bool { return writes <= 4 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, counting := serveCounting(t, rowsParser(n, nil), tc.options...)
			conn := connect(t, addr)
			before := counting.writes.Load()
			require.Equal(t, want, queryAll(t, conn))
			writes := counting.writes.Load() - before
			require.Truef(t, tc.ok(writes), "%d writes", writes)
		})
	}
}

// rawClient sends messages in chosen groupings, so a test controls what the
// server has already received when it responds.
type rawClient struct {
	conn   net.Conn
	client *mock.Client
}

func dialRaw(t *testing.T, addr *net.TCPAddr) *rawClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr.String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := mock.NewClient(t, conn)
	client.Handshake(t)
	client.Authenticate(t)
	client.ReadyForQuery(t, types.ServerIdle)
	return &rawClient{conn: conn, client: client}
}

func (c *rawClient) send(t *testing.T, build func(w *buffer.Writer)) {
	t.Helper()
	var out bytes.Buffer
	build(buffer.NewWriter(discardLogger(), &out))
	_, err := c.conn.Write(out.Bytes())
	require.NoError(t, err)
}

// readUntil reads server messages up to and including want.
func (c *rawClient) readUntil(t *testing.T, want types.ServerMessage) []types.ServerMessage {
	t.Helper()
	require.NoError(t, c.conn.SetReadDeadline(time.Now().Add(clientTimeout)))
	defer c.conn.SetReadDeadline(time.Time{}) //nolint:errcheck
	var seen []types.ServerMessage
	for {
		kind, _, err := c.client.ReadTypedMsg()
		require.NoError(t, err, "waiting for %s after %v", want, seen)
		seen = append(seen, types.ServerMessage(kind))
		if types.ServerMessage(kind) == want {
			return seen
		}
	}
}

func simpleQuery(w *buffer.Writer, query string) {
	w.Start(types.ServerMessage(types.ClientSimpleQuery))
	w.AddString(query)
	w.AddNullTerminate()
	_ = w.End()
}

// execute sends Parse, Bind and Execute for the unnamed statement and portal.
func execute(w *buffer.Writer, query string) {
	w.Start(types.ServerMessage(types.ClientParse))
	w.AddString("")
	w.AddNullTerminate()
	w.AddString(query)
	w.AddNullTerminate()
	w.AddInt16(0)
	_ = w.End()

	w.Start(types.ServerMessage(types.ClientBind))
	w.AddString("")
	w.AddNullTerminate()
	w.AddString("")
	w.AddNullTerminate()
	w.AddInt16(0) // parameter formats
	w.AddInt16(0) // parameters
	w.AddInt16(0) // result formats
	_ = w.End()

	w.Start(types.ServerMessage(types.ClientExecute))
	w.AddString("")
	w.AddNullTerminate()
	w.AddInt32(0)
	_ = w.End()
}

func message(w *buffer.Writer, kind types.ClientMessage) {
	w.Start(types.ServerMessage(kind))
	_ = w.End()
}

// firstThenStalled answers the first query with one row and later queries with
// two rows, stalling on gate after the first of them.
func firstThenStalled(gate <-chan struct{}) ParseFn {
	var queries int
	return func(ctx context.Context, query Query) (PreparedStatements, error) {
		queries++
		if queries == 1 {
			return rowsParser(1, nil)(ctx, query)
		}
		return rowsParser(2, gate)(ctx, query)
	}
}

// TestWriteBufferFlushesReadyForQuery pipelines two queries in one write. The
// second stalls until the client has the first result, which only the flush
// after ReadyForQuery can deliver: the server already has the second query, so
// it does not block on a read. The delay limit is disabled.
func TestWriteBufferFlushesReadyForQuery(t *testing.T) {
	gate, release := stallGate()
	addr, _ := serveCounting(t, firstThenStalled(gate), WriteBufferSize(8192), WriteBufferMaxDelay(-1))
	t.Cleanup(release)
	client := dialRaw(t, addr)

	client.send(t, func(w *buffer.Writer) {
		simpleQuery(w, "SELECT first")
		simpleQuery(w, "SELECT second")
	})
	require.Equal(t, []types.ServerMessage{types.ServerRowDescription, types.ServerDataRow, types.ServerCommandComplete, types.ServerReady}, client.readUntil(t, types.ServerReady))

	release()
	client.readUntil(t, types.ServerReady)
}

// TestWriteBufferFlushesOnClientFlush is the extended-protocol counterpart:
// a Flush separates two executions sent in one write.
func TestWriteBufferFlushesOnClientFlush(t *testing.T) {
	gate, release := stallGate()
	addr, _ := serveCounting(t, firstThenStalled(gate), WriteBufferSize(8192), WriteBufferMaxDelay(-1))
	t.Cleanup(release)
	client := dialRaw(t, addr)

	client.send(t, func(w *buffer.Writer) {
		execute(w, "SELECT first")
		message(w, types.ClientFlush)
		execute(w, "SELECT second")
		message(w, types.ClientSync)
	})
	require.Equal(t, []types.ServerMessage{types.ServerParseComplete, types.ServerBindComplete, types.ServerDataRow, types.ServerCommandComplete}, client.readUntil(t, types.ServerCommandComplete))

	release()
	client.readUntil(t, types.ServerReady)
}

func TestWriteBufferDelayDeliversRowsFromStalledHandler(t *testing.T) {
	gate, release := stallGate()
	addr, _ := serveCounting(t, rowsParser(2, gate), WriteBufferSize(8192))
	t.Cleanup(release)
	client := dialRaw(t, addr)

	client.send(t, func(w *buffer.Writer) { simpleQuery(w, "SELECT n") })
	require.Equal(t, []types.ServerMessage{types.ServerRowDescription, types.ServerDataRow}, client.readUntil(t, types.ServerDataRow))

	release()
	client.readUntil(t, types.ServerReady)
}

// TestWriteBufferAuthentication covers the password exchange, where the server
// waits for the client before any ReadyForQuery, and a rejected password, whose
// error must arrive before the connection closes.
func TestWriteBufferAuthentication(t *testing.T) {
	auth := ClearTextPassword(func(ctx context.Context, _, _, password string) (context.Context, bool, error) {
		return ctx, password == "secret", nil
	})
	addr, _ := serveCounting(t, rowsParser(3, nil), SessionAuthStrategy(auth), WriteBufferSize(8192), WriteBufferMaxDelay(-1))

	connect := func(password string) (*pgx.Conn, error) {
		ctx, cancel := context.WithTimeout(t.Context(), clientTimeout)
		defer cancel()
		return pgx.Connect(ctx, fmt.Sprintf("postgres://test:%s@127.0.0.1:%d/test?sslmode=disable", password, addr.Port))
	}

	conn, err := connect("secret")
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2}, queryAll(t, conn))
	require.NoError(t, conn.Close(t.Context()))

	_, err = connect("wrong")
	require.ErrorContains(t, err, "invalid username/password")
}

func BenchmarkWriteBuffer(b *testing.B) {
	for _, rows := range []int{16, 1024} {
		for _, tc := range []struct {
			name    string
			options []OptionFn
		}{
			{"disabled", nil},
			{"enabled", []OptionFn{WriteBufferSize(8192)}},
		} {
			b.Run(fmt.Sprintf("rows=%d/%s", rows, tc.name), func(b *testing.B) {
				addr, counting := serveCounting(b, rowsParser(rows, nil), tc.options...)
				conn := connect(b, addr)
				before := counting.writes.Load()
				queries := 0
				for b.Loop() {
					require.Len(b, queryAll(b, conn), rows)
					queries++
				}
				b.ReportMetric(float64(counting.writes.Load()-before)/float64(queries), "writes/op")
			})
		}
	}
}
