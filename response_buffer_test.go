package wire

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

// countingConn records every server Write. It counts before it writes, so a
// client that has received a response always sees the writes that carried it.
type countingConn struct {
	net.Conn
	countOnly bool // benchmarks: count without keeping sizes and bytes
	mu        sync.Mutex
	writes    int
	bytes     int
	sizes     []int
	out       bytes.Buffer
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes++
	c.bytes += len(p)
	if !c.countOnly {
		c.sizes = append(c.sizes, len(p))
		c.out.Write(p)
	}
	c.mu.Unlock()
	return c.Conn.Write(p)
}

// take returns the write sizes and bytes since the previous call.
func (c *countingConn) take() ([]int, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	sizes, out := c.sizes, bytes.Clone(c.out.Bytes())
	c.writes, c.bytes, c.sizes = 0, 0, nil
	c.out.Reset()
	return sizes, out
}

// counts returns the number of writes and bytes since the previous call.
func (c *countingConn) counts() (writes, bytes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	writes, bytes = c.writes, c.bytes
	c.writes, c.bytes = 0, 0
	return writes, bytes
}

type countingListener struct {
	net.Listener
	countOnly bool
	conns     chan *countingConn
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	counted := &countingConn{Conn: conn, countOnly: l.countOnly}
	l.conns <- counted
	return counted, nil
}

type bufferingMode struct {
	name     string
	buffer   int
	parallel bool
}

var bufferingModes = []bufferingMode{
	{"unbuffered", 0, false},
	{"unbuffered/parallel", 0, true},
	{"buffered", 8 << 10, false},
	{"buffered/parallel", 8 << 10, true},
}

const largeResultRows = 400

// bufferingHandler serves the statements used by the response buffering tests.
func bufferingHandler(_ context.Context, query Query) (PreparedStatements, error) {
	columns := WithColumns(Columns{{Name: "value", Oid: pgtype.TextOID}})
	switch query.Query {
	case "small":
		return Prepared(NewStatement(func(_ context.Context, w DataWriter, _ []Parameter) error {
			if err := w.Row([]any{"value"}); err != nil {
				return err
			}
			return w.Complete("SELECT 1")
		}, columns)), nil
	case "large":
		return Prepared(NewStatement(func(_ context.Context, w DataWriter, _ []Parameter) error {
			rows := make([][]any, largeResultRows)
			for i := range rows {
				rows[i] = []any{fmt.Sprintf("%04d%s", i, strings.Repeat("x", 200))}
			}
			if err := WriteRows(w, rows); err != nil {
				return err
			}
			return w.Complete(fmt.Sprintf("SELECT %d", len(rows)))
		}, columns)), nil
	case "fatal":
		return Prepared(NewStatement(func(context.Context, DataWriter, []Parameter) error {
			err := psqlerr.WithCode(errors.New("terminating connection"), codes.AdminShutdown)
			return psqlerr.WithSeverity(err, psqlerr.LevelFatal)
		}, columns)), nil
	case "copy":
		return Prepared(NewStatement(func(ctx context.Context, w DataWriter, _ []Parameter) error {
			reader, err := w.CopyIn(TextFormat)
			if err != nil {
				return err
			}
			chunks := 0
			for {
				err := reader.Read(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return err
				}
				chunks++
			}
			return w.Complete(fmt.Sprintf("COPY %d", chunks))
		}, columns)), nil
	}
	return nil, fmt.Errorf("unknown query %q", query.Query)
}

// bufferingClient connects to a server in the given mode and returns the
// client together with the server side of the connection.
func bufferingClient(tb testing.TB, mode bufferingMode, logger *slog.Logger) (*pgconn.PgConn, *countingConn) {
	tb.Helper()
	server, err := NewServer(bufferingHandler, Logger(logger),
		WithResponseBuffering(mode.buffer),
		ParallelPipeline(ParallelPipelineConfig{Enabled: mode.parallel}))
	require.NoError(tb, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	_, bench := tb.(*testing.B)
	counting := &countingListener{Listener: listener, countOnly: bench, conns: make(chan *countingConn, 1)}
	tb.Cleanup(func() {
		require.NoError(tb, server.Close())
		server.Wait()
	})
	go server.Serve(counting) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://%s/test?sslmode=disable", listener.Addr()))
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = conn.Close(context.Background()) })
	require.NoError(tb, conn.Conn().SetDeadline(time.Now().Add(5*time.Second)))

	counted := <-counting.conns
	counted.take() // startup is not part of any cycle
	return conn, counted
}

// receive reads messages until stop returns true and returns their types.
func receive(t *testing.T, f *pgproto3.Frontend, stop func(pgproto3.BackendMessage) bool) []string {
	t.Helper()
	var types []string
	for {
		msg, err := f.Receive()
		require.NoError(t, err)
		types = append(types, strings.TrimPrefix(fmt.Sprintf("%T", msg), "*pgproto3."))
		if stop(msg) {
			return types
		}
	}
}

func isReady(msg pgproto3.BackendMessage) bool {
	_, ok := msg.(*pgproto3.ReadyForQuery)
	return ok
}

func isError(msg pgproto3.BackendMessage) bool {
	_, ok := msg.(*pgproto3.ErrorResponse)
	return ok
}

func TestResponseBufferingCycles(t *testing.T) {
	want := map[string][3]int{ // extended, rebind, simple
		"unbuffered":          {6, 5, 4},
		"unbuffered/parallel": {5, 4, 4},
		"buffered":            {1, 1, 1},
		"buffered/parallel":   {1, 1, 1},
	}
	var baseline [3][]byte
	for _, mode := range bufferingModes {
		t.Run(mode.name, func(t *testing.T) {
			conn, counted := bufferingClient(t, mode, slogt.New(t))
			f := conn.Frontend()

			f.Send(&pgproto3.Parse{Name: "s", Query: "small"})
			f.Send(&pgproto3.Bind{PreparedStatement: "s"})
			f.Send(&pgproto3.Describe{ObjectType: 'P'})
			f.Send(&pgproto3.Execute{})
			f.Send(&pgproto3.Sync{})
			require.NoError(t, f.Flush())
			require.Equal(t, []string{"ParseComplete", "BindComplete", "RowDescription", "DataRow", "CommandComplete", "ReadyForQuery"}, receive(t, f, isReady))
			extended, extendedBytes := counted.take()

			f.Send(&pgproto3.Bind{PreparedStatement: "s"})
			f.Send(&pgproto3.Describe{ObjectType: 'P'})
			f.Send(&pgproto3.Execute{})
			f.Send(&pgproto3.Sync{})
			require.NoError(t, f.Flush())
			require.Equal(t, []string{"BindComplete", "RowDescription", "DataRow", "CommandComplete", "ReadyForQuery"}, receive(t, f, isReady))
			rebind, rebindBytes := counted.take()

			f.Send(&pgproto3.Query{String: "small"})
			require.NoError(t, f.Flush())
			require.Equal(t, []string{"RowDescription", "DataRow", "CommandComplete", "ReadyForQuery"}, receive(t, f, isReady))
			simple, simpleBytes := counted.take()

			t.Logf("writes per cycle: extended=%d rebind=%d simple=%d", len(extended), len(rebind), len(simple))
			require.Equal(t, want[mode.name], [3]int{len(extended), len(rebind), len(simple)})

			cycles := [3][]byte{extendedBytes, rebindBytes, simpleBytes}
			if baseline[0] == nil {
				baseline = cycles
			}
			require.Equal(t, baseline, cycles, "buffering changes write boundaries only")
		})
	}
}

func TestResponseBufferingFlush(t *testing.T) {
	for _, mode := range bufferingModes {
		t.Run(mode.name, func(t *testing.T) {
			conn, counted := bufferingClient(t, mode, slogt.New(t))
			f := conn.Frontend()

			// Without Sync, the client waits for these only because it sent Flush.
			f.Send(&pgproto3.Parse{Name: "a", Query: "small"})
			f.Send(&pgproto3.Parse{Name: "b", Query: "small"})
			f.Send(&pgproto3.Flush{})
			require.NoError(t, f.Flush())
			parsed := 0
			require.Equal(t, []string{"ParseComplete", "ParseComplete"}, receive(t, f, func(msg pgproto3.BackendMessage) bool {
				if _, ok := msg.(*pgproto3.ParseComplete); ok {
					parsed++
				}
				return parsed == 2
			}))
			writes, _ := counted.take()
			if mode.buffer > 0 {
				require.Len(t, writes, 1, "both ParseCompletes are held until Flush")
			} else {
				require.Len(t, writes, 2)
			}

			f.Send(&pgproto3.Sync{})
			require.NoError(t, f.Flush())
			require.Equal(t, []string{"ReadyForQuery"}, receive(t, f, isReady))
			writes, _ = counted.take()
			require.Len(t, writes, 1)
		})
	}
}

func TestResponseBufferingErrorFlushedBeforeSync(t *testing.T) {
	for _, mode := range bufferingModes {
		t.Run(mode.name, func(t *testing.T) {
			conn, counted := bufferingClient(t, mode, slogt.New(t))
			f := conn.Frontend()

			// No Sync yet: the ErrorResponse must arrive on its own.
			f.Send(&pgproto3.Parse{Name: "s", Query: "small"})
			f.Send(&pgproto3.Bind{PreparedStatement: "missing"})
			f.Send(&pgproto3.Execute{})
			require.NoError(t, f.Flush())
			require.Equal(t, []string{"ParseComplete", "ErrorResponse"}, receive(t, f, isError))
			writes, _ := counted.take()
			if mode.buffer > 0 {
				require.Len(t, writes, 1, "ParseComplete and ErrorResponse share the error flush")
			} else {
				require.Len(t, writes, 2)
			}

			f.Send(&pgproto3.Sync{})
			require.NoError(t, f.Flush())
			require.Equal(t, []string{"ReadyForQuery"}, receive(t, f, isReady))
			writes, _ = counted.take()
			require.Len(t, writes, 1)
		})
	}
}

func TestResponseBufferingLargeResult(t *testing.T) {
	var baseline []byte
	for _, mode := range bufferingModes {
		t.Run(mode.name, func(t *testing.T) {
			conn, counted := bufferingClient(t, mode, slogt.New(t))
			f := conn.Frontend()

			f.Send(&pgproto3.Parse{Name: "s", Query: "large"})
			f.Send(&pgproto3.Bind{PreparedStatement: "s"})
			f.Send(&pgproto3.Describe{ObjectType: 'P'})
			f.Send(&pgproto3.Execute{})
			f.Send(&pgproto3.Sync{})
			require.NoError(t, f.Flush())
			got := receive(t, f, isReady)
			require.Len(t, got, largeResultRows+5)
			writes, out := counted.take()
			t.Logf("writes=%d bytes=%d sizes=%v", len(writes), len(out), writes)

			if baseline == nil {
				baseline = out
			}
			require.Equal(t, baseline, out, "buffering changes write boundaries only")

			switch {
			case mode.buffer > 0 && mode.parallel:
				// Pending frames, the pre-rendered result (written without a
				// second copy), then ReadyForQuery.
				require.Len(t, writes, 3)
				require.Greater(t, writes[1], mode.buffer)
			case mode.buffer > 0:
				require.Greater(t, len(writes), len(out)/mode.buffer-1)
				for _, size := range writes[:len(writes)-1] {
					require.GreaterOrEqual(t, size, mode.buffer)
					require.Less(t, size, mode.buffer+512, "flush at the first frame that reaches the threshold")
				}
			}
		})
	}
}

func TestResponseBufferingFatalFlushedBeforeClose(t *testing.T) {
	for _, mode := range bufferingModes {
		t.Run(mode.name, func(t *testing.T) {
			conn, counted := bufferingClient(t, mode, slogt.New(t))
			f := conn.Frontend()

			f.Send(&pgproto3.Parse{Name: "s", Query: "fatal"})
			f.Send(&pgproto3.Bind{PreparedStatement: "s"})
			f.Send(&pgproto3.Execute{})
			f.Send(&pgproto3.Sync{})
			require.NoError(t, f.Flush())
			require.Equal(t, []string{"ParseComplete", "BindComplete", "ErrorResponse"}, receive(t, f, func(msg pgproto3.BackendMessage) bool {
				if m, ok := msg.(*pgproto3.ErrorResponse); ok {
					require.Equal(t, "FATAL", m.Severity)
					return true
				}
				return false
			}))
			_, err := f.Receive()
			require.Error(t, err, "the server closes the connection after FATAL")

			writes, _ := counted.take()
			if mode.buffer > 0 {
				require.Len(t, writes, 1)
			} else {
				require.Len(t, writes, 3)
			}
		})
	}
}

func TestResponseBufferingFlushedOnProtocolError(t *testing.T) {
	for _, mode := range bufferingModes {
		t.Run(mode.name, func(t *testing.T) {
			conn, counted := bufferingClient(t, mode, slogt.New(t))
			f := conn.Frontend()

			// A Bind without a body fails to decode and the server closes the
			// connection without an ErrorResponse. Frames buffered before it
			// are still delivered, as they are without buffering.
			f.Send(&pgproto3.Parse{Name: "s", Query: "small"})
			require.NoError(t, f.Flush())
			_, err := conn.Conn().Write([]byte{'B', 0, 0, 0, 4})
			require.NoError(t, err)
			want := 1
			if mode.parallel {
				want = 0 // queued responses are dropped, with or without buffering
			} else {
				require.Equal(t, []string{"ParseComplete"}, receive(t, f, func(pgproto3.BackendMessage) bool { return true }))
			}
			_, err = f.Receive()
			require.Error(t, err, "the server closes the connection")
			writes, _ := counted.take()
			require.Len(t, writes, want)
		})
	}
}

func TestResponseBufferingCopyIn(t *testing.T) {
	for _, mode := range bufferingModes {
		t.Run(mode.name, func(t *testing.T) {
			conn, counted := bufferingClient(t, mode, slogt.New(t))
			f := conn.Frontend()

			// The client sends CopyData only after it has seen CopyInResponse.
			f.Send(&pgproto3.Query{String: "copy"})
			require.NoError(t, f.Flush())
			require.Equal(t, []string{"RowDescription", "CopyInResponse"}, receive(t, f, func(msg pgproto3.BackendMessage) bool {
				_, ok := msg.(*pgproto3.CopyInResponse)
				return ok
			}))
			writes, _ := counted.take()
			if mode.buffer > 0 {
				require.Len(t, writes, 1)
			} else {
				require.Len(t, writes, 2)
			}

			f.Send(&pgproto3.CopyData{Data: []byte("a\n")})
			f.Send(&pgproto3.CopyData{Data: []byte("b\n")})
			f.Send(&pgproto3.CopyDone{})
			require.NoError(t, f.Flush())
			require.Equal(t, []string{"CommandComplete", "ReadyForQuery"}, receive(t, f, isReady))
			writes, _ = counted.take()
			if mode.buffer > 0 {
				require.Len(t, writes, 1)
			} else {
				require.Len(t, writes, 2)
			}
		})
	}
}

func TestWithResponseBuffering(t *testing.T) {
	for threshold, want := range map[int]int{-1: 0, 0: 0, 8 << 10: 8 << 10} {
		server, err := NewServer(nil, WithResponseBuffering(threshold))
		require.NoError(t, err)
		require.Equal(t, want, server.ResponseBufferBytes)
	}
}

// BenchmarkResponseBuffering measures one extended-protocol cycle over
// loopback TCP. The large cycle returns 400 rows (86 KB); the others return
// one row. writes/op counts server Write calls (one write(2) each on a plain
// TCP connection). B/op and allocs/op cover the whole process; the client
// reads frames without allocating.
func BenchmarkResponseBuffering(b *testing.B) {
	encode := func(msgs ...pgproto3.FrontendMessage) []byte {
		var buf bytes.Buffer
		f := pgproto3.NewFrontend(nil, &buf)
		for _, msg := range msgs {
			f.Send(msg)
		}
		if err := f.Flush(); err != nil {
			b.Fatal(err)
		}
		return buf.Bytes()
	}
	setup := encode(&pgproto3.Parse{Name: "s", Query: "small"}, &pgproto3.Parse{Name: "l", Query: "large"}, &pgproto3.Sync{})
	cycles := []struct {
		name    string
		request []byte
	}{
		{"parse-bind-describe-execute-sync", encode(
			&pgproto3.Parse{Query: "small"}, &pgproto3.Bind{}, &pgproto3.Describe{ObjectType: 'P'},
			&pgproto3.Execute{}, &pgproto3.Sync{})},
		{"bind-describe-execute-sync", encode(
			&pgproto3.Bind{PreparedStatement: "s"}, &pgproto3.Describe{ObjectType: 'P'},
			&pgproto3.Execute{}, &pgproto3.Sync{})},
		{"bind-describe-execute-sync/large", encode(
			&pgproto3.Bind{PreparedStatement: "l"}, &pgproto3.Describe{ObjectType: 'P'},
			&pgproto3.Execute{}, &pgproto3.Sync{})},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, cycle := range cycles {
		for _, mode := range bufferingModes {
			b.Run(cycle.name+"/"+mode.name, func(b *testing.B) {
				conn, counted := bufferingClient(b, mode, logger)
				hijacked, err := conn.Hijack()
				if err != nil {
					b.Fatal(err)
				}
				raw := hijacked.Conn
				b.Cleanup(func() { _ = raw.Close() })
				_ = raw.SetDeadline(time.Time{})
				reader := bufio.NewReaderSize(raw, 64<<10)
				var header [5]byte
				roundTrip := func(request []byte) {
					if _, err := raw.Write(request); err != nil {
						b.Fatal(err)
					}
					for header[0] != 'Z' {
						if _, err := io.ReadFull(reader, header[:]); err != nil {
							b.Fatal(err)
						}
						if header[0] == 'E' {
							b.Fatal("unexpected ErrorResponse")
						}
						size := int(header[1])<<24 | int(header[2])<<16 | int(header[3])<<8 | int(header[4])
						if _, err := reader.Discard(size - 4); err != nil {
							b.Fatal(err)
						}
					}
					header[0] = 0
				}
				roundTrip(setup)
				roundTrip(cycle.request) // warm buffers
				counted.counts()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					roundTrip(cycle.request)
				}
				b.StopTimer()
				writes, wireBytes := counted.counts()
				b.ReportMetric(float64(writes)/float64(b.N), "writes/op")
				b.ReportMetric(float64(wireBytes)/float64(b.N), "wire-B/op")
			})
		}
	}
}
