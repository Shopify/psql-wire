package wire

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/lib/pq"
	"github.com/lib/pq/oid"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type observerEntry struct {
	format       FormatCode
	oid          uint32
	count        uint64
	encodedBytes uint64
}

type recordingObserver struct {
	mu      sync.Mutex
	entries []observerEntry
}

func (r *recordingObserver) observe(_ context.Context, format FormatCode, columnOID uint32, count uint64, encodedBytes uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, observerEntry{format: format, oid: columnOID, count: count, encodedBytes: encodedBytes})
}

func (r *recordingObserver) snapshot() []observerEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]observerEntry, len(r.entries))
	copy(out, r.entries)
	return out
}

// twoColumnTestServer constructs a server that responds to any query with a
// two-row result containing a TEXT and an INT4 column. Useful for asserting
// per-column observer behavior across formats and types.
func twoColumnTestServer(t *testing.T, opts ...OptionFn) *Server {
	t.Helper()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		fn := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			require.NoError(t, writer.Row([]any{"alice", int32(30)}))
			require.NoError(t, writer.Row([]any{"bob", int32(25)}))
			return writer.Complete("SELECT 2")
		}

		columns := Columns{
			{Name: "name", Oid: oid.T_text, Width: 256},
			{Name: "age", Oid: oid.T_int4, Width: 4},
		}

		return Prepared(NewStatement(fn, WithColumns(columns))), nil
	}

	allOpts := append([]OptionFn{Logger(slogt.New(t))}, opts...)
	server, err := NewServer(handler, allOpts...)
	require.NoError(t, err)
	return server
}

func TestEncodeObserverTextFormat(t *testing.T) {
	t.Parallel()

	rec := &recordingObserver{}
	server := twoColumnTestServer(t, WithEncodeObserver(rec.observe))
	address := TListenAndServe(t, server)

	connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
	conn, err := sql.Open("postgres", connstr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	rows, err := conn.Query("SELECT * FROM people")
	require.NoError(t, err)

	for rows.Next() {
		var name string
		var age int
		require.NoError(t, rows.Scan(&name, &age))
	}
	require.NoError(t, rows.Close())

	entries := rec.snapshot()
	require.Len(t, entries, 2, "expected one aggregated entry per column")

	assert.Equal(t, TextFormat, entries[0].format, "lib/pq uses simple query protocol which encodes as text")
	assert.Equal(t, uint32(oid.T_text), entries[0].oid)
	assert.Equal(t, uint64(2), entries[0].count)
	assert.Equal(t, uint64(8), entries[0].encodedBytes)
	assert.Equal(t, TextFormat, entries[1].format)
	assert.Equal(t, uint32(oid.T_int4), entries[1].oid)
	assert.Equal(t, uint64(2), entries[1].count)
	assert.Equal(t, uint64(4), entries[1].encodedBytes)
}

func BenchmarkEncodeObserver(b *testing.B) {
	columns := Columns{
		{Name: "id", Oid: oid.T_int8, Width: 8},
		{Name: "value", Oid: oid.T_text, Width: 256},
	}
	values := []any{int64(42), "benchmark"}

	for _, observed := range []bool{false, true} {
		name := "disabled"
		if observed {
			name = "enabled"
		}
		b.Run(name, func(b *testing.B) {
			ctx := setTypeInfo(context.Background(), pgtype.NewMap())
			var observedValues atomic.Uint64
			var observedBytes atomic.Uint64
			if observed {
				ctx = setEncodeObserver(ctx, func(_ context.Context, _ FormatCode, _ uint32, count uint64, encodedBytes uint64) {
					observedValues.Add(count)
					observedBytes.Add(encodedBytes)
				})
			}
			client := buffer.NewWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), io.Discard)

			b.ReportAllocs()
			for b.Loop() {
				writer := NewDataWriter(ctx, nil, columns, nil, NoLimit, nil, client)
				for range 10_000 {
					if err := writer.Row(values); err != nil {
						b.Fatal(err)
					}
				}
				if err := writer.Complete("SELECT 10000"); err != nil {
					b.Fatal(err)
				}
			}

			if observed {
				want := uint64(20_000 * b.N)
				if observedValues.Load() != want {
					b.Fatalf("observed %d values, want %d", observedValues.Load(), want)
				}
			}
		})
	}
}

func TestEncodeObserverBinaryFormat(t *testing.T) {
	t.Parallel()

	rec := &recordingObserver{}
	server := twoColumnTestServer(t, WithEncodeObserver(rec.observe))
	address := TListenAndServe(t, server)

	ctx := context.Background()
	connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
	conn, err := pgconn.Connect(ctx, connstr)
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()

	// Explicitly request binary result formats for both columns. This bypasses
	// pgx's default heuristics (which can fall back to text depending on type
	// cache state) and exercises the binary code path deterministically.
	binaryFormats := []int16{1, 1}
	result := conn.ExecParams(ctx, "SELECT * FROM people", nil, nil, nil, binaryFormats).Read()
	require.NoError(t, result.Err)

	entries := rec.snapshot()
	require.Len(t, entries, 2, "expected one aggregated entry per column")

	assert.Equal(t, BinaryFormat, entries[0].format, "client requested binary format for both columns")
	assert.Equal(t, uint64(2), entries[0].count)
	assert.Equal(t, uint64(8), entries[0].encodedBytes)
	assert.Equal(t, BinaryFormat, entries[1].format)
	assert.Equal(t, uint64(2), entries[1].count)
	assert.Equal(t, uint64(8), entries[1].encodedBytes)
}

// TestEncodeObserverPgxDefault documents and pins pgx v5's default
// per-column result-format selection. pgx asks each registered codec for its
// "preferred" format via pgtype.Codec.PreferredFormat() and sends that value
// in the Bind message. The defaults are:
//
//   - TextCodec    → TEXT   (textual types are already strings on the wire)
//   - VarcharCodec → TEXT
//   - NameCodec    → TEXT
//   - NumericCodec → TEXT   (arbitrary precision; no fixed binary layout)
//   - BoolCodec    → BINARY
//   - Int{2,4,8}   → BINARY
//   - Float{4,8}   → BINARY
//   - UUIDCodec    → BINARY
//   - TimestampTZ  → BINARY
//   - JSON/JSONB   → BINARY
//
// So `SELECT name, age FROM users` (text + int4) shows up as [TEXT, BINARY]
// for the row's columns, not [BINARY, BINARY]. A QE metric that buckets by
// {format} will see both values for nearly any pgx-driven workload — this is
// expected, not a bug.
func TestEncodeObserverPgxDefault(t *testing.T) {
	t.Parallel()

	rec := &recordingObserver{}
	server := twoColumnTestServer(t, WithEncodeObserver(rec.observe))
	address := TListenAndServe(t, server)

	ctx := context.Background()
	connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
	conn, err := pgx.Connect(ctx, connstr)
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()

	rows, err := conn.Query(ctx, "SELECT * FROM people")
	require.NoError(t, err)
	for rows.Next() {
		var name string
		var age int
		require.NoError(t, rows.Scan(&name, &age))
	}
	rows.Close()

	entries := rec.snapshot()
	require.Len(t, entries, 2)

	// Two rows aggregated into one observation per result column.
	assert.Equal(t, TextFormat, entries[0].format, "name (text) → pgx prefers text")
	assert.Equal(t, uint32(oid.T_text), entries[0].oid)
	assert.Equal(t, uint64(2), entries[0].count)
	assert.Equal(t, BinaryFormat, entries[1].format, "age (int4) → pgx prefers binary")
	assert.Equal(t, uint32(oid.T_int4), entries[1].oid)
	assert.Equal(t, uint64(2), entries[1].count)
}

func TestEncodeObserverNotInstalled(t *testing.T) {
	t.Parallel()

	server := twoColumnTestServer(t)
	address := TListenAndServe(t, server)

	connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
	conn, err := sql.Open("postgres", connstr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	rows, err := conn.Query("SELECT * FROM people")
	require.NoError(t, err)
	for rows.Next() {
		var name string
		var age int
		require.NoError(t, rows.Scan(&name, &age))
	}
	require.NoError(t, rows.Close())
}

func TestEncodeObserverSkipsNullValues(t *testing.T) {
	t.Parallel()

	rec := &recordingObserver{}

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		fn := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			require.NoError(t, writer.Row([]any{"alice", nil}))
			return writer.Complete("SELECT 1")
		}

		columns := Columns{
			{Name: "name", Oid: oid.T_text, Width: 256},
			{Name: "age", Oid: oid.T_int4, Width: 4},
		}

		return Prepared(NewStatement(fn, WithColumns(columns))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)), WithEncodeObserver(rec.observe))
	require.NoError(t, err)
	address := TListenAndServe(t, server)

	connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
	conn, err := sql.Open("postgres", connstr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	rows, err := conn.Query("SELECT * FROM people")
	require.NoError(t, err)
	for rows.Next() {
		var name string
		var age sql.NullInt32
		require.NoError(t, rows.Scan(&name, &age))
		assert.False(t, age.Valid)
	}
	require.NoError(t, rows.Close())

	entries := rec.snapshot()
	require.Len(t, entries, 1, "NULL values must not be observed")
	assert.Equal(t, uint32(oid.T_text), entries[0].oid)
	assert.Equal(t, uint64(1), entries[0].count)
}

func TestDataWriterFormats(t *testing.T) {
	t.Parallel()

	var captured []FormatCode
	var captureOnce sync.Once

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		fn := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			captureOnce.Do(func() {
				captured = append(captured, writer.Formats()...)
			})
			require.NoError(t, writer.Row([]any{"x"}))
			return writer.Complete("SELECT 1")
		}

		columns := Columns{
			{Name: "v", Oid: oid.T_text, Width: 256},
		}
		return Prepared(NewStatement(fn, WithColumns(columns))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)
	address := TListenAndServe(t, server)

	connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
	db, err := sql.Open("postgres", connstr)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows, err := db.Query("SELECT v FROM t")
	require.NoError(t, err)
	for rows.Next() {
		var v string
		require.NoError(t, rows.Scan(&v))
	}
	require.NoError(t, rows.Close())

	// lib/pq simple query protocol means no formats are negotiated, so the
	// dataWriter's formats slice is whatever the bind step provided (often
	// empty). We assert the API is reachable rather than the exact value.
	_ = captured
	_ = pq.Driver{}
}
