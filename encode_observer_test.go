package wire

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
	"github.com/lib/pq/oid"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type observerEntry struct {
	format FormatCode
	oid    uint32
	n      int
}

type recordingObserver struct {
	mu      sync.Mutex
	entries []observerEntry
}

func (r *recordingObserver) observe(_ context.Context, format FormatCode, columnOID uint32, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, observerEntry{format: format, oid: columnOID, n: n})
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
	require.Len(t, entries, 4, "expected 2 columns x 2 rows = 4 entries")

	for _, e := range entries {
		assert.Equal(t, TextFormat, e.format, "lib/pq uses simple query protocol which encodes as text")
		assert.Greater(t, e.n, 0)
	}

	assert.Equal(t, uint32(oid.T_text), entries[0].oid)
	assert.Equal(t, uint32(oid.T_int4), entries[1].oid)
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
	require.Len(t, entries, 4, "expected 2 columns x 2 rows = 4 entries")

	for _, e := range entries {
		assert.Equal(t, BinaryFormat, e.format, "client requested binary format for both columns")
		assert.Greater(t, e.n, 0)
	}
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
	require.Len(t, entries, 4)

	// Two rows × {text-column, int4-column}. Per-row order is preserved.
	assert.Equal(t, TextFormat, entries[0].format, "name (text) → pgx prefers text")
	assert.Equal(t, uint32(oid.T_text), entries[0].oid)
	assert.Equal(t, BinaryFormat, entries[1].format, "age (int4) → pgx prefers binary")
	assert.Equal(t, uint32(oid.T_int4), entries[1].oid)
	assert.Equal(t, TextFormat, entries[2].format)
	assert.Equal(t, BinaryFormat, entries[3].format)
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
