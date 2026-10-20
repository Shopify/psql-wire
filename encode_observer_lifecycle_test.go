package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/stretchr/testify/require"
)

// These tests pin when aggregated encode observations are published relative
// to the portal lifecycle. Every value encoded by a handler is reported exactly
// once, and each Execute boundary publishes only the values it encoded.

func textObservation(count uint64, encodedBytes uint64) observerEntry {
	return observerEntry{format: TextFormat, oid: pgtype.TextOID, count: count, encodedBytes: encodedBytes}
}

func TestEncodeObserverLegacyLimitPublishesEncodedRows(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, tc := range []struct {
			name   string
			limit  uint32
			errors []string
			tags   []string
		}{
			{name: "exact", limit: 2, tags: []string{"SELECT 2"}},
			{name: "overflow", limit: 1, errors: []string{"54000"}},
		} {
			t.Run(fmt.Sprintf("parallel=%t/%s", parallel, tc.name), func(t *testing.T) {
				rec := &recordingObserver{}
				conn := compatibilityClient(t, func(context.Context, Query) (PreparedStatements, error) {
					return Prepared(NewStatement(func(_ context.Context, w DataWriter, _ []Parameter) error {
						for i := 0; i < 2; i++ {
							if err := w.Row([]any{"row"}); err != nil {
								return err
							}
						}
						return w.Complete("SELECT 2")
					}, WithColumns(Columns{{Name: "value", Oid: pgtype.TextOID}}))), nil
				}, PortalSuspension(false), WithEncodeObserver(rec.observe), ParallelPipeline(ParallelPipelineConfig{Enabled: parallel}))
				f := conn.Frontend()
				f.Send(&pgproto3.Parse{Name: "s", Query: "limited"})
				f.Send(&pgproto3.Bind{DestinationPortal: "p", PreparedStatement: "s"})
				f.Send(&pgproto3.Execute{Portal: "p", MaxRows: tc.limit})
				f.Send(&pgproto3.Sync{})
				require.NoError(t, f.Flush())
				result := readCompatibilityBatch(t, f)
				require.Equal(t, tc.errors, result.errors)
				require.Equal(t, tc.tags, result.tags)
				// The overflowing handler unwinds with ErrRowLimitExceeded; the
				// row it encoded before the limit is still published. In parallel
				// mode the errored result is not replayed, but it was encoded.
				require.Equal(t, []observerEntry{textObservation(uint64(tc.limit), 3*uint64(tc.limit))}, rec.snapshot())
			})
		}
	}
}

func TestEncodeObserverHandlerErrorPublishesEncodedValues(t *testing.T) {
	for _, mode := range []string{"simple", "extended", "parallel"} {
		t.Run(mode, func(t *testing.T) {
			rec := &recordingObserver{}
			handlerErr := errors.New("backend failed")
			conn := compatibilityClient(t, func(context.Context, Query) (PreparedStatements, error) {
				return Prepared(NewStatement(func(_ context.Context, w DataWriter, _ []Parameter) error {
					for i := 0; i < 2; i++ {
						if err := w.Row([]any{"row"}); err != nil {
							return err
						}
					}
					return handlerErr
				}, WithColumns(Columns{{Name: "value", Oid: pgtype.TextOID}}))), nil
			}, WithEncodeObserver(rec.observe), ParallelPipeline(ParallelPipelineConfig{Enabled: mode == "parallel"}))
			f := conn.Frontend()
			if mode == "simple" {
				f.Send(&pgproto3.Query{String: "failing"})
			} else {
				f.Send(&pgproto3.Parse{Name: "s", Query: "failing"})
				f.Send(&pgproto3.Bind{DestinationPortal: "p", PreparedStatement: "s"})
				f.Send(&pgproto3.Execute{Portal: "p"})
				f.Send(&pgproto3.Sync{})
			}
			require.NoError(t, f.Flush())
			result := readObserverBatch(t, f, false)
			require.Len(t, result.errors, 1)
			require.Equal(t, []observerEntry{textObservation(2, 6)}, rec.snapshot())
		})
	}
}

// A suspended portal is torn down either by an explicit Close or, since
// v0.20, by the idle ReadyForQuery that answers Sync.
func TestEncodeObserverSuspendedPortalTeardownDoesNotRepublish(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, teardown := range []string{"close", "idle-sync"} {
			t.Run(fmt.Sprintf("parallel=%t/%s", parallel, teardown), func(t *testing.T) {
				rec := &recordingObserver{}
				returned := make(chan error, 1)
				conn := compatibilityClient(t, func(context.Context, Query) (PreparedStatements, error) {
					return Prepared(NewStatement(func(_ context.Context, w DataWriter, _ []Parameter) (err error) {
						defer func() { returned <- err }()
						for i := 0; i < 3; i++ {
							if err := w.Row([]any{"row"}); err != nil {
								return err
							}
						}
						return w.Complete("SELECT 3")
					}, WithColumns(Columns{{Name: "value", Oid: pgtype.TextOID}}))), nil
				}, WithEncodeObserver(rec.observe), ParallelPipeline(ParallelPipelineConfig{Enabled: parallel}))
				f := conn.Frontend()
				f.Send(&pgproto3.Parse{Name: "s", Query: "resumable"})
				f.Send(&pgproto3.Bind{DestinationPortal: "p", PreparedStatement: "s"})
				f.Send(&pgproto3.Execute{Portal: "p", MaxRows: 1})
				if teardown == "close" {
					f.Send(&pgproto3.Flush{})
				} else {
					f.Send(&pgproto3.Sync{})
				}
				require.NoError(t, f.Flush())
				suspended := readObserverBatch(t, f, teardown == "close")
				require.Empty(t, suspended.errors)
				require.Equal(t, []string{"row"}, suspended.rows)
				require.True(t, suspended.suspended)

				if teardown == "close" {
					require.Equal(t, []observerEntry{textObservation(1, 3)}, rec.snapshot())
					f.Send(&pgproto3.Close{ObjectType: 'P', Name: "p"})
					f.Send(&pgproto3.Sync{})
					require.NoError(t, f.Flush())
					require.Empty(t, readObserverBatch(t, f, false).errors)
				}
				select {
				case err := <-returned:
					require.ErrorIs(t, err, ErrSuspendedHandlerClosed)
				case <-time.After(5 * time.Second):
					t.Fatal("closed portal did not unwind its suspended handler")
				}
				require.Equal(t, []observerEntry{textObservation(1, 3)}, rec.snapshot(), "teardown must not republish rows")
			})
		}
	}
}

func TestEncodeObserverFormatsFollowNegotiatedColumns(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, tc := range []struct {
			formats []int16
			want    []observerEntry
		}{
			{formats: nil, want: []observerEntry{
				{format: TextFormat, oid: pgtype.TextOID, count: 1, encodedBytes: 1},
				{format: TextFormat, oid: pgtype.Int4OID, count: 1, encodedBytes: 1},
			}},
			{formats: []int16{1}, want: []observerEntry{
				{format: BinaryFormat, oid: pgtype.TextOID, count: 1, encodedBytes: 1},
				{format: BinaryFormat, oid: pgtype.Int4OID, count: 1, encodedBytes: 4},
			}},
			{formats: []int16{0, 1}, want: []observerEntry{
				{format: TextFormat, oid: pgtype.TextOID, count: 1, encodedBytes: 1},
				{format: BinaryFormat, oid: pgtype.Int4OID, count: 1, encodedBytes: 4},
			}},
		} {
			t.Run(fmt.Sprintf("parallel=%t/formats=%v", parallel, tc.formats), func(t *testing.T) {
				rec := &recordingObserver{}
				conn := compatibilityClient(t, func(context.Context, Query) (PreparedStatements, error) {
					return Prepared(NewStatement(func(_ context.Context, w DataWriter, _ []Parameter) error {
						if err := w.Row([]any{"x", int32(7)}); err != nil {
							return err
						}
						return w.Complete("SELECT 1")
					}, WithColumns(Columns{{Name: "text", Oid: pgtype.TextOID}, {Name: "int", Oid: pgtype.Int4OID}}))), nil
				}, WithEncodeObserver(rec.observe), ParallelPipeline(ParallelPipelineConfig{Enabled: parallel}))
				result := conn.ExecParams(context.Background(), "formats", nil, nil, nil, tc.formats).Read()
				require.NoError(t, result.Err)
				require.Equal(t, tc.want, rec.snapshot())
			})
		}
	}
}

func TestEncodeObserverAggregatesTotalsExcludingNulls(t *testing.T) {
	rec := &recordingObserver{}
	ctx := setEncodeObserver(setTypeInfo(context.Background(), pgtype.NewMap()), rec.observe)
	portal := &Portal{statement: &Statement{
		columns: Columns{{Name: "id", Oid: pgtype.Int8OID}, {Name: "note", Oid: pgtype.TextOID}},
		fn: func(_ context.Context, w DataWriter, _ []Parameter) error {
			for i := int64(0); i < 1000; i++ {
				var note any
				if i%2 == 0 {
					note = "ab"
				}
				if err := w.Row([]any{i, note}); err != nil {
					return err
				}
			}
			return w.Complete("SELECT 1000")
		},
	}}
	var out bytes.Buffer
	require.NoError(t, portal.execute(ctx, NoLimit, nil, buffer.NewWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), &out)))

	// Text int8 values 0..999: 10 one-digit, 90 two-digit, 900 three-digit.
	require.Equal(t, []observerEntry{
		{format: TextFormat, oid: pgtype.Int8OID, count: 1000, encodedBytes: 10 + 90*2 + 900*3},
		textObservation(500, 1000),
	}, rec.snapshot(), "one callback per column; NULL notes excluded")
}

func TestEncodeObserverCancellationPublishesEncodedValues(t *testing.T) {
	rec := &recordingObserver{}
	ctx, cancel := context.WithCancel(setEncodeObserver(setTypeInfo(context.Background(), pgtype.NewMap()), rec.observe))
	defer cancel()
	portal := &Portal{statement: &Statement{
		columns: Columns{{Name: "value", Oid: pgtype.TextOID}},
		fn: func(_ context.Context, w DataWriter, _ []Parameter) error {
			for i := 0; i < 2; i++ {
				if err := w.Row([]any{"row"}); err != nil {
					return err
				}
			}
			cancel()
			if err := w.Row([]any{"row"}); err != nil {
				return err
			}
			return w.Complete("SELECT 3")
		},
	}}
	err := portal.execute(ctx, NoLimit, nil, buffer.NewWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), io.Discard))
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []observerEntry{textObservation(2, 6)}, rec.snapshot())
}

func TestEncodeObserverDirectColumnWritesReportPerValue(t *testing.T) {
	rec := &recordingObserver{}
	ctx := setEncodeObserver(setTypeInfo(context.Background(), pgtype.NewMap()), rec.observe)
	writer := buffer.NewWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), io.Discard)
	columns := Columns{{Name: "value", Oid: pgtype.TextOID}}
	require.NoError(t, columns.Write(ctx, nil, writer, []any{"row"}))
	require.Equal(t, []observerEntry{textObservation(1, 3)}, rec.snapshot(), "public Columns.Write has no flush point")
	require.NoError(t, columns.Write(ctx, nil, writer, []any{nil}))
	require.Len(t, rec.snapshot(), 1, "NULL values must not be observed")
	require.NoError(t, columns[0].Write(ctx, writer, BinaryFormat, "value"))
	require.Equal(t, []observerEntry{
		textObservation(1, 3),
		{format: BinaryFormat, oid: pgtype.TextOID, count: 1, encodedBytes: 5},
	}, rec.snapshot(), "public Column.Write has no flush point")
}

// observerLookupContext counts EncodeObserver lookups on the context.
type observerLookupContext struct {
	context.Context
	lookups int
}

func (ctx *observerLookupContext) Value(key any) any {
	if key == ctxEncodeObserver {
		ctx.lookups++
	}
	return ctx.Context.Value(key)
}

func TestEncodeObserverResolvedOncePerHandler(t *testing.T) {
	for _, observed := range []bool{false, true} {
		t.Run(fmt.Sprint(observed), func(t *testing.T) {
			rec := &recordingObserver{}
			base := setTypeInfo(context.Background(), pgtype.NewMap())
			if observed {
				base = setEncodeObserver(base, rec.observe)
			}
			ctx := &observerLookupContext{Context: base}
			portal := &Portal{statement: &Statement{
				columns: Columns{{Name: "a", Oid: pgtype.TextOID}, {Name: "b", Oid: pgtype.TextOID}},
				fn: func(_ context.Context, w DataWriter, _ []Parameter) error {
					for i := 0; i < 100; i++ {
						if err := w.Row([]any{"x", "y"}); err != nil {
							return err
						}
					}
					return w.Complete("SELECT 100")
				},
			}}
			require.NoError(t, portal.execute(ctx, NoLimit, nil, buffer.NewWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), io.Discard)))
			require.Equal(t, 1, ctx.lookups, "the handler's writer resolves the observer once, not per value")
			if observed {
				require.Equal(t, []observerEntry{textObservation(100, 100), textObservation(100, 100)}, rec.snapshot())
			}
		})
	}
}

type observerBatch struct {
	rows, tags, errors []string
	suspended          bool
}

// readObserverBatch reads responses until ReadyForQuery, or until
// PortalSuspended when untilSuspended is set (after a Flush).
func readObserverBatch(t *testing.T, f *pgproto3.Frontend, untilSuspended bool) observerBatch {
	t.Helper()
	var result observerBatch
	for {
		msg, err := f.Receive()
		require.NoError(t, err)
		switch m := msg.(type) {
		case *pgproto3.DataRow:
			result.rows = append(result.rows, string(m.Values[0]))
		case *pgproto3.PortalSuspended:
			result.suspended = true
			if untilSuspended {
				return result
			}
		case *pgproto3.CommandComplete:
			result.tags = append(result.tags, string(m.CommandTag))
		case *pgproto3.ErrorResponse:
			result.errors = append(result.errors, m.Code)
		case *pgproto3.ReadyForQuery:
			return result
		}
	}
}

// observedAtFrame records how many observations were published when each
// completion or suspension frame reached the connection.
type observedAtFrame struct {
	rec      *recordingObserver
	complete []int
}

func (w *observedAtFrame) Write(p []byte) (int, error) {
	if p[0] == byte(types.ServerCommandComplete) || p[0] == byte(types.ServerPortalSuspended) {
		w.complete = append(w.complete, len(w.rec.snapshot()))
	}
	return len(p), nil
}

func TestEncodeObserverPublishesBeforeClientSeesBoundary(t *testing.T) {
	rec := &recordingObserver{}
	ctx := setEncodeObserver(setTypeInfo(context.Background(), pgtype.NewMap()), rec.observe)
	out := &observedAtFrame{rec: rec}
	writer := buffer.NewWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), out)
	portal := &Portal{statement: &Statement{
		columns: Columns{{Name: "value", Oid: pgtype.TextOID}},
		fn: func(_ context.Context, w DataWriter, _ []Parameter) error {
			for i := 0; i < 3; i++ {
				if err := w.Row([]any{"row"}); err != nil {
					return err
				}
			}
			return w.Complete("SELECT 3")
		},
	}}
	defer portal.Close()
	require.NoError(t, portal.execute(ctx, 2, nil, writer))
	require.NoError(t, portal.execute(ctx, 2, nil, writer))
	// A client that reads PortalSuspended or CommandComplete can already see
	// the observations for the rows before it.
	require.Equal(t, []int{1, 2}, out.complete)
	require.Equal(t, []observerEntry{textObservation(2, 6), textObservation(1, 3)}, rec.snapshot())
}
