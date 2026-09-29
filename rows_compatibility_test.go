package wire

import (
	"context"
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

// PortalSuspension(false) rewrites a finite Execute to an internal NoLimit so
// the handler never parks. These tests pin that the client's MaxRows, not the
// internal limit, decides whether WriteRows may batch, and that WriteRows keeps
// the legacy exact-limit completion and limit+1 ErrRowLimitExceeded contract.

func legacyLimitContext() context.Context {
	session := &Session{Server: &Server{disablePortalSuspension: true}}
	return context.WithValue(setTypeInfo(context.Background(), pgtype.NewMap()), sessionKey, session)
}

func TestLegacyExecuteLimitAppliesToWriteRows(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, tc := range []struct {
			limit  uint32
			rows   []string
			tags   []string
			errors []string
		}{
			{limit: 2, rows: []string{"one", "two"}, errors: []string{"54000"}},
			{limit: 3, rows: []string{"one", "two", "three"}, tags: []string{"SELECT 3"}},
			{limit: 0, rows: []string{"one", "two", "three"}, tags: []string{"SELECT 3"}},
		} {
			t.Run(fmt.Sprintf("parallel=%t/limit=%d", parallel, tc.limit), func(t *testing.T) {
				rec := &recordingObserver{}
				returned := make(chan error, 1)
				conn := compatibilityClient(t, func(context.Context, Query) (PreparedStatements, error) {
					return Prepared(NewStatement(func(_ context.Context, w DataWriter, _ []Parameter) (err error) {
						defer func() { returned <- err }()
						if err := WriteRows(w, [][]any{{"one"}, {"two"}, {"three"}}); err != nil {
							return err
						}
						return w.Complete("SELECT 3")
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
				wantRows := tc.rows
				if parallel && tc.errors != nil {
					wantRows = nil // a failed parallel Execute replays only its error
				}
				require.Equal(t, wantRows, result.rows)
				select {
				case err := <-returned:
					if tc.errors != nil {
						require.ErrorIs(t, err, ErrRowLimitExceeded)
					} else {
						require.NoError(t, err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("row-limited handler still retains resources")
				}
				var encodedBytes uint64
				for _, row := range tc.rows {
					encodedBytes += uint64(len(row))
				}
				require.Equal(t, []observerEntry{textObservation(uint64(len(tc.rows)), encodedBytes)}, rec.snapshot())
			})
		}
	}
}

func TestLegacyFiniteExecuteIsNotBatched(t *testing.T) {
	for _, tc := range []struct {
		limit     Limit
		batchable bool
		err       error
		// DataRow frames and the socket writes that carried them.
		rows, rowWrites int
	}{
		{limit: 2, batchable: false, err: ErrRowLimitExceeded, rows: 2, rowWrites: 2},
		{limit: 3, batchable: false, rows: 3, rowWrites: 3},
		{limit: NoLimit, batchable: true, rows: 3, rowWrites: 1},
	} {
		t.Run(fmt.Sprint(tc.limit), func(t *testing.T) {
			out := &rowPackets{}
			var batchable bool
			portal := &Portal{statement: &Statement{
				columns: Columns{{Name: "value", Oid: pgtype.TextOID}},
				fn: func(_ context.Context, w DataWriter, _ []Parameter) error {
					batchable = w.(*dataWriter).batchable
					if err := WriteRows(w, [][]any{{"one"}, {"two"}, {"three"}}); err != nil {
						return err
					}
					return w.Complete("SELECT 3")
				},
			}}
			defer portal.Close()
			err := portal.execute(legacyLimitContext(), tc.limit, nil, buffer.NewWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), out))
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.batchable, batchable, "batching follows the client's MaxRows")
			frames := rowFrames(t, out.Bytes())
			var rows, rowWrites int
			for _, frame := range frames {
				if frame[0] == byte(types.ServerDataRow) {
					rows++
				}
			}
			for _, packet := range out.packets {
				if packet[0] == byte(types.ServerDataRow) {
					rowWrites++
				}
			}
			require.Equal(t, tc.rows, rows)
			require.Equal(t, tc.rowWrites, rowWrites)
		})
	}
}

func TestRowsKeepsLegacyLimitWhenBatchable(t *testing.T) {
	out := &rowPackets{}
	writer := newRowsWriter(Columns{{Oid: pgtype.TextOID}}, nil, out)
	writer.limit = 2 // defensive: even a batchable writer must not bypass Row's limit
	require.ErrorIs(t, writer.Rows([][]any{{"one"}, {"two"}, {"three"}}), ErrRowLimitExceeded)
	require.Equal(t, uint32(2), writer.Written())
	require.Len(t, out.packets, 2, "rows are written one frame per call, not batched")
}

func TestEncodeObserverBatchedRowsMatchRowTotals(t *testing.T) {
	rows := make([][]any, 1000)
	for i := range rows {
		var note any
		if i%2 == 0 {
			note = "ab"
		}
		rows[i] = []any{int64(i), note}
	}
	columns := Columns{{Name: "id", Oid: pgtype.Int8OID}, {Name: "note", Oid: pgtype.TextOID}}
	observe := func(batch bool) ([]observerEntry, *rowPackets, int) {
		rec := &recordingObserver{}
		ctx := &observerLookupContext{Context: setEncodeObserver(setTypeInfo(context.Background(), pgtype.NewMap()), rec.observe)}
		portal := &Portal{statement: &Statement{
			columns: columns,
			fn: func(_ context.Context, w DataWriter, _ []Parameter) error {
				if batch {
					if err := WriteRows(w, rows); err != nil {
						return err
					}
				} else {
					for _, row := range rows {
						if err := w.Row(row); err != nil {
							return err
						}
					}
				}
				return w.Complete("SELECT 1000")
			},
		}}
		out := &rowPackets{}
		require.NoError(t, portal.execute(ctx, NoLimit, nil, buffer.NewWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), out)))
		return rec.snapshot(), out, ctx.lookups
	}
	rowEntries, rowOut, _ := observe(false)
	batchEntries, batchOut, lookups := observe(true)
	require.Equal(t, rowOut.Bytes(), batchOut.Bytes())
	require.Less(t, len(batchOut.packets), len(rowOut.packets), "WriteRows batched the unlimited Execute")
	require.Equal(t, rowEntries, batchEntries)
	require.Equal(t, []observerEntry{
		{format: TextFormat, oid: pgtype.Int8OID, count: 1000, encodedBytes: 10 + 90*2 + 900*3},
		textObservation(500, 1000),
	}, batchEntries)
	require.Equal(t, 1, lookups, "batched rows resolve the observer once per handler")
}
