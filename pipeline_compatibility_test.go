package wire

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

// Exercise the public Execute/Sync boundary: a resumed parallel portal must
// write into this Execute's response buffer, not the buffer already replayed.
func TestPipelineSuspensionPreservesRowsAndObservation(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprint(parallel), func(t *testing.T) {
			rec := &recordingObserver{}
			handler := func(ctx context.Context, query string) (PreparedStatements, error) {
				return Prepared(NewStatement(func(ctx context.Context, w DataWriter, _ []Parameter) error {
					if len(w.Formats()) != 1 || w.Formats()[0] != BinaryFormat {
						return fmt.Errorf("negotiated formats lost: %v", w.Formats())
					}
					for i := int32(0); i < 5; i++ {
						if err := w.Row([]any{i}); err != nil {
							return err
						}
					}
					return w.Complete("SELECT 5")
				}, WithColumns(Columns{{Name: "id", Oid: pgtype.Int4OID}}))), nil
			}
			server, err := NewServer(handler, Logger(slogt.New(t)), WithEncodeObserver(rec.observe), ParallelPipeline(ParallelPipelineConfig{Enabled: parallel}))
			require.NoError(t, err)
			addr := TListenAndServe(t, server)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://%s/test?sslmode=disable", addr))
			require.NoError(t, err)
			defer func() { _ = conn.Close(context.Background()) }()
			require.NoError(t, conn.Conn().SetDeadline(time.Now().Add(5*time.Second)))
			f := conn.Frontend()
			f.Send(&pgproto3.Parse{Name: "s", Query: "SELECT id"})
			f.Send(&pgproto3.Bind{DestinationPortal: "p", PreparedStatement: "s", ResultFormatCodes: []int16{1}})
			for cycle, limit := range []uint32{2, 10} {
				f.Send(&pgproto3.Execute{Portal: "p", MaxRows: limit})
				f.Send(&pgproto3.Sync{})
				require.NoError(t, f.Flush())
				var rows [][]byte
				var suspended bool
				var tag string
			read:
				for {
					msg, err := f.Receive()
					require.NoError(t, err)
					switch m := msg.(type) {
					case *pgproto3.DataRow:
						require.Len(t, m.Values, 1)
						rows = append(rows, append([]byte(nil), m.Values[0]...))
					case *pgproto3.PortalSuspended:
						suspended = true
					case *pgproto3.CommandComplete:
						tag = string(m.CommandTag)
					case *pgproto3.ErrorResponse:
						t.Fatalf("server error: %+v", m)
					case *pgproto3.ReadyForQuery:
						break read
					}
				}
				if cycle == 0 {
					require.Equal(t, [][]byte{{0, 0, 0, 0}, {0, 0, 0, 1}}, rows)
					require.True(t, suspended)
					require.Empty(t, tag)
				} else {
					require.Equal(t, [][]byte{{0, 0, 0, 2}, {0, 0, 0, 3}, {0, 0, 0, 4}}, rows)
					require.False(t, suspended)
					require.Equal(t, "SELECT 5", tag)
				}
			}
			entries := rec.snapshot()
			require.Len(t, entries, 5)
			for _, e := range entries {
				require.Equal(t, observerEntry{format: BinaryFormat, oid: pgtype.Int4OID, n: 4}, e)
			}
		})
	}
}
