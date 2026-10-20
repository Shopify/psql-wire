package wire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A 128 KiB message limit, and a Bind parameter that makes the Bind message
// larger than that limit.
const (
	oversizeMessageBufferSize = 128 << 10
	oversizeParamBytes        = oversizeMessageBufferSize + 1024
)

// oversizeClient starts a server whose Parse fails for "FAIL" and whose
// other statements return one row that contains their own query text.
func oversizeClient(t *testing.T, parallel bool) *pgconn.PgConn {
	t.Helper()
	return compatibilityClient(t, func(_ context.Context, query Query) (PreparedStatements, error) {
		if query.Query == "FAIL" {
			return nil, errors.New("planned parse failure")
		}
		value := query.Query
		return Prepared(NewStatement(func(_ context.Context, writer DataWriter, _ []Parameter) error {
			if err := writer.Row([]any{value}); err != nil {
				return err
			}
			return writer.Complete("SELECT 1")
		}, WithColumns(Columns{{Name: "value_of_" + value, Oid: pgtype.TextOID}}))), nil
	},
		ParallelPipeline(ParallelPipelineConfig{Enabled: parallel}),
		MessageBufferSize(oversizeMessageBufferSize),
	)
}

func sendNamedQuery(frontend *pgproto3.Frontend, name, query string) {
	frontend.Send(&pgproto3.Parse{Name: name, Query: query})
	frontend.Send(&pgproto3.Bind{DestinationPortal: name, PreparedStatement: name})
	frontend.Send(&pgproto3.Describe{ObjectType: 'P', Name: name})
	frontend.Send(&pgproto3.Execute{Portal: name})
}

func describeMessages(messages []pgproto3.BackendMessage) []string {
	out := make([]string, 0, len(messages))
	for _, message := range messages {
		switch m := message.(type) {
		case *pgproto3.ErrorResponse:
			out = append(out, fmt.Sprintf("ErrorResponse(%s: %s)", m.Code, m.Message))
		default:
			out = append(out, fmt.Sprintf("%T", message)[len("*pgproto3."):])
		}
	}
	return out
}

// After an error in an extended-query series, PostgreSQL sends one
// ErrorResponse and discards every message until Sync. An oversized message
// in that series must also be discarded. The test sends a second series on
// the same connection, because an extra ErrorResponse or ReadyForQuery puts
// the client one result behind. Both series are sent before any read, so the
// test records the complete server stream. It splits the stream the way a
// client does: series 1 ends at the first ReadyForQuery.
func TestOversizedBindAfterParseErrorEmitsOneError(t *testing.T) {
	for _, parallel := range []bool{true, false} {
		t.Run(fmt.Sprintf("ParallelPipeline=%v", parallel), func(t *testing.T) {
			conn := oversizeClient(t, parallel)
			frontend := conn.Frontend()

			// Batch 1: three good queries, failing Parse, oversized Bind.
			for i, query := range []string{"first", "second", "third"} {
				sendNamedQuery(frontend, string(rune('a'+i)), query)
			}
			frontend.Send(&pgproto3.Parse{Name: "bad", Query: "FAIL"})
			// PostgreSQL requires the server to discard this Bind after the
			// Parse error. The frame also exceeds the 128 KiB message limit.
			frontend.Send(&pgproto3.Bind{
				DestinationPortal: "bad",
				PreparedStatement: "bad",
				Parameters:        [][]byte{make([]byte, oversizeParamBytes)},
			})
			frontend.Send(&pgproto3.Describe{ObjectType: 'P', Name: "bad"})
			frontend.Send(&pgproto3.Execute{Portal: "bad"})
			frontend.Send(&pgproto3.Sync{})

			// Batch 2: a normal query on the same connection.
			sendNamedQuery(frontend, "after", "after")
			frontend.Send(&pgproto3.Sync{})
			require.NoError(t, frontend.Flush())

			// Read until batch 2's DataRow and its trailing ReadyForQuery.
			var stream []string
			sawAfterRow := false
			for {
				msg, err := frontend.Receive()
				require.NoError(t, err, "stream so far: %v", stream)
				stream = append(stream, describeMessages([]pgproto3.BackendMessage{msg})[0])
				if dr, ok := msg.(*pgproto3.DataRow); ok && string(dr.Values[0]) == "after" {
					sawAfterRow = true
				}
				if _, ok := msg.(*pgproto3.ReadyForQuery); ok && sawAfterRow {
					break
				}
			}
			t.Logf("full stream: %v", stream)

			split := 0
			for i, m := range stream {
				if m == "ReadyForQuery" {
					split = i + 1
					break
				}
			}
			batch1, batch2 := stream[:split], stream[split:]
			t.Logf("batch 1 (client view): %v", batch1)
			t.Logf("batch 2 (client view): %v", batch2)

			errorCount, readyCount := 0, 0
			for _, m := range stream {
				if strings.HasPrefix(m, "ErrorResponse") {
					errorCount++
				}
				if m == "ReadyForQuery" {
					readyCount++
				}
			}
			// PostgreSQL: one ErrorResponse for the failed Parse, then discard
			// Bind, Describe, Execute until Sync; one ReadyForQuery per Sync.
			assert.Equal(t, 1, errorCount, "exactly one ErrorResponse across both batches")
			assert.Equal(t, 2, readyCount, "exactly one ReadyForQuery per Sync (2 Syncs sent)")

			assert.Equal(t, []string{
				"ParseComplete", "BindComplete", "RowDescription", "DataRow", "CommandComplete",
				"ParseComplete", "BindComplete", "RowDescription", "DataRow", "CommandComplete",
				"ParseComplete", "BindComplete", "RowDescription", "DataRow", "CommandComplete",
				"ErrorResponse(XXUUU: planned parse failure)",
				"ReadyForQuery",
			}, batch1, "batch 1")
			assert.Equal(t, []string{
				"ParseComplete", "BindComplete", "RowDescription", "DataRow", "CommandComplete",
				"ReadyForQuery",
			}, batch2, "batch 2 must start with its own ParseComplete, not a stray ErrorResponse or ReadyForQuery")
		})
	}
}

// Oversized extended-query message with no earlier error: exactly one
// ErrorResponse, and the only ReadyForQuery comes from Sync.
func TestOversizedBindAloneKeepsExtendedQueryState(t *testing.T) {
	for _, parallel := range []bool{true, false} {
		t.Run(fmt.Sprintf("ParallelPipeline=%v", parallel), func(t *testing.T) {
			conn := oversizeClient(t, parallel)
			frontend := conn.Frontend()
			frontend.Send(&pgproto3.Parse{Name: "s", Query: "big"})
			frontend.Send(&pgproto3.Bind{DestinationPortal: "s", PreparedStatement: "s", Parameters: [][]byte{make([]byte, oversizeParamBytes)}})
			frontend.Send(&pgproto3.Describe{ObjectType: 'P', Name: "s"})
			frontend.Send(&pgproto3.Execute{Portal: "s"})
			frontend.Send(&pgproto3.Sync{})
			sendNamedQuery(frontend, "after", "after")
			frontend.Send(&pgproto3.Sync{})
			require.NoError(t, frontend.Flush())

			var stream []string
			sawAfterRow := false
			for {
				msg, err := frontend.Receive()
				require.NoError(t, err, "stream so far: %v", stream)
				stream = append(stream, describeMessages([]pgproto3.BackendMessage{msg})[0])
				if dr, ok := msg.(*pgproto3.DataRow); ok && string(dr.Values[0]) == "after" {
					sawAfterRow = true
				}
				if _, ok := msg.(*pgproto3.ReadyForQuery); ok && sawAfterRow {
					break
				}
			}
			t.Logf("full stream: %v", stream)
			require.Len(t, stream, 9, "stream: %v", stream)
			assert.Equal(t, "ParseComplete", stream[0])
			assert.True(t, strings.HasPrefix(stream[1], "ErrorResponse"), stream[1])
			assert.Equal(t, []string{"ReadyForQuery", "ParseComplete", "BindComplete", "RowDescription", "DataRow", "CommandComplete", "ReadyForQuery"}, stream[2:])
		})
	}
}

// pipelineSlot is what a libpq-style client attributes to one queued command.
type pipelineSlot struct {
	column, row, err string
	sync             bool
}

func (s pipelineSlot) String() string {
	switch {
	case s.sync:
		return "PipelineSync"
	case s.err != "":
		return "Error(" + s.err + ")"
	default:
		return fmt.Sprintf("Rows(%s=%s)", s.column, s.row)
	}
}

func readPipelineSlot(t *testing.T, pipeline *pgconn.Pipeline) pipelineSlot {
	t.Helper()
	result, err := pipeline.GetResults()
	if err != nil {
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		return pipelineSlot{err: pgErr.Message}
	}
	switch r := result.(type) {
	case *pgconn.PipelineSync:
		return pipelineSlot{sync: true}
	case *pgconn.ResultReader:
		slot := pipelineSlot{}
		if fields := r.FieldDescriptions(); len(fields) > 0 {
			slot.column = fields[0].Name
		}
		for r.NextRow() {
			slot.row = string(r.Values()[0])
		}
		_, err := r.Close()
		require.NoError(t, err)
		return slot
	default:
		t.Fatalf("unexpected pipeline result %T", result)
		return pipelineSlot{}
	}
}

// A pipeline client reads one result for each queued command, in order, as
// libpq does. Batch 1 has four queries: the fourth Parse fails and its Bind is
// oversized. Batch 2 is a normal query on the same connection and must get
// its own rows.
func TestOversizedBindAfterParseErrorPipelineSlots(t *testing.T) {
	for _, parallel := range []bool{true, false} {
		t.Run(fmt.Sprintf("ParallelPipeline=%v", parallel), func(t *testing.T) {
			conn := oversizeClient(t, parallel)
			pipeline := conn.StartPipeline(context.Background())

			for _, query := range []string{"q0", "q1", "q2"} {
				pipeline.SendQueryParams(query, nil, nil, nil, nil)
			}
			pipeline.SendQueryParams("FAIL", [][]byte{make([]byte, oversizeParamBytes)}, []uint32{pgtype.TextOID}, nil, nil)
			require.NoError(t, pipeline.Sync())
			pipeline.SendQueryParams("after", nil, nil, nil, nil)
			require.NoError(t, pipeline.Sync())

			// Expected slots: q0, q1, q2, q3 (error), sync, after, sync.
			var got []string
			for i := 0; i < 7; i++ {
				got = append(got, readPipelineSlot(t, pipeline).String())
			}
			t.Logf("pipeline slots: %v", got)
			require.Equal(t, []string{
				"Rows(value_of_q0=q0)",
				"Rows(value_of_q1=q1)",
				"Rows(value_of_q2=q2)",
				"Error(planned parse failure)",
				"PipelineSync",
				"Rows(value_of_after=after)",
				"PipelineSync",
			}, got)
			require.NoError(t, pipeline.Close())
		})
	}
}
