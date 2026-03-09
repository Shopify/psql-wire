package wire

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorCode(t *testing.T) {
	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		stmt := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return psqlerr.WithSeverity(psqlerr.WithCode(errors.New("unimplemented feature"), codes.FeatureNotSupported), psqlerr.LevelFatal)
		})

		return Prepared(stmt), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	assert.NoError(t, err)

	address := TListenAndServe(t, server)

	t.Run("lib/pq", func(t *testing.T) {
		connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
		conn, err := sql.Open("postgres", connstr)
		assert.NoError(t, err)

		_, err = conn.Query("SELECT *;")
		assert.Error(t, err)

		err = conn.Close()
		assert.NoError(t, err)
	})

	t.Run("jackc/pgx", func(t *testing.T) {
		ctx := context.Background()
		connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
		conn, err := pgx.Connect(ctx, connstr)
		assert.NoError(t, err)

		rows, _ := conn.Query(ctx, "SELECT *;")
		rows.Close()
		assert.Error(t, rows.Err())

		err = conn.Close(ctx)
		assert.NoError(t, err)
	})
}

func TestSessionWriteError(t *testing.T) {
	t.Run("simple query error includes ready for query", func(t *testing.T) {
		logger := slogt.New(t)
		sink := bytes.NewBuffer([]byte{})
		writer := buffer.NewWriter(logger, sink)

		session := &Session{
			Server:     &Server{logger: logger},
			Statements: &DefaultStatementCache{},
			Portals:    &DefaultPortalCache{},
		}

		err := session.WriteError(writer, psqlerr.WithCode(errors.New("some error"), codes.Syntax))
		assert.NoError(t, err)

		reader := buffer.NewReader(logger, sink, buffer.DefaultBufferSize)

		msgType, _, err := reader.ReadTypedMsg()
		assert.NoError(t, err)
		assert.Equal(t, types.ServerMessage(msgType), types.ServerErrorResponse)

		msgType, _, err = reader.ReadTypedMsg()
		assert.NoError(t, err)
		assert.Equal(t, types.ServerMessage(msgType), types.ServerReady)
	})

	t.Run("extended query error sets discard flag without ready for query", func(t *testing.T) {
		logger := slogt.New(t)
		sink := bytes.NewBuffer([]byte{})
		writer := buffer.NewWriter(logger, sink)

		session := &Session{
			Server:          &Server{logger: logger},
			Statements:      &DefaultStatementCache{},
			Portals:         &DefaultPortalCache{},
			inExtendedQuery: true,
		}

		err := session.WriteError(writer, psqlerr.WithCode(errors.New("some error"), codes.Syntax))
		assert.NoError(t, err)
		assert.True(t, session.discardUntilSync)

		reader := buffer.NewReader(logger, sink, buffer.DefaultBufferSize)

		msgType, _, err := reader.ReadTypedMsg()
		assert.NoError(t, err)
		assert.Equal(t, types.ServerMessage(msgType), types.ServerErrorResponse)

		// No ReadyForQuery in extended query mode
		_, _, err = reader.ReadTypedMsg()
		assert.Error(t, err)
	})

	t.Run("fatal error skips ready for query", func(t *testing.T) {
		logger := slogt.New(t)
		sink := bytes.NewBuffer([]byte{})
		writer := buffer.NewWriter(logger, sink)

		session := &Session{
			Server:     &Server{logger: logger},
			Statements: &DefaultStatementCache{},
			Portals:    &DefaultPortalCache{},
		}

		err := session.WriteError(writer, psqlerr.WithCode(errors.New("invalid username/password"), codes.InvalidPassword))
		assert.NoError(t, err)

		reader := buffer.NewReader(logger, sink, buffer.DefaultBufferSize)

		msgType, _, err := reader.ReadTypedMsg()
		assert.NoError(t, err)
		assert.Equal(t, types.ServerMessage(msgType), types.ServerErrorResponse)

		_, _, err = reader.ReadTypedMsg()
		assert.Error(t, err)
	})
}

// TestExtendedQueryParseErrorRecovery verifies that a non-fatal error during
// the extended query protocol doesn't desynchronize the connection.
func TestExtendedQueryParseErrorRecovery(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		if query == "SELECT error" {
			return nil, psqlerr.WithCode(errors.New("test error"), codes.Syntax)
		}

		stmt := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})
		return Prepared(stmt), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)

	address := TListenAndServe(t, server)

	ctx := context.Background()
	connstr := fmt.Sprintf("postgres://%s:%d?default_query_exec_mode=cache_statement", address.IP, address.Port)
	conn, err := pgx.Connect(ctx, connstr)
	require.NoError(t, err)

	// First query: triggers a non-fatal error
	rows, _ := conn.Query(ctx, "SELECT error")
	rows.Close()
	assert.Error(t, rows.Err())

	// Second query on the same connection must succeed
	rows, err = conn.Query(ctx, "SELECT 1;")
	require.NoError(t, err)
	rows.Close()
	assert.NoError(t, rows.Err())

	err = conn.Close(ctx)
	assert.NoError(t, err)
}

// TestExtendedQueryExecuteErrorRecovery verifies that an error during Execute
// doesn't desynchronize the connection.
func TestExtendedQueryExecuteErrorRecovery(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		columns := Columns{{Name: "result", Oid: 25}} // text

		if query == "SELECT error" {
			stmt := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				return psqlerr.WithCode(errors.New("execution failed"), codes.DataException)
			}, WithColumns(columns))
			return Prepared(stmt), nil
		}

		stmt := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		}, WithColumns(columns))
		return Prepared(stmt), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)

	address := TListenAndServe(t, server)

	ctx := context.Background()
	connstr := fmt.Sprintf("postgres://%s:%d?default_query_exec_mode=cache_statement", address.IP, address.Port)
	conn, err := pgx.Connect(ctx, connstr)
	require.NoError(t, err)

	// First query: parses successfully but fails during Execute
	rows, _ := conn.Query(ctx, "SELECT error")
	rows.Close()
	assert.Error(t, rows.Err())

	// Second query must succeed
	rows, err = conn.Query(ctx, "SELECT 1;")
	require.NoError(t, err)
	rows.Close()
	assert.NoError(t, rows.Err())

	err = conn.Close(ctx)
	assert.NoError(t, err)
}
