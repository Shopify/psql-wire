package wire

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/lib/pq/oid"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleExecuteSuccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	typeMap := pgtype.NewMap()
	ctx = setTypeInfo(ctx, typeMap)

	logger := slogt.New(t)

	mockParse := func(ctx context.Context, query string) (PreparedStatements, error) {
		stmt := NewStatement(
			func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				writer.Row([]any{"Hello World"})
				return writer.Complete("SELECT 1")
			},
			WithParameters([]oid.Oid{}),
			WithColumns(Columns{
				{Name: "greeting", Oid: oid.T_text},
			}),
		)
		return PreparedStatements{stmt}, nil
	}

	session := &Session{
		Server: &Server{
			logger: logger,
			parse:  mockParse,
		},
		Statements:    &DefaultStatementCache{},
		Portals:       &DefaultPortalCache{},
		ResponseQueue: NewResponseQueue(),
	}

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	// Parse
	inputBuf := &bytes.Buffer{}
	mockWriter := mock.NewWriter(t, inputBuf)
	mockWriter.Start(types.ClientParse)
	mockWriter.AddString("stmt1")
	mockWriter.AddNullTerminate()
	mockWriter.AddString("SELECT 'Hello World'")
	mockWriter.AddNullTerminate()
	mockWriter.AddInt16(0)
	mockWriter.End()

	reader := buffer.NewReader(logger, inputBuf, buffer.DefaultBufferSize)
	_, _, err := reader.ReadTypedMsg()
	require.NoError(t, err)

	err = session.handleParse(ctx, reader, writer)
	require.NoError(t, err)

	// Bind
	inputBuf = &bytes.Buffer{}
	mockWriter = mock.NewWriter(t, inputBuf)
	mockWriter.Start(types.ClientBind)
	mockWriter.AddString("portal1")
	mockWriter.AddNullTerminate()
	mockWriter.AddString("stmt1")
	mockWriter.AddNullTerminate()
	mockWriter.AddInt16(0) // param formats
	mockWriter.AddInt16(0) // param values
	mockWriter.AddInt16(0) // result formats
	mockWriter.End()

	reader = buffer.NewReader(logger, inputBuf, buffer.DefaultBufferSize)
	_, _, err = reader.ReadTypedMsg()
	require.NoError(t, err)

	err = session.handleBind(ctx, reader, writer)
	require.NoError(t, err)

	// Describe portal
	inputBuf = &bytes.Buffer{}
	mockWriter = mock.NewWriter(t, inputBuf)
	mockWriter.Start(types.ClientDescribe)
	mockWriter.AddByte(byte(types.DescribePortal))
	mockWriter.AddString("portal1")
	mockWriter.AddNullTerminate()
	mockWriter.End()

	reader = buffer.NewReader(logger, inputBuf, buffer.DefaultBufferSize)
	_, _, err = reader.ReadTypedMsg()
	require.NoError(t, err)

	err = session.handleDescribe(ctx, reader, writer)
	require.NoError(t, err)

	// Execute
	inputBuf = &bytes.Buffer{}
	mockWriter = mock.NewWriter(t, inputBuf)
	mockWriter.Start(types.ClientExecute)
	mockWriter.AddString("portal1")
	mockWriter.AddNullTerminate()
	mockWriter.AddInt32(0) // no limit
	mockWriter.End()

	reader = buffer.NewReader(logger, inputBuf, buffer.DefaultBufferSize)
	_, _, err = reader.ReadTypedMsg()
	require.NoError(t, err)

	// Only check Execute's output
	outBuf.Reset()

	err = session.handleExecute(ctx, reader, writer)
	require.NoError(t, err)

	// Verify queue state: ParseComplete, BindComplete, PortalDescribe, Execute
	assert.Equal(t, 4, session.ResponseQueue.Len())
	events := session.ResponseQueue.DrainAll()
	require.Len(t, events, 4)

	// Verify Execute Event
	execEvent := events[3]
	assert.Equal(t, ResponseExecute, execEvent.Kind)
	assert.NotNil(t, execEvent.ResultChannel)

	result := <-execEvent.ResultChannel
	require.NoError(t, result.GetError())

	dataWriter := NewDataWriter(ctx, result.Columns(), execEvent.Formats, NoLimit, nil, writer)
	err = result.Replay(ctx, dataWriter)
	require.NoError(t, err)

	responseReader := buffer.NewReader(logger, outBuf, buffer.DefaultBufferSize)

	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ClientMessage(types.ServerDataRow), msgType)

	// Verify row content "Hello World"
	colCount, err := responseReader.GetUint16()
	require.NoError(t, err)
	assert.Equal(t, uint16(1), colCount)

	colLen, err := responseReader.GetInt32()
	require.NoError(t, err)
	assert.Equal(t, int32(11), colLen) // "Hello World" length

	val, err := responseReader.GetBytes(11)
	require.NoError(t, err)
	assert.Equal(t, "Hello World", string(val))

	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ClientMessage(types.ServerCommandComplete), msgType)
}
