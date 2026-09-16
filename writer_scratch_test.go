package wire

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/lib/pq/oid"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

func TestDataWriterEncodeScratchReuse(t *testing.T) {
	t.Parallel()

	comparison := newRowWriteComparison(t, Columns{{Oid: uint32(oid.T_text)}}, []FormatCode{BinaryFormat})
	require.Nil(t, comparison.writer.encodeScratch)
	require.NoError(t, comparison.row(t, []any{strings.Repeat("a", 32)}))
	first := comparison.writer.encodeScratch
	require.Zero(t, len(first))
	require.GreaterOrEqual(t, cap(first), 32)

	require.NoError(t, comparison.row(t, []any{strings.Repeat("b", 4096)}))
	grown := comparison.writer.encodeScratch
	require.GreaterOrEqual(t, cap(grown), 4096)
	require.NotSame(t, &first[:cap(first)][0], &grown[:cap(grown)][0])

	for _, value := range []any{nil, pgtype.Text{}, (*string)(nil), "", "small"} {
		require.NoError(t, comparison.row(t, []any{value}))
		scratch := comparison.writer.encodeScratch
		require.Zero(t, len(scratch))
		require.Equal(t, cap(grown), cap(scratch))
		require.Same(t, &grown[:cap(grown)][0], &scratch[:cap(scratch)][0], "nil, empty and smaller values must retain the grown buffer")
	}

	require.NoError(t, comparison.writer.Complete("SELECT 8"))
	require.Nil(t, comparison.writer.encodeScratch)
	require.ErrorIs(t, comparison.writer.Row([]any{"closed"}), ErrClosedWriter)
	require.Nil(t, comparison.writer.encodeScratch)
}

func TestDataWriterEncodeScratchCapacityBound(t *testing.T) {
	t.Parallel()

	comparison := newRowWriteComparison(t, Columns{{Oid: uint32(oid.T_text)}}, []FormatCode{BinaryFormat})
	for _, size := range []int{32, maxEncodeScratchCapacity, maxEncodeScratchCapacity + 1, 16, 4 * maxEncodeScratchCapacity, 0, 8} {
		require.NoError(t, comparison.row(t, []any{strings.Repeat("x", size)}))
		require.LessOrEqual(t, cap(comparison.writer.encodeScratch), maxEncodeScratchCapacity)
		if size > maxEncodeScratchCapacity {
			require.Nil(t, comparison.writer.encodeScratch, "oversized encoding must be dropped after copying")
		}
	}
}

// The codec exposes the supplied buffer and can deliberately grow it before
// returning a NULL or error, or return a short value backed by a large buffer.
// This checks ownership/retention without relying on a particular pgx codec's
// allocation strategy.
type scratchTestValue struct {
	text     string
	capacity int
	null     bool
	err      error
}

type scratchTestCodec struct {
	pgtype.TextCodec
	inputs [][]byte
}

func (codec *scratchTestCodec) PlanEncode(_ *pgtype.Map, _ uint32, _ int16, value any) pgtype.EncodePlan {
	if _, ok := value.(scratchTestValue); ok {
		return codec
	}
	return nil
}

func (codec *scratchTestCodec) Encode(value any, buf []byte) ([]byte, error) {
	codec.inputs = append(codec.inputs, buf)
	v := value.(scratchTestValue)
	if v.capacity > cap(buf) {
		buf = make([]byte, 0, v.capacity)
	}
	buf = append(buf, v.text...)
	if v.err != nil {
		return buf, v.err
	}
	if v.null {
		return nil, nil
	}
	return buf, nil
}

func TestDataWriterEncodeScratchCodecResults(t *testing.T) {
	t.Parallel()

	const customOID = 90001
	comparison := newRowWriteComparison(t, Columns{{Oid: customOID}}, []FormatCode{BinaryFormat})
	codec := &scratchTestCodec{}
	TypeMap(comparison.writer.ctx).RegisterType(&pgtype.Type{Name: "scratch_test", OID: customOID, Codec: codec})
	TypeMap(comparison.referenceCtx).RegisterType(&pgtype.Type{Name: "scratch_test", OID: customOID, Codec: &scratchTestCodec{}})

	require.NoError(t, comparison.row(t, []any{scratchTestValue{text: "warm", capacity: 128}}))
	scratch := comparison.writer.encodeScratch
	require.Equal(t, 128, cap(scratch))
	require.NotNil(t, codec.inputs[0], "first encode still gets a non-nil empty buffer")
	require.Zero(t, cap(codec.inputs[0]))

	encodeErr := errors.New("codec failed after growing")
	for _, value := range []scratchTestValue{
		{text: "null", capacity: 4096, null: true},
		{text: "error", capacity: 4096, err: encodeErr},
		{text: "partial null", null: true},
		{text: "partial error", err: encodeErr},
		{text: "fits"},
	} {
		err := comparison.row(t, []any{value})
		if value.err != nil {
			require.ErrorIs(t, err, encodeErr)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, 128, cap(comparison.writer.encodeScratch), "nil/error results must not replace retained scratch")
		require.Same(t, &scratch[:cap(scratch)][0], &comparison.writer.encodeScratch[:cap(scratch)][0])
		input := codec.inputs[len(codec.inputs)-1]
		require.Zero(t, len(input), "every encode starts at offset zero")
		require.Same(t, &scratch[:cap(scratch)][0], &input[:cap(input)][0])
	}

	require.NoError(t, comparison.row(t, []any{scratchTestValue{text: "grow", capacity: 4096}}))
	require.Equal(t, 4096, cap(comparison.writer.encodeScratch), "successful grown result must be remembered")
	require.NoError(t, comparison.row(t, []any{scratchTestValue{text: "short", capacity: maxEncodeScratchCapacity + 1}}))
	require.Nil(t, comparison.writer.encodeScratch, "bound capacity, not encoded length")
	require.NoError(t, comparison.row(t, []any{scratchTestValue{text: "small", capacity: 32}}))
	require.Equal(t, 32, cap(comparison.writer.encodeScratch))

	// Public Column.Write and Columns.Write must never retain or supply scratch.
	publicCodec := &scratchTestCodec{}
	TypeMap(comparison.referenceCtx).RegisterType(&pgtype.Type{Name: "scratch_test", OID: customOID, Codec: publicCodec})
	for range 2 {
		comparison.reference.Start(types.ServerDataRow)
		require.NoError(t, comparison.columns[0].Write(comparison.referenceCtx, comparison.reference, BinaryFormat, scratchTestValue{text: "public"}))
		require.NoError(t, comparison.columns.Write(comparison.referenceCtx, comparison.formats, comparison.reference, []any{scratchTestValue{text: "public"}}))
	}
	require.Len(t, publicCodec.inputs, 4)
	for _, input := range publicCodec.inputs {
		require.NotNil(t, input)
		require.Zero(t, len(input))
		require.Zero(t, cap(input))
	}
}

func TestDataWriterCloseClearsEncodeScratch(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		close func(*dataWriter) error
	}{
		{name: "close", close: func(writer *dataWriter) error { writer.close(); return nil }},
		{name: "empty", close: (*dataWriter).Empty},
		{name: "complete", close: func(writer *dataWriter) error { return writer.Complete("SELECT 0") }},
		{name: "complete_write_error", close: func(writer *dataWriter) error {
			writer.client.Writer = &rowWriteOutput{err: errors.New("completion failed")}
			return writer.Complete("SELECT 0")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			comparison := newRowWriteComparison(t, Columns{{Oid: uint32(oid.T_text)}}, nil)
			comparison.writer.encodeScratch = make([]byte, 0, 128)
			err := test.close(comparison.writer)
			if test.name == "complete_write_error" {
				require.EqualError(t, err, "completion failed")
			} else {
				require.NoError(t, err)
			}
			require.True(t, comparison.writer.closed)
			require.Nil(t, comparison.writer.encodeScratch)
		})
	}
}

func TestColumnScratchContextCheckDoesNotTouchBuffer(t *testing.T) {
	t.Parallel()

	comparison := newRowWriteComparison(t, Columns{{Oid: uint32(oid.T_text)}}, nil)
	require.NoError(t, comparison.row(t, []any{"warm"}))
	scratch := comparison.writer.encodeScratch
	ctx, cancel := context.WithCancel(comparison.writer.ctx)
	cancel()
	comparison.writer.ctx = ctx
	require.ErrorIs(t, comparison.writer.Row([]any{"cancelled"}), context.Canceled)
	require.Equal(t, cap(scratch), cap(comparison.writer.encodeScratch))
	require.Same(t, &scratch[:cap(scratch)][0], &comparison.writer.encodeScratch[:cap(scratch)][0])
}

func TestPortalExecutePreEncodeUsesScratch(t *testing.T) {
	t.Parallel()

	// executeAsync pre-encodes by calling portal.execute into a bytes.Buffer.
	// Scratch must be used on that path, match the public allocation writer,
	// and be dropped when the writer completes.
	columns := Columns{{Oid: uint32(oid.T_text)}, {Oid: uint32(oid.T_bytea)}}
	formats := []FormatCode{TextFormat, BinaryFormat}
	rows := [][]any{
		{strings.Repeat("a", 32), bytes.Repeat([]byte{1}, 64)},
		{nil, []byte{}},
		{strings.Repeat("b", 4096), bytes.Repeat([]byte{2}, 8)},
		{strings.Repeat("c", maxEncodeScratchCapacity+1), nil},
		{"tail", bytes.Repeat([]byte{3}, 16)},
	}

	observer := &recordingObserver{}
	ctx := setEncodeObserver(setTypeInfo(context.Background(), pgtype.NewMap()), observer.observe)
	portal := &Portal{
		statement: &Statement{
			columns: columns,
			fn: func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				dw := writer.(*dataWriter)
				for i, values := range rows {
					if err := writer.Row(values); err != nil {
						return err
					}
					switch i {
					case 0, 1, 2:
						require.Positive(t, cap(dw.encodeScratch))
						require.LessOrEqual(t, cap(dw.encodeScratch), maxEncodeScratchCapacity)
					case 3:
						require.Nil(t, dw.encodeScratch, "oversized cell must drop retained scratch")
					case 4:
						require.Positive(t, cap(dw.encodeScratch), "later small cells allocate a new bounded scratch")
						require.LessOrEqual(t, cap(dw.encodeScratch), maxEncodeScratchCapacity)
					}
				}
				if err := writer.Complete("SELECT 5"); err != nil {
					return err
				}
				require.Nil(t, dw.encodeScratch)
				require.True(t, dw.closed)
				return nil
			},
		},
		formats: formats,
	}

	buf := &bytes.Buffer{}
	require.NoError(t, portal.execute(ctx, NoLimit, nil, buffer.NewWriter(slogt.New(t), buf)))

	referenceObserver := &recordingObserver{}
	referenceCtx := setEncodeObserver(setTypeInfo(context.Background(), pgtype.NewMap()), referenceObserver.observe)
	referenceOut := &rowWriteOutput{}
	reference := buffer.NewWriter(slogt.New(t), referenceOut)
	for _, values := range rows {
		require.NoError(t, columns.Write(referenceCtx, formats, reference, values))
	}
	require.NoError(t, commandComplete(reference, "SELECT 5"))
	require.Equal(t, bytes.Join(referenceOut.frames, nil), buf.Bytes())
	require.Equal(t, referenceObserver.snapshot(), observer.snapshot())
}
