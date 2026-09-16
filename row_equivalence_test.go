package wire

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/lib/pq/oid"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

// Keep these equivalence tests usable against the unmodified library as well.
// Columns.Write retains the original per-cell buffer allocation path.
type rowWriteOutput struct {
	frames [][]byte
	err    error
}

func (out *rowWriteOutput) Write(p []byte) (int, error) {
	out.frames = append(out.frames, bytes.Clone(p))
	if out.err != nil {
		return 0, out.err
	}
	return len(p), nil
}

type rowWriteComparison struct {
	columns           Columns
	formats           []FormatCode
	referenceCtx      context.Context
	reference         *buffer.Writer
	writer            *dataWriter
	referenceOutput   *rowWriteOutput
	output            *rowWriteOutput
	referenceObserver *recordingObserver
	observer          *recordingObserver
	attempts          uint32
	successes         uint32
}

func newRowWriteComparison(t *testing.T, columns Columns, formats []FormatCode) *rowWriteComparison {
	t.Helper()

	comparison := &rowWriteComparison{
		columns:           columns,
		formats:           formats,
		referenceOutput:   &rowWriteOutput{},
		output:            &rowWriteOutput{},
		referenceObserver: &recordingObserver{},
		observer:          &recordingObserver{},
	}
	comparison.referenceCtx = setEncodeObserver(setTypeInfo(context.Background(), pgtype.NewMap()), comparison.referenceObserver.observe)
	ctx := setEncodeObserver(setTypeInfo(context.Background(), pgtype.NewMap()), comparison.observer.observe)
	comparison.reference = buffer.NewWriter(slogt.New(t), comparison.referenceOutput)
	tag := ""
	comparison.writer = &dataWriter{
		ctx:     ctx,
		columns: columns,
		formats: formats,
		client:  buffer.NewWriter(slogt.New(t), comparison.output),
		yield:   func(struct{}) bool { return true },
		tag:     &tag,
	}
	return comparison
}

func (comparison *rowWriteComparison) row(t *testing.T, values []any) error {
	t.Helper()

	referenceErr := comparison.columns.Write(comparison.referenceCtx, comparison.formats, comparison.reference, values)
	err := comparison.writer.Row(values)
	if referenceErr == nil {
		require.NoError(t, err)
		comparison.successes++
	} else {
		require.EqualError(t, err, referenceErr.Error())
	}
	comparison.attempts++
	require.Equal(t, comparison.successes, comparison.writer.Written(), "v0.19 counts only successfully encoded rows")
	require.Equal(t, comparison.columns, comparison.writer.Columns())
	require.Equal(t, comparison.formats, comparison.writer.Formats())
	require.Equal(t, comparison.referenceOutput.frames, comparison.output.frames, "framing, output calls and prior rows must match")
	require.Equal(t, comparison.reference.Bytes(), comparison.writer.client.Bytes(), "partial frames must match on error")
	require.Equal(t, comparison.referenceObserver.snapshot(), comparison.observer.snapshot())
	return err
}

func decodeRowFrame(t *testing.T, frame []byte, columns int) [][]byte {
	t.Helper()
	require.GreaterOrEqual(t, len(frame), 7)
	require.Equal(t, byte(types.ServerDataRow), frame[0])
	require.Equal(t, uint32(len(frame)-1), binary.BigEndian.Uint32(frame[1:5]))
	var row pgproto3.DataRow
	require.NoError(t, row.Decode(frame[5:]))
	require.Len(t, row.Values, columns)
	return row.Values
}

func TestDataWriterRowEncodingMatchesColumnsWrite(t *testing.T) {
	t.Parallel()

	columns := Columns{
		{Oid: uint32(oid.T_text)}, {Oid: uint32(oid.T_bytea)}, {Oid: uint32(oid.T_int8)}, {Oid: uint32(oid.T_bool)},
		{Oid: uint32(oid.T_float8)}, {Oid: uint32(oid.T_jsonb)}, {Oid: uint32(oid.T_uuid)},
		{Oid: uint32(oid.T_timestamptz)}, {Oid: uint32(oid.T_numeric)}, {Oid: uint32(oid.T__text)},
	}
	for _, test := range []struct {
		name    string
		formats []FormatCode
	}{
		{name: "default"},
		{name: "text", formats: []FormatCode{TextFormat}},
		{name: "binary", formats: []FormatCode{BinaryFormat}},
		{name: "mixed", formats: []FormatCode{TextFormat, BinaryFormat, BinaryFormat, TextFormat, BinaryFormat, TextFormat, BinaryFormat, TextFormat, BinaryFormat, TextFormat}},
		{name: "short_formats_use_first_for_remaining", formats: []FormatCode{BinaryFormat, TextFormat}},
		{name: "extra_formats_ignored", formats: []FormatCode{0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 7}},
	} {
		t.Run(test.name, func(t *testing.T) {
			comparison := newRowWriteComparison(t, columns, test.formats)
			for _, size := range []int{0, 1, 32, 4096, 17, 64 * 1024, 64*1024 + 1, 5, 0} {
				text := strings.Repeat("x", size)
				values := []any{
					text, bytes.Repeat([]byte{0, 255}, size/2), int64(-size), size%2 == 0,
					float64(size) / 3, map[string]any{"text": text, "size": size},
					pgtype.UUID{Bytes: [16]byte{1, 2, 3, 4, 255}, Valid: true},
					time.Date(2026, 9, 16, 1, 2, 3, 456000000, time.UTC),
					pgtype.Numeric{Int: big.NewInt(int64(size)), Exp: -2, Valid: true},
					[]string{text, "", "tail"},
				}
				require.NoError(t, comparison.row(t, values))
				decodeRowFrame(t, comparison.output.frames[len(comparison.output.frames)-1], len(columns))
				// A NULL row between sizes must not discard reusable storage or
				// accidentally expose bytes from the preceding non-NULL row.
				require.NoError(t, comparison.row(t, make([]any, len(columns))))
				for _, value := range decodeRowFrame(t, comparison.output.frames[len(comparison.output.frames)-1], len(columns)) {
					require.Nil(t, value)
				}
			}
			// Typed NULLs currently have zero length, not the -1 marker. This
			// optimization deliberately does not repair that existing quirk.
			require.NoError(t, comparison.row(t, []any{
				pgtype.Text{}, []byte(nil), pgtype.Int8{}, pgtype.Bool{}, pgtype.Float8{},
				[]byte(nil), pgtype.UUID{}, pgtype.Timestamptz{}, pgtype.Numeric{}, []string(nil),
			}))
			for _, value := range decodeRowFrame(t, comparison.output.frames[len(comparison.output.frames)-1], len(columns)) {
				require.NotNil(t, value)
				require.Empty(t, value)
			}
		})
	}
}

func TestDataWriterRowNullAndEmptyEncoding(t *testing.T) {
	t.Parallel()

	for _, format := range []FormatCode{TextFormat, BinaryFormat} {
		t.Run(fmt.Sprint(format), func(t *testing.T) {
			comparison := newRowWriteComparison(t, Columns{
				{Oid: uint32(oid.T_text)}, {Oid: uint32(oid.T_bytea)}, {Oid: uint32(oid.T_bytea)},
				{Oid: uint32(oid.T_bytea)}, {Oid: uint32(oid.T_text)}, {Oid: uint32(oid.T_text)}, {Oid: uint32(oid.T_int8)},
			}, []FormatCode{format})
			for _, text := range []string{"", strings.Repeat("warm", 512), ""} {
				require.NoError(t, comparison.row(t, []any{text, nil, []byte(nil), []byte{}, (*string)(nil), sql.NullString{}, pgtype.Int8{}}))
				values := decodeRowFrame(t, comparison.output.frames[len(comparison.output.frames)-1], 7)
				require.Equal(t, []byte(text), values[0])
				require.Nil(t, values[1])
				for _, index := range []int{2, 4, 5, 6} {
					require.NotNil(t, values[index], "typed NULL remains a zero-length value")
					require.Empty(t, values[index])
				}
				if format == TextFormat {
					require.Equal(t, []byte(`\x`), values[3])
				} else {
					require.NotNil(t, values[3])
					require.Empty(t, values[3])
				}
			}
			require.Len(t, comparison.observer.snapshot(), 18, "only untyped nil skips observation")
		})
	}
}

type rowErrorText struct{ err error }

func (value rowErrorText) TextValue() (pgtype.Text, error) {
	return pgtype.Text{}, value.err
}

func TestDataWriterRowErrorsMatchColumnsWrite(t *testing.T) {
	t.Parallel()

	encodeErr := errors.New("encode failed")
	outputErr := errors.New("output failed")
	for _, test := range []struct {
		name       string
		setup      func(*rowWriteComparison)
		values     []any
		wantErr    error
		wantText   string
		wantWrites int
		wantObs    int
	}{
		{name: "column_count", values: []any{"one"}, wantText: "unexpected columns, 2 columns are defined inside the given table but 1 were given"},
		{name: "missing_type_map", setup: func(c *rowWriteComparison) {
			c.referenceCtx, c.writer.ctx = context.Background(), context.Background()
		}, values: []any{"one", "two"}, wantText: "postgres connection info has not been defined inside the given context"},
		{name: "cancelled", setup: func(c *rowWriteComparison) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			c.referenceCtx, c.writer.ctx = ctx, ctx
		}, values: []any{"one", "two"}, wantErr: context.Canceled},
		{name: "deadline", setup: func(c *rowWriteComparison) {
			ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
			defer cancel()
			c.referenceCtx, c.writer.ctx = ctx, ctx
		}, values: []any{"one", "two"}, wantErr: context.DeadlineExceeded},
		{name: "encode_second_column", values: []any{"one", rowErrorText{encodeErr}}, wantErr: encodeErr, wantObs: 1},
		{name: "unsupported_value", values: []any{"one", make(chan int)}, wantText: "cannot find encode plan", wantObs: 1},
		{name: "output", setup: func(c *rowWriteComparison) {
			c.referenceOutput.err, c.output.err = outputErr, outputErr
		}, values: []any{"one", "two"}, wantErr: outputErr, wantWrites: 1, wantObs: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			comparison := newRowWriteComparison(t, Columns{{Oid: uint32(oid.T_text)}, {Oid: uint32(oid.T_text)}}, []FormatCode{BinaryFormat})
			if test.setup != nil {
				test.setup(comparison)
			}
			writtenBefore := comparison.writer.Written()
			err := comparison.row(t, test.values)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			} else {
				require.ErrorContains(t, err, test.wantText)
			}
			require.Equal(t, writtenBefore, comparison.writer.Written(), "failed rows do not increment Written")
			require.Len(t, comparison.output.frames, test.wantWrites)
			require.Len(t, comparison.observer.snapshot(), test.wantObs)
			// A failed frame must not contaminate the next row.
			comparison.referenceCtx = setEncodeObserver(setTypeInfo(context.Background(), pgtype.NewMap()), comparison.referenceObserver.observe)
			comparison.writer.ctx = setEncodeObserver(setTypeInfo(context.Background(), pgtype.NewMap()), comparison.observer.observe)
			comparison.referenceOutput.err, comparison.output.err = nil, nil
			require.NoError(t, comparison.row(t, []any{"after", "error"}))
		})
	}
}

func TestDataWriterRowCancellationBetweenColumns(t *testing.T) {
	t.Parallel()

	for _, cancelAt := range []int{1, 2} {
		t.Run(fmt.Sprint(cancelAt), func(t *testing.T) {
			comparison := newRowWriteComparison(t, Columns{{Oid: uint32(oid.T_text)}, {Oid: uint32(oid.T_text)}}, nil)
			install := func(ctx context.Context, rec *recordingObserver) context.Context {
				ctx, cancel := context.WithCancel(ctx)
				t.Cleanup(cancel)
				calls := 0
				var observedCtx context.Context
				observedCtx = setEncodeObserver(ctx, func(ctx context.Context, format FormatCode, oid uint32, n int) {
					require.Same(t, observedCtx, ctx)
					rec.observe(ctx, format, oid, n)
					calls++
					if calls == cancelAt {
						cancel()
					}
				})
				return observedCtx
			}
			comparison.referenceCtx = install(comparison.referenceCtx, comparison.referenceObserver)
			comparison.writer.ctx = install(comparison.writer.ctx, comparison.observer)
			err := comparison.row(t, []any{"first", "last"})
			if cancelAt == 1 {
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, comparison.output.frames)
			} else {
				require.NoError(t, err, "there is no extra context check after the final column")
				require.Len(t, comparison.output.frames, 1)
			}
			require.Len(t, comparison.observer.snapshot(), cancelAt)
		})
	}

	t.Run("zero_columns_do_not_check_context", func(t *testing.T) {
		comparison := newRowWriteComparison(t, nil, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		comparison.referenceCtx, comparison.writer.ctx = ctx, ctx
		require.NoError(t, comparison.row(t, nil))
		decodeRowFrame(t, comparison.output.frames[0], 0)
	})
}

func TestDataWriterIndependentConcurrentRowEncoding(t *testing.T) {
	for i := range 16 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			// Each writer, type map, observer and output has its own owner.
			comparison := newRowWriteComparison(t, Columns{{Oid: uint32(oid.T_text)}, {Oid: uint32(oid.T_bytea)}}, []FormatCode{FormatCode(i % 2), BinaryFormat})
			for j := range 32 {
				require.NoError(t, comparison.row(t, []any{
					strings.Repeat(fmt.Sprintf("writer-%d-row-%d", i, j), j+1),
					bytes.Repeat([]byte{byte(i), byte(j)}, j*16),
				}))
			}
		})
	}
}

func TestDataWriterRowLimitAndClose(t *testing.T) {
	t.Parallel()

	comparison := newRowWriteComparison(t, Columns{{Oid: uint32(oid.T_text)}}, nil)
	comparison.writer.limit = 2
	require.NoError(t, comparison.row(t, []any{"one"}))
	require.NoError(t, comparison.row(t, []any{"last"}))
	require.ErrorIs(t, comparison.writer.Row([]any{"over limit"}), ErrRowLimitExceeded)
	require.Equal(t, uint32(2), comparison.writer.Written())
	require.Len(t, comparison.output.frames, 2)
	require.Len(t, comparison.observer.snapshot(), 2)
	require.ErrorIs(t, comparison.writer.Empty(), ErrDataWritten)
	require.NoError(t, comparison.writer.Complete("SELECT 2"))
	require.NoError(t, commandComplete(comparison.reference, "SELECT 2"))
	require.Equal(t, comparison.referenceOutput.frames, comparison.output.frames)
	require.ErrorIs(t, comparison.writer.Row([]any{"closed"}), ErrClosedWriter)
	require.ErrorIs(t, comparison.writer.Complete("SELECT 2"), ErrClosedWriter)
	require.Equal(t, uint32(2), comparison.writer.Written())
}
